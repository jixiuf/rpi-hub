# rpi-hub 架构

## 1. 拓扑

```
办公机 (client)                          服务器 (relay, 常驻)
┌──────────────────────────┐             ┌──────────────────────────┐
│ rpi instance A           │             │ rpi instance B           │
│  rpi-hub ──┐             │   ssh -L    │  rpi-hub ──┐             │
│  rpi-wechat│             │  (出站方向)  │  rpi-wechat│             │
└────────────┼─────────────┘             └────────────┼─────────────┘
             │                                        │
        hub.db (本地, 单机时)              hub.db (relay 侧, 跨机时)
```

- **本地**：所有实例直接打开同一个 `hub.db`（SQLite WAL），没有 daemon/端口/锁文件。
- **跨机（已实现，不依赖 ssh）**：服务器侧 `remote.mode=relay` 监听 TCP（默认
  `127.0.0.1:9527`）或 unix socket；办公机 `remote.mode=client` 直连 `relay.connect`。
  办公机→服务器方向本来就可达，无需反向端口。
- 鉴权：共享 token 文件（0600，32 位 hex，relay 缺失时自动生成）。两种模式——
  - 裸 JSON-RPC：行分隔 `{id,method,params}` → `{id,result|error}`，首帧必须 `authenticate`；
  - `"http": true`：`POST /hub-rpc`，token 在 `X-Hub-Auth` 头，一请求一 RPC。专为防火墙场景：
    只放行"转发到真实 HTTP 服务"的端口时，relay 用 HTTP 应答，其它路径 404，可让 nginx 按
    路径反代到本机 relay。方法表见 `rpi-hub-core::api::method`；插件只看到
    `Arc<dyn HubStore>`，本地/远程同一套调用。
- 客户端断线指数退避重连并自动重新认证；调用失败重试一次。relay 退出时 client 报
  `cannot reach the hub relay`（worker 300ms 重试，恢复即继续）。
- 已在真实拓扑验证：办公室 WiFi → 公司防火墙 → apmprod:8085（`http:true`），跨机发现、
  `/use` 租约交接、relay 侧 SQLite 一致（`scripts/smoke-relay.sh`）。

## 2. 数据库

```sql
instances(node_id PK, name, host, pid, cwd, session_id, title, model, role,
          busy, epoch, started_at, last_seen)  UNIQUE(name, host)
leases(capability PK, owner_node, owner_epoch, owner_name, generation,
       expires_at, updated_at)
envelopes(id PK, to_node, from_node, from_name, kind, payload, correlation,
          created_at, expires_at, acked_at)
seen(id PK, ts)
kv(key PK, value)
```

- `node_id = sha256(host ‖ cwd ‖ pid)[..16]`：**同一进程内跨 `/reload` 不变**（pid/cwd 不变），
  重启即变——正是租约需要的粒度。
- **显示名 = 项目目录名**：`name@host` 直接表达"哪个项目在哪台机器"（如 `pi-hub@apmprod`、
  `rpi-hub@ljmacjxf`）。`instanceName` 只是整机手动覆盖项，**默认不要设**——同机多个会话
  全部同名只能靠 `-<pid>` 后缀区分，对使用者不可读；目录名天然区分。
- `epoch`：每次插件加载生成一次，所有写都必须匹配，用来 fencing 旧加载遗留的线程。
- `payload` 存整个信封 JSON：新增 kind 不需要迁移表结构。
- WAL + `busy_timeout=3000` + `BEGIN IMMEDIATE`（读-改-写）处理多进程并发。

保留期：presence 30s（心跳 3s），text/cmd 2min，reply 10min，`seen` 30min，已 ack 的信封 1h。

## 3. 信封

```jsonc
{"id":"…","kind":"text","fromNode":"…","fromName":"home","toNode":"…",
 "text":"…","correlation":"…","createdAt":0,"expiresAt":0}
{"kind":"cmd",  "command":{"name":"reload","args":[]}}
{"kind":"reply","correlation":"…","ok":true,"text":"…"}
{"kind":"notice","text":"…"}
```

`kind=text` 注入目标 agent；`cmd` 走 intent→runtime action；`reply` 完成 `hub_ask`/`hub_cmd`；
`notice` 只显示不进模型上下文。

投递语义：入库即持久；接收方处理完再 `ack`；未 ack 且未过期会被再次读出（at-least-once），
进程内 `replies` 按 correlation 去重。

## 4. 租约（谁接微信）

```sql
-- 认领：无租约 / 已过期 / 自己的（含 reload 换 epoch）→ 成功
UPDATE leases SET owner_node=?, owner_epoch=?, owner_name=?,
       generation = CASE WHEN owner_node=? AND expires_at>? THEN generation ELSE generation+1 END,
       expires_at=?+ttl
 WHERE capability=? AND (owner_node IS NULL OR owner_node=? OR expires_at<?);

-- 续约 / 发送前检查：owner_node + owner_epoch + generation 全匹配
-- 转移：generation+1，旧持有者立刻被 fence
```

规则：

- TTL 20s，5s 续租；本地 owner 每次出站前 `check_lease`（缓存 ≤2s），失败就停桥。
- `/use <target>` = `assign_lease(target)`：generation+1，旧 owner 下次续约失败 → 停止收发。
  目标实例的 worker 用同一 `node_id` 认领时走"reload 续租"分支，generation 不变。
- `/wechat stop` = `release_lease`，并把"用户显式停用"持久化，避免心跳又抢回来。
- 进程暂停超过 TTL 后恢复：若无人抢，`renew` 允许续上（没有分裂）；若被抢，generation 已变 →
  续约/检查都失败。

## 5. worker

一个线程（`src/worker.rs`），周期：

| 周期 | 动作 |
| --- | --- |
| 300ms | 读自己的信箱并处理（text/cmd/reply/notice），处理完 ack |
| 3s | 心跳（带 busy/session/model）；返回 0 行说明 epoch 过期 → 退出 |
| 5s | 租约续期 / 空闲时按 `autoClaim` 认领 |
| 15s | `GetExecutionContext` 刷新 sessionId/model/cwd |
| 60s | 清理死实例、孤儿租约、过期信封、旧 `seen` |

`reload` 意图特殊处理：**先回包 ack**，再调 `Reload` action，然后请求本 load 退出，
避免"重载把正在回包的人卸载掉"。
通知类 message 走 `SendMessage{customType, display:true, triggerTurn:false}`，只显示不触发模型。

## 6. 委派（跨网络调研）

`hub_ask`：

1. 发送方插入 `replies[correlation] = pending` 占位（否则快回复会被当成"无人等待"的通知）。
2. 发 `kind=text` + correlation 给目标。
3. 目标 worker 注入 agent 输入，把 correlation 压入 `pending` 队列。
4. 目标 `MessageEnd`（assistant 且有可见文本）→ 发 `kind=reply`；`TurnEndPayload` 为
   `aborted/error` → 回失败。
5. 发送方 worker 收到 reply → 填入 `replies` → `hub_ask` 的 `poll` 在下一次 tick 读到并返回。
6. 超时/取消 → 删除占位；迟到的 reply 变成通知，不会静默丢弃。

默认 300s，上限 900s。目标在忙时 `SendUserMessage` 以 `deliverAs:"steer"` 排队。

## 6.5 无实例的远端：`hub_run`

`hub_ask` 依赖目标机上有实例在跑；没有实例时用 `hub_run`（`remoteHosts` 配置）：

```
ssh -o BatchMode=yes -- <ssh> sh -lc 'cd "$1" || exit 70; shift; exec "$@" -p "$(cat)"' sh <cwd> <rpi> [args…]
```

- 任务走 **stdin**（`-p "$(cat)"`），既没有 argv 长度限制，也没有 shell 拼接/注入面；
  cwd / rpi 路径 / 附加参数都作为位置参数传入并做字符白名单校验。
- 超时 kill，stdout ≤1MiB、stderr ≤32KiB；结果回到等待中的 `hub_run`，或作为 `/run` 通知。
- 语义差异要记住：一次性（除非远端自己 `--continue`），用远端机器的 `~/.rpi` 凭据与项目配置，
  不共享调用方的会话上下文。有实例时优先 `hub_ask`。

## 7. 失败模式

| 场景 | 行为 |
| --- | --- |
| `/reload` | 新 epoch 注册；旧线程心跳/续约被拒后退出；租约 generation 不变，微信不断线 |
| 进程崩溃 | presence 30s 后清理；租约 TTL 20s 后别人可认领 |
| 两个实例同名 | 后注册者 `-<pid>`；重启后旧行过期恢复正常名 |
| 信箱重复投递 | 处理后才 ack；接收方按 correlation/`seen` 去重 |
| DB 被新版本写过 | `kv.schema_version` 大于本版 → 明确报错，拒绝半懂运行 |
| 命令 handler 太慢 | 只做 SQL；网络/等待放 worker（TUI 线程不能被占） |

## 8. 路线图

1. `rpi-wechat`：iLink 长轮询、回复分块、`send_file/send_image`、`/use` 交接、QR 登录
2. 媒体与图片注入、`/sessions`/`/goto`、`/start`（tmux）、隔离委派（目标机 `rpi -p` 子进程）

## 9. 已验证的真实拓扑

`RELAY_PORT=8085 scripts/smoke-relay.sh apmprod`：apmprod（Linux）用 tmux 起 relay 实例
（`http:true`，端口 8085 经公司防火墙可达），本机以 client 直连；断言跨机 presence、
`/use` 把微信租约切到远端、relay 数据库里 owner 一致。单机回归 `scripts/smoke.sh`
覆盖两实例、重名、唯一租约。
