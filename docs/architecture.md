# rpi-mesh 架构

## 1. 拓扑

```
办公机 (client)                          服务器 (relay, 常驻)
┌──────────────────────────┐             ┌──────────────────────────┐
│ rpi instance A           │             │ rpi instance B           │
│  rpi-mesh ─┐             │  (出站方向)  │  rpi-mesh ─┐             │
│  (单插件)  │             │             │  (单插件)  │             │
└────────────┼─────────────┘             └────────────┼─────────────┘
             │                                        │
        mesh.db (本地, 单机时)             mesh.db (relay 侧, 跨机时)
```

- **本地**：所有实例直接打开同一个 `mesh.db`（SQLite WAL），没有 daemon/端口/锁文件。
- **跨机（已实现，不依赖 ssh）**：host 侧实例 `remote.role=host` 监听 TCP（默认
  `127.0.0.1:9527`）或 unix socket；guest 侧实例 `remote.role=guest` 直连 `connect`。
  guest→host 方向本来就可达，无需反向端口。
- **监听自动转移**：host 机器上任何 mesh 实例都可接管监听——bind 失败（端口忙）的实例
  降级为 guest（连本机端口）并每 5s 重试 bind，当前监听者退出后自动升级回 host。端口
  本身就是互斥量，无需租约协调。
- 鉴权：共享 token 文件（0600，32 位 hex，relay 缺失时自动生成）。两种线上格式——
  - 直连 JSON-RPC：行分隔 `{id,method,params}` → `{id,result|error}`，首帧必须 `authenticate`；
  - `"http": true`：`POST /mesh-rpc`，token 在 `X-Mesh-Auth` 头，一请求一 RPC。专为防火墙场景：
    只放行"转发到真实 HTTP 服务"的端口时，relay 用 HTTP 应答，其它路径 404，可让 nginx 按
    路径反代到本机 relay。方法表见 `rpi-mesh-core::api::method`；插件只看到
    `Arc<dyn MeshStore>`，本地/远程同一套调用。
- 客户端断线指数退避重连并自动重新认证；调用失败重试一次。relay 退出时 client 报
  `cannot reach the hub relay`（worker 300ms 重试，恢复即继续）。
- 已在真实拓扑验证：办公室 WiFi → 公司防火墙 → apmprod:8085（`http:true`），跨机发现、
  `/use` 租约交接、relay 侧 SQLite 一致（`scripts/smoke-relay.sh`）。

## 2. 数据库

```sql
instances(node_id PK, name, host, pid, cwd, session_id, title, model, role,
          busy, epoch, started_at, last_seen,
          channels)  UNIQUE(name, host)     -- channels: 实例可服务的通道能力（JSON）
leases(capability PK, owner_node, owner_name, generation,
       expires_at, updated_at, state_json)  -- state_json: 随租约走的运行态（cursor，加密）
envelopes(id PK, to_node, from_node, from_name, kind, payload, correlation,
          created_at, expires_at, acked_at) -- payload: 整体 AES-256-GCM 加密
channel_state(capability PK, state_json)    -- 共享通道事实（微信凭据，加密）
seen(id PK, ts)
kv(key PK, value)
```

- `payload` 与 `channel_state.state_json` 落盘前 AES-256-GCM 加密；密钥在 DB 之外
  （`RPI_MESH_SECRET` env 或 `<mesh 目录>/secret.key`，0600，全网一致）。

- `node_id = sha256(host ‖ cwd ‖ pid)[..16]`：**同一进程内跨 `/reload` 不变**（pid/cwd 不变），
  重启即变——正好满足租约对「进程重启后换新身份」的要求。
- **显示名 = 项目目录名**：`name@host` 直接表达"哪个项目在哪台机器"（如 `rpi-mesh@office`、
  `pi-rust@server`）。`instanceName` 只是整机手动覆盖项，**默认不要设**——同机多个会话
  全部同名只能靠 `-<pid>` 后缀区分，对使用者不可读；目录名天然区分。
- 单插件架构下不存在旧插件加载遗留线程的问题：role 切换通过 SwitchableStore 立即生效，
  进程内线程生命周期由插件 lifecycle 管理。
- `payload` 存整个信封 JSON（加密）：新增 kind 不需要迁移表结构。
- WAL + `busy_timeout=3000` + `BEGIN IMMEDIATE`（读-改-写）处理多进程并发。

保留期：presence 10s（心跳 3s；跨机退出后 ~10s 从列表消失），text/cmd 2min，reply 10min，`seen` 30min，已 ack 的信封 1h。信封 payload 与凭据加密落盘。

## 3. 信封

```jsonc
{"id":"…","kind":"text","fromNode":"…","fromName":"home","toNode":"…",
 "text":"…","correlation":"…","createdAt":0,"expiresAt":0}
{"kind":"cmd",  "command":{"name":"reload","args":[]}}
{"kind":"reply","correlation":"…","ok":true,"text":"…"}
{"kind":"notice","text":"…"}
```

`kind=text` 注入目标 agent；`cmd` 走 intent→runtime action；`reply` 完成 `hub_ask`；
`notice` 只显示不进模型上下文。

投递语义：入库即持久；接收方处理完再 `ack`；未 ack 且未过期会被再次读出（at-least-once），
进程内 `replies` 按 correlation 去重。

## 4. 租约（谁接微信）

```sql
-- 认领：无租约 / 已过期 / 自己的 → 成功
UPDATE leases SET owner_node=?, owner_name=?,
       generation = CASE WHEN owner_node=? AND expires_at>? THEN generation ELSE generation+1 END,
       expires_at=?+ttl
 WHERE capability=? AND (owner_node IS NULL OR owner_node=? OR expires_at<?);

-- 续约 / 发送前检查：owner_node + generation 全匹配
-- 转移：generation+1，旧持有者立刻被 fence
```

规则：

- **单一写者（OwnerLoop）**：wechat 插件内一个独立线程持有租约生命周期——
  5s `renew`；失败后先 `claim` 一次（机器暂停 > TTL 后恢复、无人抢走的续上场景）；
  claim 返回 `Held` 才退出并通知桥。桥循环不再触碰租约：它只读 `lease_alive`
  标志，OwnerLoop 清除即停。根因教训：续约曾写在桥循环顶部，而循环主体是 40s
  `getupdates` 长轮询，TTL 20s 必在轮询中过期 → 全网 watcher 抢占振荡。
- TTL 默认 60s（`MIN_TTL_SECONDS=45`：必须 > getupdates 40s 长轮询 + 余量），
  `LeaseConfig::validate()` 在加载时校验 renew ≤ ttl/4 与 TTL 下限，仅告警不拒载。
- 本地 owner 出站前不再逐次 `check_lease`（OwnerLoop 的 5s 节拍即门控；桥
  停止即出站停止）。
- `/use <target>` = `assign_lease(target)` + **assign/unassign 信封**：generation+1
  fence 旧 owner，同时向目标投 `assign`、向旧 owner 投 `unassign`（best effort，
  丢了只是慢一个 tick），并 publish 进程内租约变更信号（Condvar）——本机的
  OwnerLoop/watcher 亚秒级重读租约行，footer 即时跟手。旧 owner 下次续约失败
  → 停止收发；目标 watcher 收到信号立即起桥 resume 租约（同 node_id →
  generation 不变）。
- `/wechat stop` = `release_lease` + 持久化"用户显式停用"；会话过期（errcode -14）
  时 OwnerLoop 释放租约并标记 `session_expired`（登录过期），watcher 不再自动重启死会话。
- **watcher 语义（防振荡）**：绝不抢未过期且 owner 健在的租约；只在「无租约行 /
  过期且 `updated_at` 超 30s 无续约 / owner 的 presence 行已消失」三种情况接管。
  `updated_at`（最近一次成功续约时间）是区分"过期但活跃续约"与"过期且已死"的事实依据。
- **死进程 reap**：presence 行带 pid；同 host 的行用 `kill(pid, 0)` 探活，死行
  立即清除（`list_instances_alive`/`get_instance_alive`/worker 慢 tick），不再等
  30s 心跳窗口——C-c/SIGKILL 的实例立刻从 `/instances` 消失，也不阻塞 dead-owner
  租约接管。
- 进程暂停超过 TTL 后恢复：若无人抢，OwnerLoop 的 renew→claim 分支续上（没有分裂）；
  若被抢，generation 已变 → 续约/检查都失败，OwnerLoop 退出。
- footer（`SetStatus{key:"wechat"}`）只有两个写者：OwnerLoop（持有中：💬/💭）与
  watcher（非持有：🗨️/📴），全部从 store 事实渲染，不再有进程私有视图。

## 5. 线程模型

**worker**（`worker.rs`）周期：

| 周期 | 动作 |
| --- | --- |
| 300ms | 读自己的信箱并处理（text/cmd/reply/notice/assign），处理完 ack |
| 3s | 心跳（带 busy/session/model/**channels 能力**）；行被过滤说明已死亡 |
| 60s | 清理死实例（含 pid 探活）、孤儿租约、过期信封、旧 `seen` |

**OwnerLoop**（`wechat/owner.rs`）周期：

| 周期 | 动作 |
| --- | --- |
| 200ms | 检查租约行是否还归本实例所有（检测租约被夺） |
| 5s | `renew` 续期；失败先 `claim`（暂停恢复），仍 Held 则退出并通知桥 |
| 事件 | 桥退出（SessionExpired → 释放租约并标记登录过期） |

**host_negotiator**（`backends.rs`，仅 host 角色）：bind 成功 → serving；
bind busy → 降级 guest（连本机端口）+ 5s 重试 bind，成功则升级回 host。

**watcher**（`wechat/commands.rs`，200ms）：无租约 / 过期且 30s 无续约 / owner 消失时接管。

`reload` 意图特殊处理：**先回包 ack**，再调 `Reload` action，然后请求本 load 退出。
通知类 message 走 `SendMessage{customType, display:true, triggerTurn:false}`，只显示不触发模型。

## 6. 委派（跨网络调研）

`mesh_ask` / `/ask`：

1. 发送方把任务文本作为信封投进目标实例的信箱。
2. 目标实例把它交给自己的 agent 运行；**宿主的 `SendUserMessage` 会等运行结束**，
   直接返回 `{status, text}`（最终回答或失败原因）。
3. 目标实例把结果作为 reply 信封发回发送方，按任务编号对应——同时发多个任务也不会串。
4. 发送失败、目标忙、目标实例未配置模型，都会明确回复失败原因，不会无声无息。

默认等待 300s，上限 900s。目标正在忙时消息会排队而非立即执行，发送方会被告知。

 双向信箱（含跨机）

`hub_ask`/`hub_send`/`hub_cmd` 的寻址是对称的：任何实例（本机或经 relay
连入的远端）都能向任何其它实例的信箱投递——办公机→服务器和
服务器→办公机走的是同一条信箱路径（共享 mesh.db / relay RPC），不依赖
ssh。worker 300ms 轮询自己的信箱并 ack，`reply` 按 correlation 回到
发起方。

远端机器的标准接入方式：**在那台机器上跑一个 rpi 实例**（relay client
连回主 hub），之后 `/instances` 可见、`hub_ask` 可委派、`/use` 可接管
微信——不需要 ssh 一次性任务通道（`hub_run` 已移除）。

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

1. 媒体与图片注入、`/sessions`/`/goto`、隔离委派（目标机 `rpi -p` 子进程）
2. `/sum`（最后 AI 回复摘要——需 pi-rust 暴露会话历史 action）

## 9. 已验证的真实拓扑

apmprod（Linux，经公司防火墙可达）：`/mesh relay 8085` 交互式组网为 host，本机
`/mesh client apmprod-IP:8085 <token>` 以 guest 直连。实测：跨机 presence、
`/msg` `/ask`（回复经 relay 回传）、`/use` 交接微信（凭据自动同步到目标实例）、
监听自动转移（杀当前 host，同机另一实例 5s 内接管）、信封与凭据的加密落盘。
单机回归 `scripts/smoke.sh` 覆盖两实例、重名、唯一租约、/use 交接、双向信箱。
