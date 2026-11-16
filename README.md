# rpi-hub

rpi 的多实例协同：实例发现、互相发消息/指令、任务委派，以及"哪个实例接微信"的可切换租约。

设计目标来自实际用法：**一台办公机 + 一台服务器，办公机能 ssh 到服务器，反过来不行**；两台机器
网络/环境不同，部分调研必须交给位于对应网络里的实例完成；微信同一时刻只能接一个实例，但接管者要能切换。

当前状态：**本地多实例 + 跨机 relay（自研 TCP/HTTP + token 鉴权，不依赖 ssh）已在真机验证**；微信通道是下一步。

```
~/repos/rpi-hub/
├── rpi-hub-core/   # 共享库：身份、SQLite store、租约、信封协议、relay wire（无 ABI）
├── rpi-hub/        # cdylib 插件：tools / 斜杠命令 / 事件 / 后台 worker / ssh 远端一次性任务
└── rpi-wechat/     # cdylib 插件：iLink 通道（下一步）
```

> dots 里的 `~/vmacs/dots/rpi/agent/extensions` 是这份仓库的镜像（开发副本），正式源码以本仓库为准。

## 核心设计（与 pi-hub 的差异）

| 问题 | pi-hub | rpi-hub |
| --- | --- | --- |
| 多进程共享状态 | `instances.json` + 队列文件 + 锁文件 + HTTP 服务 + 手写 WS 帧 | 一个 SQLite（WAL）：`instances / leases / envelopes / seen` |
| 本地通信 | HTTP + WS + 2s 轮询兜底 | 同一个数据库，worker 300ms 拉自己的信箱 |
| 接管仲裁 | 15s TTL 文件锁 + 3s 续约 + `force` | 租约 + **generation/epoch fencing**，每次出站前 `check()` |
| `/reload` | 旧闭包可能继续跑 | 每次加载生成新 epoch，旧 epoch 写入被拒；租约按 `node_id` 原地续约 |
| 远程指令 | 注入 `/__hub_reload` 让模型回合去 dispatch | 直接映射 host runtime action（`NewSession/Fork/Reload/…`） |
| 任务结果回传 | `tasks.json` + `[TASK#N结果]` 文本约定 | correlation id + `TurnEnd`/`MessageEnd` 回包 |
| 跨机 | SSH 轮询远程实例 + 远程 takeover 文件 | relay（token 鉴权，可选 HTTP 前门模式）+ `hub_run`（ssh 一次性任务） |

因此这里没有 daemon、没有本地端口（单机时）、没有文件锁代码。

## 安装

```bash
cd ~/repos/rpi-hub

# 开发：编译 + watch + 热重载（多包 workspace 需要 -P）
rpi dev -P rpi-hub
rpi dev-local -P rpi-hub      # 只加载本扩展，隔离调试

# 安装到全局 ~/.rpi/agent/extensions
rpi install --path ./rpi-hub
rpi install --path ./rpi-wechat      # 微信通道（下一步）
```

> rpi 0.23 的 `install --path` 在 cargo 1.100 下有兼容 bug（找不到 build 输出目录）。
> 已在 pi-rust main 修复（7697ca8）。0.23 上手动安装：
> ```bash
> cargo build --release -p rpi-hub
> cp target/release/librpi_hub.dylib ~/.rpi/agent/extensions/   # macos；linux 是 .so
> ```

## 工具（给模型用）

| 工具 | 作用 |
| --- | --- |
| `hub_instances` | 列出实例（本机/远程、cwd、会话、忙闲）与微信接管者 |
| `hub_send` | 把消息投进目标实例的 agent 输入队列（让它在自己网络/文件系统里干活） |
| `hub_cmd` | 在目标实例执行 `new/fork/compact/reload/model/thinking/name/stop/status` |
| `hub_ask` | 委派任务并等待目标实例的最终回答（跨网络调研的主力） |
| `hub_run` | 在**没有跑实例**的 SSH 机器上跑一次性任务（`remoteHosts`，今天就能用） |
| `hub_switch` | 把某个渠道（默认 `wechat`）的接管切到目标实例 |

## 斜杠命令

```
/instances                 实例列表 + 当前微信接管
/use <实例|编号>            把微信接管切过去（空参数显示持有者）
/msg <实例> <内容>          发消息
/cmd <实例> <指令>          远程执行指令（结果以通知形式回到本会话）
/ask <实例> <任务>          委派任务（结果以通知形式回到本会话）
/run <主机> <任务>          远端一次性任务（remoteHosts，无需对方跑实例）
/hub status|who|name <名>|leave
```

## 状态与配置

```
~/.rpi/agent/pi-hub/
├── config.json          # 见下
├── hub.db               # SQLite（WAL），多进程共享
├── relay-token          # relay 共享 token（0600）
├── logs/hub.log         # 5MB 轮转；RPI_HUB_DEBUG=1 同时写 stderr
└── wechat/              # 微信凭据与游标（rpi-wechat 使用）
```

```json
{
  "version": 1,
  "instanceName": null,
  "lease": { "ttlSeconds": 20, "renewSeconds": 5, "resumeGraceSeconds": 10 },
  "remote": {
    "mode": "local",
    "relay": {},
    "hosts": {
      "server": { "ssh": "me@server", "cwd": "/work/project", "rpi": "rpi" }
    }
  },
  "channels": { "wechat": { "enabled": true, "autoClaim": true } },
  "limits": { "maxTextBytes": 65536, "inboxBatch": 32 }
}
```

环境变量：`RPI_HUB_DIR`（状态目录）、`RPI_HUB_INSTANCE`（实例名）、`RPI_HUB_DEBUG=1`。

**实例名默认 = 项目目录名（cwd basename）**，`name@host` 由此天然区分"哪个项目在哪台机器"。
`instanceName` 是**整机手动覆盖**（一台机器通常不要设：同机多个 rpi 会话会全部同名，
只能靠 `-<pid>` 后缀区分，对使用者不可读）。同项目同机多会话仍会自动加 `-<pid>`。
`/use home` 匹配顺序：完整名 → `name@host` → 去 pid 后缀 → 唯一前缀。

## 跨机（办公室 ⇄ 服务器，不依赖 ssh）

方向是单向的：办公室能发起出站连接到服务器，服务器不能反向拨号。**relay 跑在服务器**，
办公室实例以 `client` 身份直接 `connect`。

服务器 `~/.rpi/agent/pi-hub/config.json`：

```json
{
  "version": 1,
  "instanceName": "server",
  "remote": {
    "mode": "relay",
    "relay": { "listen": "0.0.0.0:8085", "tokenFile": "~/.rpi/agent/pi-hub/relay-token", "http": true }
  }
}
```

办公室 `~/.rpi/agent/pi-hub/config.json`：

```json
{
  "version": 1,
  "instanceName": "office",
  "remote": {
    "mode": "client",
    "relay": { "connect": "10.x.x.x:8085", "tokenFile": "~/.rpi/agent/pi-hub/relay-token", "http": true }
  }
}
```

（token 文件两端一致：0600，内容为 32 位 hex；没有文件时 relay 生成，需要手动同步一次到 client。）

- **裸模式**（`http` 缺省/false）：行分隔 JSON-RPC，首帧必须 `authenticate {token}`。
- **HTTP 前门模式**（`"http": true`）：防火墙通常只放行"转发到真实 HTTP 服务"的端口。此时 relay
  应答 `POST /hub-rpc`（token 在 `X-Hub-Auth` 头，一个请求一次 RPC），其它路径一律 404——
  同端口的真实服务不受影响，也可以让 nginx 按路径把 `/hub-rpc` 反代到本机 relay。
- 鉴权：token 文件 0600、常数时间比较、Debug 打印 `Token(***)`、日志/错误不落明文。
- `listen` 绑定非回环地址时启动日志给出警告（token 是唯一防线）。
- client 断线指数退避重连（1→15s）并自动重新认证；调用失败重试一次。
- unix socket 仍支持（`listen`/`connect` 填 `unix:` 前缀路径），本机进程间零鉴权开销。
- 两机的 `hub.db` 只有一份（在 relay 侧）：办公室节点的 presence/租约/信箱都通过连接读写。
- 完全不依赖 ssh（`sshHost/remoteSocket/localSocket` 配置会报错提示迁移）。

## 验证

```bash
cargo test                          # 45 core 单测（租约 fencing、并发抢占、信箱、relay/token/auth、HTTP 模式…）
                                    # + 7 plugin 单测（ssh 计划注入面、事件文本提取）
scripts/smoke.sh                    # 真 rpi 单机：两个实例注册、重名处理、唯一租约、/instances、/use
RELAY_PORT=8085 scripts/smoke-relay.sh HOST   # 真机跨机：relay(tmux, http:true) + 本机 client
```

`smoke-relay.sh` 在无法 ssh 到目标机时自动 skip；已实测 apmprod:8085（防火墙可直连的端口）。

## 状态

- ✅ 多实例协调（presence / 信箱 / 租约 fencing）
- ✅ 跨机 relay（TCP+token，可选 HTTP 前门模式）
- ✅ rpi-wechat 文字通道（QR 登录、收发、分块回复、租约门控）
- ✅ TUI 参数建议（`register_argument_suggester`，pi-rust PR #338 待合并）
- ⬜ 媒体收发（代码已有：`media.rs` + `send_image/file_to_wechat` 工具；待真机验证）
- ⬜ `/sessions` `/goto`、`/start`（tmux 拉起实例）、隔离委派

> 部署注意：`Cargo.toml` 中 `rpi-plugin-sdk` 目前指向本地 pi-rust 路径
> （argument-suggester 槽位在 PR #338 中，crates.io 尚未发版）。PR 合并发版后
> 改回 `rpi-plugin-sdk = "0.25"` 即可正常构建。跨机部署见
> `scripts/deploy-apmprod.sh`（会重写远端的 SDK path）。

见 `docs/architecture.md`、`docs/plugin-authoring.md`、`docs/reuse-survey.md`（rpi-package 里可复用的东西）。
