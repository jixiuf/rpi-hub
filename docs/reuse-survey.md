# 可复用资产调研（rpi-package / 在线目录）

结论先说：**多实例 presence + 租约 + 信箱这一层没有现成包，必须自己实现**；
但传输、身份语义、IM 通道 UX 都有可直接对齐或借鉴的成熟实现。

## 1. 逐个看

### `rpi-im-message`（3.0k 行，最接近 `rpi-wechat` 的先例）
飞书官方 SDK WebSocket 长连接 + 单工具多 action：

- 配置优先级：`RPI_IM_CONFIG` → 项目 `.rpi/im.json` → `~/.rpi/agent/im.json`；**凭据只在配置/环境，不进工具参数**。
- 一个有界队列（`maxQueueSize` 默认 256）、`receive` 只取一条、超时是正常结果。
- `autoReply` 通过 runtime bridge 把入站消息喂给 agent（正是我们 `SendUserMessage` 的路径）。
- `cardkit.rs` 做 Markdown→飞书卡片；`sessions.rs` 维护 IM 会话 ↔ rpi 会话映射；`progress.rs` 流式进度。

**复用方式**：不共享代码（协议不同：飞书 SDK WS vs 微信 iLink HTTP 长轮询），但 `rpi-wechat` 的工具形状、
配置优先级、队列/超时语义、markdown 分块、autoReply 语义全部对齐它，让生态一致。

### `rpi-server`（TCP JSONL JSON-RPC 2.0 + token，`rpi --server`）
已有：token 认证（首帧 `authenticate`）、`start_session`（用当前可执行文件 `--mode rpc` 起子进程）、
`send`/`subscribe`、心跳/在线状态、`Transport` trait、明确的"无 TLS、只监听回环"安全边界。

**复用方式**：跨机 relay 的**协议形态**照它的做（JSON-RPC 2.0 + 首帧认证 + subscribe 推送 + 不记录 token），但插件不能链接 `rpi-server`（它属于宿主侧），所以只借约定，不借 crate。自研 relay 已落地并真机验证（TCP/HTTP + token，出站方向连接）。

### `rpi-node` / `rpi-node-core`（`rpi --node`）
`NodeIdentity { node_id, owner_epoch }` + 适配器注册表 + mDNS 发现；mDNS 只是"定位"，认证另做。

**复用方式**：我们已有的 `node_id` / `epoch` 就是这套语义（epoch = owner epoch，用来 fence 旧持有者）。
文档与命名对齐它，不再自创词。mDNS 对"办公机→服务器"没有价值（跨网段），不引入。

### `rpi-extcore`（可依赖 crates.io）
`service::Service`、`host::HostSession`（spawn `<exe> --mode rpc`，可指定 executable）、事件订阅、
`durable::DurableCoordinator`、file-io/external-tools 协议。

**复用方式**：两条路可选，都不阻塞当前设计——
1. 给 Web/ACP 适配器复用 `Service`（不是本插件目标）；
2. 因为 `executable` 可配置，跨机"隔离委派"可以用**包一层 ssh 的可执行文件**跑 `rpi --mode rpc`，
   不必自研协议。跨机协作最终收敛为 relay 信箱（双向），无实例的远端机器标准做法是先跑一个 rpi 实例。

### `rpi-orchestrator` / `rpi-subagents`
有界 DAG / `ChildAgent` 子代理。**复用方式**：远端实例收到 `hub_ask` 后，自己可以再用 `delegate_task`
拆解——组合而非重复实现；本插件不再造 `tasks.json` 那套。

### `rpi-http` / `rpi-httpcore`、`rpi-acp`、`rpi-web`
HTTP/SSE 与 ACP 前端，负责 `ownsStdio`、监听生命周期、认证（`RPI_HTTP_PASSWORD`）。
**复用方式**：跨机鉴权在自研 relay 协议里（token），不需要它们的 HTTP 服务层；
如果要"给别人远程接进来"，直接让用户用 `rpi --http`，不要在我们的插件里再开一个 HTTP 面。

## 2. 因此做出的设计决定

| 需求 | 决定 |
| --- | --- |
| 多实例 presence / 信箱 / 租约 | **自研**（`rpi-hub-core`，SQLite WAL）。没有现成包，且这是本项目的核心价值 |
| 跨机传输 | JSON-RPC 2.0 + token 的**约定**借鉴 `rpi-server`，自研 relay（TCP，RPC 可选跑在 HTTP 表单端口；出站方向：办公机连服务器，服务器不反向拨号） |
| 身份/围栏 | 与 `rpi-node-core` 的 `node_id` + `owner_epoch` 对齐 |
| 微信通道 | 形状对齐 `rpi-im-message`（动作式工具、配置优先级、有界队列、autoReply）；协议自分块参考 pi-wechat 的 `splitAndFilterMarkdown` |
| 跨机调研（有实例） | `hub_send` / `hub_ask` 走信箱，保留对方会话上下文；双向对称（office⇄server 均可发起）|
| 跨机（无实例） | 先在远端跑一个 rpi 实例（relay client 连回主 hub），再 `hub_ask`——一次性 ssh 任务通道（`hub_run`）已移除，系统无 ssh 依赖 |

`hub_run` 覆盖没有实例在跑的机器（用远端自己的 `~/.rpi` 配置和网络）；有实例时优先 `hub_ask` 走信箱。relay 与 `hub_run` 均已真机验证。
