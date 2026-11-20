# rpi-mesh

rpi 的多实例协同：实例发现、互相发消息/任务委派，以及「哪个实例接微信」的可切换租约。
**单一插件**（`rpi-mesh`）：一个 rpi 进程里，presence、信箱、租约、微信通道都在同一个
dylib 里——旧的双插件（rpi-hub + rpi-wechat）各自静态链接 core，进程全局物两份，
是一整类协调 bug 的根源（跨 dylib 信号失效、双身份、双 footer 写者）。

设计目标来自实际用法：**一台办公机 + 一台服务器**；两台机器网络/环境不同，部分调研必须
交给位于对应网络里的实例完成；微信同一时刻只能接一个实例，但接管者要能切换。
跨机协作全程走 relay 信箱（双向：任何一侧实例都能 mesh_ask 另一侧），不依赖 ssh。

```
rpi-mesh/
├── rpi-mesh-core/   # 共享库（rlib，无 ABI）：身份、SQLite store、租约、信封协议、relay wire
└── rpi-mesh/        # 唯一 cdylib 插件：tools / 命令 / 事件 / worker / 租约 owner / 微信通道
    └── src/wechat/  # 通道实现：iLink 桥、OwnerLoop、手机命令、媒体工具
```

> **目录迁移**（pi-hub → mesh）：新版本使用 `~/.rpi/agent/mesh`（`mesh.db`）。
> 从旧布局升级请手动迁移一次：
> `mv ~/.rpi/agent/pi-hub ~/.rpi/agent/mesh && mv ~/.rpi/agent/mesh/hub.db ~/.rpi/agent/mesh/mesh.db`
> 环境变量改名：`RPI_HUB_DIR`→`RPI_MESH_DIR`、`RPI_HUB_INSTANCE`→`RPI_MESH_INSTANCE`、
> `RPI_HUB_DEBUG`→`RPI_MESH_DEBUG`（无兼容别名，按当前要求）。
>
> 个人 dots 配置里如果镜像了这份仓库（如 `~/vmacs/dots/rpi/agent/extensions`），那是开发副本；
> 正式源码以本仓库为准。

## 核心设计

| 问题 | 解决方式 |
| --- | --- |
| 多个实例如何共享状态 | 一个 SQLite 数据库，所有实例直接读写；没有 daemon、端口、锁文件 |
| 同一台机器的多个实例 | 共享同一个数据库文件，worker 每 300ms 拉取自己的消息 |
| 微信同时只能被一个实例服务 | 接管者定期续约；租约上有递增编号，旧持有者自动失效，不会出现两个实例同时收微信 |
| 微信从一个实例切到另一个 | `/use` 一条命令；新持有者立刻接管，旧持有者立刻停止，两边界面都会提示 |
| 一个实例崩溃或被杀 | 它的数据行带有进程号，其它实例能立即发现并清理；它持有的微信很快被别的实例接管 |
| 同时发多个 `/ask`，回复会不会串 | 每个任务独立追踪，结果回到发起它的那个会话；失败会明确告知原因 |

## 安装

```bash
# 开发：编译 + watch + 热重载（多包 workspace 需要 -P）
rpi dev -P rpi-mesh
rpi dev-local -P rpi-mesh      # 只加载本扩展，隔离调试

# 手动安装（macOS 务必先 rm 再 cp：原地覆盖同一 inode 会让新进程
# 映射到旧的签名页缓存，直接 SIGKILL）
cargo build --release -p rpi-mesh
rm ~/.rpi/agent/extensions/librpi_mesh.dylib
cp target/release/librpi_mesh.dylib ~/.rpi/agent/extensions/
# 或 make install
```

> 升级自旧双插件（rpi-hub + rpi-wechat）：先删掉
> `~/.rpi/agent/extensions/librpi_hub.dylib` 与 `librpi_wechat.dylib`（旧双插件），
> 否则旧插件会与新 mesh 在同一进程并存（schema v1/v2 混跑）。

## 工具（给模型用）

| 工具 | 作用 |
| --- | --- |
| `mesh_instances` | 列出实例（本机/远程、cwd、会话、忙闲）与微信接管者 |
| `mesh_send` | 把消息投进目标实例的 agent 输入队列 |
| `mesh_ask` | 委派任务并等待目标实例的最终回答 |
| `mesh_switch` | 把某个渠道（默认 `wechat`）的接管切到目标实例 |
| `mesh_start` | 启动新实例（缺省本机；指定另一实例名则在它所在的机器上启动） |
| `mesh_stop` | 停止实例（本机或远端自动路由） |
| `mesh_cmd` | 在另一实例上执行管理指令（status/model/thinking/name/stop/reload） |
|  | 对应斜杠命令：`/cmd <实例> <指令>`，TUI 与手机端均可 |
| `send_image_to_wechat` / `send_file_to_wechat` | 微信媒体外发（iLink CDN 加密上传） |

## 斜杠命令

```
/instances                 实例列表 + 当前微信接管
/use <实例|编号>            把微信接管切过去（含切回自己）
/msg <实例> <内容>          发消息（fire-and-forget，对方不回话）
/ask <实例> <任务>          委派任务（结果以通知形式回到本会话）
/start [目录]               本机 tmux 启动新实例（缺省=当前实例目录；允许同目录多实例）
/start <实例>:<目录>        跨机：在该实例所在机器上启动新实例
                            （实例 = /instances 列表里的名字或编号，
                              relay 机器上的任何实例都可作执行者）
/stop <实例|编号>           停止本机实例的 tmux（租约自动交还）
/stop <实例>:<实例|编号>    跨机：在该实例所在机器上停止目标实例
/cmd <实例> <指令>          跨机管理指令：status / model [id] / thinking <级别> /
                            name <文本> / stop / reload（会话类指令不支持跨机）
/mesh relay|client|local|token|status（跨机组网与管理）
/wechat login|start|stop|status
```

`/msg` `/ask` `/use` `/stop` `/start` `/cmd` 都注册了参数建议（宿主 `register_argument_suggester`）。

## 典型用法

两台机器：本机 `office`（办公室）、远端 `home@server`（机房，专职常驻实例）。

```bash
# 组网：server 上执行
/mesh relay 8085
# office 上执行（token 从 server 的 /mesh relay 输出里拿）
/mesh client <server-ip>:8085 <token>

# 查看全网实例（server 上的实例出现在列表里）
/instances

# 把微信交给 server 的实例（手机上从此直接和 home@server 对话）
/use home@server

# 委派调研：任务在 server 的环境里跑（内网、数据都在那边）
/ask home@server 查一下线上 orders 服务的最新发布记录

# 改远端实例的模型 / 会话名 / 停止它正在跑的生成
/cmd home@server model sonnet
/cmd home@server name orders-调研
/cmd home@server stop

# 在 server 上起一个新项目实例（目录在 server 上）
/start home@server:/home/admin/repos/api-server

# 停掉它（无论谁执行，目标所在机器自动路由）
/stop home@server:api-server

# 临时回本机处理微信
/use office
```

手机端同样支持：`/instances` 查看编号、`/use <编号>` 切换、`/start /stop` 本机操作、
普通文本直接对话——微信消息始终由当前持有者处理。

## 微信手机命令

```
/instances          实例列表（与 TUI 同序同格式）
/use <编号|名字>    切换微信接管；自动回显目标实例状态
/new                本实例开新会话
/status             本实例状态
/start [目录]       本机启动新实例（参数是路径，绝对路径可用）
/stop <编号|名字>   停止本机实例（微信租约自动交还）/help               命令列表
```

## 交互式组网（免改配置，立即生效）

不需要手编 config.json，两条命令完成组网——**切换立即生效**（无需 /reload）：

**relay 机器**（有公网/可达 IP 的那台）：

```
/mesh relay            # 本机成为 host：开始监听（默认端口 9527；/mesh relay 8085 指定）
                       # 输出：本机对外 IP、端口、token（供复制）
```

**其它机器**（家里/办公室）：

```
/mesh client <relay的IP>:<端口> <token>   # token 就是从 relay 输出里复制的那个
```

`/mesh status` 显示当前角色（host/guest/单机）、监听/连接地址、token 指纹（前 8 位，
两端比对用）。`/mesh local` 回单机。角色持久化，重启自动恢复。

**relay 机器建议跑一个专职常驻实例**（tmux while 循环保活）——当前监听者退出后，该机其它实例会自动接管监听：
常驻实例退出，同机其它实例 5s 内接管监听。两台机器不互通没关系——只要各自能连 relay。

## 状态与配置

```
~/.rpi/agent/mesh/
├── config.json
├── mesh.db              # SQLite（WAL），多进程共享；schema v4
├── relay-token
├── secret.key           # 静态加密密钥（0600）——见下「敏感数据加密」
├── logs/mesh.log logs/wechat.log
└── wechat/              # 微信凭据（0600）与媒体暂存
```

### 敏感数据加密

mesh.db 里的两类敏感数据在落盘前用 **AES-256-GCM** 加密：微信 bot token 与全部
信封正文（/msg /ask 的内容）。路由列（to_node/kind）保持明文，relay 无需解密即可转发。

密钥存在 DB 之外：优先读环境变量 `RPI_MESH_SECRET`（64 位 hex），否则用
`<mesh 目录>/secret.key`（首次启动自动生成，权限 0600）。连同一个 mesh.db 的
所有机器必须用同一把密钥——部署新机器时把 relay 的 `secret.key` 复制过去。
密钥丢了，加密的数据就解不开了：微信凭据要重新扫码登录，信封数据等它自然过期。

### 微信凭据自动同步

登录过微信的实例把凭据发布到 mesh.db（加密存储）；其它实例启动时自动拉取落盘，
不用重新扫码。`/use` 交接前会检查目标实例有没有凭据（presence 行的 channels 字段），
没有就拒绝——避免把微信交给一个收不到消息的实例。

```json
{
  "version": 1,
  "instanceName": null,
  "lease": { "ttlSeconds": 60, "renewSeconds": 5 },
  "remote": { "role": null, "relay": {} },
  "channels": { "wechat": { "enabled": true } },
  "limits": { "maxTextBytes": 65536, "inboxBatch": 32 },
  "instanceSpawn": { "sessionPrefix": "rpi", "command": "rpi", "tmuxCommand": "tmux" }
}
```

`lease.ttlSeconds` 必须 ≥ 45（要覆盖微信桥 40s 的 getupdates 长轮询）；`renewSeconds` ≤ ttl/4。

环境变量：`RPI_MESH_DIR`（状态目录）、`RPI_MESH_INSTANCE`（实例名）、`RPI_MESH_DEBUG=1`、`RPI_MESH_HOST`（主机名覆盖）。

**实例名默认 = 项目目录名**，`name@host` 天然区分「哪个项目在哪台机器」。同项目同机多会话
自动加 `-<pid>`。`/use home` 匹配顺序：完整名 → `name@host` → 去 pid 后缀 → 唯一前缀。

## 跨机（办公室 ⇄ 服务器，不依赖 ssh）

交互式组网（推荐，见上方「交互式组网」）：relay 机器 `/mesh relay [端口]`，其它机器
`/mesh client <ip>:<端口> <token>`。角色持久化，重启自动恢复。

两种线上格式：直连 JSON-RPC（首帧 authenticate）与 HTTP 表单端口（`http: true`，
`POST /mesh-rpc`，token 在 `X-Mesh-Auth`）——专为「防火墙只放行转发到真实 HTTP 服务」
的端口设计，nginx 可按路径反代。

- token 文件 0600、常数时间比较、日志不落明文；非回环监听启动时警告。
- client 断线指数退避重连（1→15s）并自动重认证；调用失败重试一次。
- 全网共享 relay 侧的唯一 mesh.db；完全无 ssh 依赖。
- **relay 机器上的监听会自动转移**：该机任何 mesh 实例都可接管监听（端口忙则 5s 重试，
  当前监听者退出后自动顶上）；建议 relay 机器跑一个专职常驻实例（tmux while 保活）。
- **故障影响面**：relay 失联时跨机功能停（明确挂，无脑裂），本机 agent/`/start` 不受影响；
  微信消息积压在腾讯服务端，恢复后由持有者自动续上。

详细见 `docs/architecture.md`。

## 验证

```bash
cargo test -p rpi-mesh-core -p rpi-mesh --locked   # 61 core + 25 mesh 单测
scripts/smoke.sh /path/to/rpi                      # 单机端到端：presence/重名/租约//use//ask 信箱
RELAY_PORT=8085 scripts/smoke-relay.sh HOST        # 跨机 relay 实测
```

见 `docs/architecture.md`、`docs/plugin-authoring.md`、`docs/reuse-survey.md`。
