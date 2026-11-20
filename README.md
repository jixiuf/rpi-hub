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

| 问题 | 机制 |
| --- | --- |
| 多进程共享状态 | 一个 SQLite（WAL）：`instances / leases / envelopes / seen`；leases 带 `state_json`（通道 cursor 与所有权原子移交） |
| 本地通信 | 同一个数据库，worker 300ms 拉自己的信箱；无 daemon/端口/锁文件 |
| 接管仲裁 | 租约 + generation fencing；TTL 60s（> 40s getupdates 长轮询），OwnerLoop 5s 续约 + 200ms 事实轮询 |
| 接管转移 | `/use` = assign 租约 + assign/unassign 信封 + generation fence；旧 owner 毫秒级感知 |
| 实例死亡 | presence 行带 pid：同 host 死进程 `kill(pid,0)` 立即清出列表、不再阻塞接管 |
| `/ask` 回链 | 宿主 `SendUserMessage` 同步返回 run 结果（status + 最终文本），correlation 精确绑定，绝不静默挂死 |
| 任务委派 | `mesh_ask`/`/ask` 委派目标实例，结果以通知回发起方 |

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
| `send_image_to_wechat` / `send_file_to_wechat` | 微信媒体外发（iLink CDN 加密上传） |

## 斜杠命令

```
/instances                 实例列表 + 当前微信接管
/use <实例|编号>            把微信接管切过去（含切回自己）
/msg <实例> <内容>          发消息（fire-and-forget，对方不回话）
/ask <实例> <任务>          委派任务（结果以通知形式回到本会话）
/start [目录]               本机 tmux 启动新实例（缺省=当前实例目录；允许同目录多实例）
/stop <实例|编号>           停止本机实例的 tmux（租约自动交还）
/mesh status|who|name <名>|leave
/wechat login|start|stop|status
```

`/msg` `/ask` `/use` `/stop` 都注册了参数建议（宿主 `register_argument_suggester`）。

## 微信手机命令

```
/instances          实例列表（与 TUI 同序同格式）
/use <编号|名字>    切换微信接管；自动回显目标实例状态
/new                本实例开新会话
/status             本实例状态
/start [目录]       本机启动新实例（参数是路径，绝对路径可用）
/stop <编号|名字>   停止本机实例（微信租约自动交还）
/help               命令列表
```

## 交互式组网（免改配置）

不需要手编 config.json，两条命令完成组网：

**relay 机器**（有公网/可达 IP 的那台）：

```
/mesh relay            # 转为 relay 模式（默认端口 9527；/mesh relay 8085 指定端口）
                       # 输出：本机对外 IP、端口、relay-token（供复制）
/reload                # 生效，开始监听
```

**其它机器**（家里/办公室）：

```
/mesh client <relay的IP>:<端口>    # 转为 client 模式（持久化到 config）
                                    # 提示 relay-token 是否就位；/mesh token 可查看
                                    # 把 relay 的 relay-token 复制到本机同路径
/reload                            # 生效，自动连接 relay
```

`/mesh status` 查看当前模式；`/mesh local` 回单机模式。角色持久化在 config.json，
重启后自动恢复。两台机器不互通没关系——只要各自能连 relay 即可。

## 状态与配置

```
~/.rpi/agent/mesh/
├── config.json
├── mesh.db              # SQLite（WAL），多进程共享；schema v2（leases.state_json）
├── relay-token
├── logs/mesh.log logs/wechat.log
└── wechat/              # 微信凭据（0600）与媒体暂存
```

```json
{
  "version": 1,
  "instanceName": null,
  "lease": { "ttlSeconds": 60, "renewSeconds": 5 },
  "remote": { "mode": "local", "relay": {} },
  "channels": { "wechat": { "enabled": true } },
  "limits": { "maxTextBytes": 65536, "inboxBatch": 32 },
  "instanceSpawn": { "sessionPrefix": "rpi", "command": "rpi", "tmuxCommand": "tmux" }
}
```

`lease.ttlSeconds` 必须 ≥ 45（要覆盖微信桥 40s 的 getupdates 长轮询）；`renewSeconds` ≤ ttl/4。
旧配置里的 `resumeGraceSeconds` / `autoClaim` 已废弃（serde 忽略未知键）。

环境变量：`RPI_MESH_DIR`（状态目录）、`RPI_MESH_INSTANCE`（实例名）、`RPI_MESH_DEBUG=1`、`RPI_MESH_HOST`（主机名覆盖）。

**实例名默认 = 项目目录名**，`name@host` 天然区分「哪个项目在哪台机器」。同项目同机多会话
自动加 `-<pid>`。`/use home` 匹配顺序：完整名 → `name@host` → 去 pid 后缀 → 唯一前缀。

## 跨机（办公室 ⇄ 服务器，不依赖 ssh）

relay 跑在服务器（`remote.mode=relay`，监听 TCP/unix socket），办公室以
`remote.mode=client` 直连。两种线上格式：直连 JSON-RPC（首帧 authenticate）与
HTTP 表单端口（`http: true`，`POST /mesh-rpc`，token 在 `X-Mesh-Auth`）——专为
「防火墙只放行转发到真实 HTTP 服务」的端口设计，nginx 可按路径反代。

- token 文件 0600、常数时间比较、日志不落明文；非回环监听启动时警告。
- client 断线指数退避重连（1→15s）并自动重认证；调用失败重试一次。
- 两机共享 relay 侧的唯一 hub.db；完全无 ssh 依赖。

详细见 `docs/architecture.md`。

## 验证

```bash
cargo test -p rpi-mesh-core -p rpi-mesh --locked   # 61 core + 25 mesh 单测
scripts/smoke.sh /path/to/rpi                      # 单机端到端：presence/重名/租约//use//ask 信箱
RELAY_PORT=8085 scripts/smoke-relay.sh HOST        # 跨机 relay 实测
```

见 `docs/architecture.md`、`docs/plugin-authoring.md`、`docs/reuse-survey.md`。
