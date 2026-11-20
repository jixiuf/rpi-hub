# AGENTS.md

rpi 的多实例协同插件仓库：实例发现、互发消息/指令、任务委派、微信接管的可切换租约。
**单一插件架构**（feat/mesh-redesign 起）：`rpi-mesh-core`（共享库，无 ABI）+
`rpi-mesh`（唯一 cdylib 插件，同时拥有 presence/信箱/租约/通道实现）。旧的双插件
（rpi-hub + rpi-wechat）已删除——双 dylib 各自静态链接 core，进程全局物两份，
是一整类协调 bug 的根源，见 docs/architecture.md §1。

## 常用命令

```bash
cargo build -p rpi-mesh                       # 插件 dylib
cargo test -p rpi-mesh-core -p rpi-mesh --locked # 单测（61 core + 27 plugin）
scripts/smoke.sh <rpi 二进制路径>              # 真宿主端到端（全绿）
rpi dev -P rpi-mesh                           # 编译 + watch + 自动 reload
rpi dev-local -P rpi-mesh                     # 只加载本扩展，隔离排查
```

## 约束（改代码前必读）

- 插件只准依赖 `rpi-plugin-sdk`，共享代码放 `rpi-mesh-core`。不许链接宿主 crate。
- **一个进程只有一份身份**：node_id/epoch/store/config 全部在 `mesh::Mesh`，
  `wechat::channel::Channel` 只包一层（`mesh` 字段）+ 通道自有状态。禁止再建第二份
  node_id/epoch/store。
- **wechat footer 只有两个写者**：owner 循环（持有中）与 watcher（非持有）。
- **租约只有一个写者**：`wechat::owner`（OwnerLoop）。桥只读 `lease_alive` 标志。
- 每个 `extern "C"` 边界用 `guard`/`guard_or` 兜 panic；每个注册槽位都是 `Option`，旧宿主没有就降级。
- 回调预算见 `docs/plugin-authoring.md` §3：命令 handler 微秒级，事件只入队，网络和长事务一律丢 worker。
- 参数建议器是全量候选列表契约，不要按 `args` 过滤，见 `docs/plugin-authoring.md` §3.5。
- 参数、文本、队列、响应都要有上限。
- **跨 dylib 不存在进程内信号**：一个 rlib 被两个 dylib 静态链接时全局物两份。
  mesh 是单 dylib，如果未来再拆插件，记住这个教训（Condvar 信号会静默失效）。
- **跨机角色状态机**（backends.rs）：host bind 失败自动降级 guest + 后台重试升级；
  guest（同机）失联自动尝试 bind 接管。改 backends 时不能破坏这个行为。
- **凭据同步**：登录实例发布到 mesh.db（加密），其它实例 bootstrap 拉取；presence 的
  `channels` 能力表是 `/use` 交接的校验依据——目标无能力必须拒绝。
- **敏感数据加密**：envelope payload 与凭据落盘前 AES-256-GCM 加密；密钥在 DB 外
  （env 或 secret.key），全网一致。新增敏感字段必须走 `secret::encrypt`。

## 格式化与 lint

本仓库整体不是 fmt-clean 的。改哪个 crate 就只格式化哪个：`cargo fmt -p rpi-hub`，然后用
`git diff --stat` 确认没碰别的文件，多余的 fmt-only 改动回退掉。clippy 以 main 为基线，
不新增告警即可（现存告警不必顺手修）。

## 重构 checklist（每轮结构改动后必走）

以"数据流生命周期"为单位审计，而不只看代码结构：

- **写者-续期者配对**：每个一次性注册/写入（presence 行、租约、文件状态）都要找到它的
  持续刷新者或删除者；找不到配对的，要么补上，要么删掉（channel 行 bug 即此类）。
- **外部输入的 serde 格式核对**：带 `rename_all` 的结构如果可能被外部写入（手放的 JSON、
  smoke seed、别的工具），字段名必须逐一核对——`snake_case` 撞上 `camelCase` 会静默
  parse 失败（smoke credentials bug 即此类）；`load()` 类函数的 parse 失败必须留日志。
- **grep 契约**：拆门面/trait 时同步写验收 grep（如租约裸方法在插件层零命中），合并前跑。

## 已知坑

- macOS 上替换 `~/.rpi/agent/extensions/*.dylib` 必须先 `rm` 再 `cp`。原地覆盖保留 inode，
  新进程会映射到内核缓存的旧签名页，直接 SIGKILL（崩溃报告显示 CODESIGNING Invalid Page）。
- smoke 用假 credentials 时字段必须是 camelCase（`userId`/`baseUrl`/`savedAt`）——
  `Credentials` 结构是 `serde(rename_all = "camelCase")`，snake_case 会静默 parse 失败、
  watcher 不启动、零日志（曾因此排查一整轮）。
- `Cargo.toml` 的 `rpi-plugin-sdk` 是 path 依赖，指向本地 pi-rust 检出（suggester 槽位未发版）。
  PR #338 合并发版后改回 `rpi-plugin-sdk = "0.25"`。跨机部署脚本会重写远端的这个 path。

## 文档

- `docs/architecture.md`：拓扑、SQLite schema、信封/租约语义、worker 周期、失败模式。
- `docs/plugin-authoring.md`：写 rpi 插件的全部约束，改插件代码前对照一遍。
- `docs/reuse-survey.md`：rpi-package 各包的复用结论，以及由此来的设计决定。
