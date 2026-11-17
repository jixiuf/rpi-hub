# AGENTS.md

rpi 的多实例协同插件仓库：实例发现、互发消息/指令、任务委派、微信接管的可切换租约。
三个 crate：`rpi-hub-core`（共享库，无 ABI）、`rpi-hub` 与 `rpi-wechat`（cdylib 插件）。

## 常用命令

```bash
cargo build -p rpi-hub -p rpi-wechat          # 插件 dylib
cargo test -p rpi-hub-core -p rpi-hub --locked # 单测（45 core + 10 plugin）
scripts/smoke.sh <rpi 二进制路径>              # 真宿主端到端（见下：目前实例数断言失败）
rpi dev -P rpi-hub                            # 编译 + watch + 自动 reload
rpi dev-local -P rpi-hub                      # 只加载本扩展，隔离排查
```

## 约束（改代码前必读）

- 插件只准依赖 `rpi-plugin-sdk`，共享代码放 `rpi-hub-core`。不许链接宿主 crate。
- 每个 `extern "C"` 边界用 `guard`/`guard_or` 兜 panic；每个注册槽位都是 `Option`，旧宿主没有就降级。
- 回调预算见 `docs/plugin-authoring.md` §3：命令 handler 微秒级，事件只入队，网络和长事务一律丢 worker。
- 参数建议器是全量候选列表契约，不要按 `args` 过滤，见 `docs/plugin-authoring.md` §3.5。
- 参数、文本、队列、响应都要有上限。

## 格式化与 lint

本仓库整体不是 fmt-clean 的。改哪个 crate 就只格式化哪个：`cargo fmt -p rpi-hub`，然后用
`git diff --stat` 确认没碰别的文件，多余的 fmt-only 改动回退掉。clippy 以 main 为基线，
不新增告警即可（现存告警不必顺手修）。

## 已知坑

- macOS 上替换 `~/.rpi/agent/extensions/*.dylib` 必须先 `rm` 再 `cp`。原地覆盖保留 inode，
  新进程会映射到内核缓存的旧签名页，直接 SIGKILL（崩溃报告显示 CODESIGNING Invalid Page）。
- `scripts/smoke.sh` 当前失败是既有问题：wechat 插件自注册 `*-wechat` 实例行，每进程两行，
  脚本断言还停在每进程一行。修脚本或注册逻辑前，别把它当成新改动的回归。
- `Cargo.toml` 的 `rpi-plugin-sdk` 是 path 依赖，指向本地 pi-rust 检出（suggester 槽位未发版）。
  PR #338 合并发版后改回 `rpi-plugin-sdk = "0.25"`。跨机部署脚本会重写远端的这个 path。

## 文档

- `docs/architecture.md`：拓扑、SQLite schema、信封/租约语义、worker 周期、失败模式。
- `docs/plugin-authoring.md`：写 rpi 插件的全部约束，改插件代码前对照一遍。
- `docs/reuse-survey.md`：rpi-package 各包的复用结论，以及由此来的设计决定。
