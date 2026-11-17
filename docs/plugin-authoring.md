# 怎么写一个 rpi 插件

面向本仓库的实现者，也适用于任何新的 rpi 扩展。内容按"能跑起来"的顺序排列，
每条约束都来自 rpi 源码（`crates/rpi-plugin-sdk`、`crates/rpi-extensions`、`crates/rpi-cli`）。

## 1. 形态

- 插件是 **Rust `cdylib`**，宿主用 `libloading` 加载，只查找一个符号 `rpi_plugin_register`。
- 依赖只允许 `rpi-plugin-sdk`（ABI 契约）。**不要**链接 `rpi-cli` / `rpi-extensions` / `rpi-harness`。
- 需要共享代码时用普通 `rlib`（本仓库的 `rpi-hub-core`），不要让它依赖宿主。

```toml
[package]
name = "my-ext"
edition = "2021"

[lib]
crate-type = ["cdylib", "rlib"]   # rlib 只为集成测试里有类型可用

[dependencies]
rpi-plugin-sdk = "=0.24"          # ABI v4；rpi 0.23/0.24 都是 v4
serde_json = "1"
```

## 2. 入口与能力检测

```rust
rpi_plugin_sdk::export_plugin!(|api| {
    // 每个注册槽位都是 Option：旧宿主没有就降级，不要 panic / 不要解引用空指针
    let Some(register_tool) = api.register_tool else { return -1 };
    // ... 注册
    0
});
```

可注册项（`PluginApi` 字段）：

`register_tool`、`register_tool_with_wake`、`register_command`、`register_shortcut`、
`register_flag`、`register_provider`、`register_streaming_provider`、
`register_message_renderer`、`register_markdown_transformer`、`register_entry_renderer`、
`register_event_handler`、`register_resources_discover`、`register_before_agent_start`、
`register_oauth_provider(_cancellable)`、`register_tool_result`、
`register_argument_suggester`、`register_plugin_lifecycle`、
`register_tool_prompt`、`runtime_action`、`declare`。

`register_argument_suggester` 在 vtable 末尾追加，旧宿主永远读不到这个槽位：插件必须把
`None` 当作"此宿主不支持参数建议"，静默降级。

## 3. 线程模型（最容易踩的坑）

| 回调 | 运行位置 | 预算 | 你能做什么 |
| --- | --- | --- | --- |
| 工具 `execute` | host blocking 线程 | — | 解析参数、建 handle |
| 工具 `poll` | 同上（循环驱动） | 单次必须快 | 非阻塞地推进状态；`partial_cb` 同步调用 |
| 事件 handler | host blocking 线程 | 5s/10s/15s | **只入队**，不要 IO |
| 斜杠命令 handler | **TUI 线程** | 人感知的"瞬间" | 几条 SQL；长任务丢 worker |
| 参数建议 handler | 宿主的建议 worker 线程 | 500ms，超时即弃 | 见 §3.5；保持便宜、重度缓存 |
| `before_agent_start` | 请求前 | — | 返回替换后的 systemPrompt |
| 生命周期 begin/finish | host 清理线程 | finish 决定能否卸载 | 停线程、flush；finish 后不能再有回调 |

所以本仓库的划分是：

- `src/worker.rs`：一个专用 `std::thread`，负责心跳、信箱派发、租约续期、清理。
- `src/events.rs`：只翻 `AtomicBool` / 压一个待回复队列。
- `src/commands.rs`：只做微秒级 SQLite 语句，返回 `{kind:"message",text}`。

## 3.5 参数建议（`register_argument_suggester`）

给斜杠命令注册参数候选（如 `/msg <实例>`），TUI 在参数区打字时弹补全。契约是**全量候选列表**：

- 宿主永远用 `{"command":"/msg","args":""}` 调用，按**命令名**缓存返回的完整列表，
  按输入前缀的收窄在宿主本地做。不要在插件里按 `args` 过滤——宿主的缓存是按命令存的，
  过滤过的列表会让用户删字符时丢候选。
- 每条路径恰好释放一次返回字符串（宿主拷贝后用你注册的 `plugin_free_string` 回收）。
- 响应上限：宿主拒绝超过 64 KiB 的响应，解析后最多 64 项、每字段 256 字符；插件自己也
  该设上限（本仓库 `suggest.rs`：32 项、256 字符）。
- 宿主侧每个命令同时至多一个调用在飞，刷新窗口内不重复调用，所以实现可以假定"便宜 +
  可缓存"：本仓库缓存实例列表 2s（心跳 3s 才变一次），client 模式下一次刷新就是一次 relay
  往返。

```rust
extern "C" fn suggest_arguments(
    request_json: StbStringRef,
    out: *mut StbString,
    _ud: *mut c_void,
) -> i32 {
    rpi_plugin_sdk::guard_or(1, || {
        let payload = build_full_list().unwrap_or_else(|| serde_json::json!({ "items": [] }));
        unsafe { *out = StbString::from_string(payload.to_string()); }
        0
    })
}
```

## 4. 工具四段式生命周期

```rust
rpi_plugin_sdk::execute_handler! {
    fn my_execute(_call_id, params) {          // params: OwnedString，宿主已把参数所有权交给你
        let text = params.to_string_lossy();
        MY_DRIVES.insert(Drive { /* ... */ })  // 返回 StepHandle（非复用 token，不是裸指针）
    }
}

extern "C" fn my_poll(handle, _partial_cb, _ud) -> StepResult {
    rpi_plugin_sdk::guard(|| {
        // 必须非阻塞；取消标志置位后尽快返回 Done/Err
        StepResult::done(StbString::from_string(r#"{"content":[{"type":"text","text":"ok"}],"details":{}}"#.into()))
    }).unwrap_or_else(|_| StepResult::err(StbString::from_string("panic".into())))
}

extern "C" fn my_cancel(handle: StepHandle) { MY_DRIVES.cancel(handle); }   // 幂等、不 free
extern "C" fn my_destroy(handle: StepHandle) { drop(MY_DRIVES.remove(handle)); } // 恰好一次
```

工具返回值是 `AgentToolResult` JSON：`{"content":[{"type":"text","text":...}],"details":{}}`。
失败用 `StepResult::err(msg)`（没有 `isError` 字段）。

参数里会多一个保留键 `__rpi`（`{cwd, sessionId}`，模型看不到），所以**不要**给参数结构加
`deny_unknown_fields`。

## 5. 调用宿主：runtime action

```rust
let out = (api.runtime_action)(
    RuntimeActionId::SendUserMessage as u32,
    StbStringRef::from_str(&json),
    &mut out_string,
    api.user_data,          // 注册时拿到的 opaque bridge 句柄
);
```

- `api.user_data` 是 `ActionBridge` 句柄；事件 handler 注册时把它作为 `user_data` 传进去，之后任何
  线程都能用它调 action。
- **`/reload` 会让旧句柄失效**（返回 `{"error":"expired/stale runtime handle"}`）；由于 reload 会重新
  加载 dylib 并重新 `register`，只要不在跨 load 的静态里长期缓存旧指针即可。
- 宿主产出的 `out` 字符串要用宿主的 `free_string`（`api.free_string`）回收；插件产出的字符串用
  `rpi_plugin_sdk::callbacks::free_string`。

常用 action：`SendUserMessage` / `SendMessage` / `AppendEntry` / `NewSession` / `Fork` /
`SwitchSession` / `Reload` / `Compact` / `SetModel` / `SetThinkingLevel` / `SetSessionName` /
`Abort` / `GetExecutionContext` / `SetStatus` / `UiDialog` / `ListCommands` / `ChildAgent`。

参数名（实测源码）：`NewSession {}`、`Fork {}`、`SwitchSession {id}`、`Compact {customInstructions?}`、
`SetModel {model}`、`SetThinkingLevel {level}`、`SetSessionName {name}`、`Abort {}`、
`GetExecutionContext {}`、`SetStatus {key,value}`、`SendUserMessage {text, triggerTurn?, deliverAs?}`。

## 6. 事件

标签是 `EventTag`（38 个）。本插件用到的：

| 事件 | payload | 用途 |
| --- | --- | --- |
| `AgentStart` / `AgentEnd` / `AgentSettled` | 空 | 忙闲标记 |
| `MessageEnd` | 整条消息 JSON | 取出 assistant 文本 → 回复 `hub_ask` |
| `TurnEndPayload` | `{outcome, stopReason, ...}` | `aborted/error` 时让等待方失败 |
| `SessionShutdown` | 空 | 请求 worker 退出 |

handler 里**不能 free** 事件字符串；用 `EventView` 的 accessor 拷贝需要的字段。

## 7. 生命周期与 `/reload`

- `/reload` 会加载新的 dylib、卸载旧的：**静态状态不跨 reload**。
- 必须用 `register_plugin_lifecycle(begin, finish, ud)`：
  - `begin`：只置停止标志（不要 join、不要回调宿主）。
  - `finish`：join 线程、清掉全局引用；返回 0 表示"再也不会有插件代码执行"，非 0 会让宿主永久保留
    这个库（用来避免卸载仍在运行的代码）。
- 本插件的做法：每次加载生成 `epoch`，所有 store 写入带 epoch；旧 epoch 的写入被 SQLite 条件更新
  拒绝（`WHERE node_id=? AND epoch=?`），因此旧线程即使晚一步也不会和新实例同时续租微信。

## 8. 开发与安装

```bash
rpi dev -P rpi-hub          # 编译 + watch + 自动 reload（多包 workspace 必须 -P）
rpi dev-local -P rpi-hub    # 只加载本扩展，隔离排查
cargo test                  # 纯 Rust 单测，不需要宿主
rpi install --path ./rpi-hub
```

`rpi install --path` 会执行 `cargo build --release --package <name>`，产物装到
`~/.rpi/agent/extensions`；`rpi --extensions-dir target/debug` 可在不安装的情况下试加载。

## 9. 发布前检查清单

- 所有能力槽位都做了 `Option` 判断；
- 每个 `extern "C"` 边界用 `guard`/`guard_or` 兜 panic；
- `poll` 不阻塞、可取消；`cancel` 幂等；`destroy` 恰好一次；
- 事件/命令回调里没有网络、没有长事务；
- 生命周期 `finish` 之后不再有线程运行；
- 参数、路径、文本长度、队列长度都有上限；
- 真实 `rpi` 加载过一次（`scripts/smoke.sh` 或 `rpi dev-local`）。
