//! Command intents a remote instance executes against its **own** session.
//!
//! This is the part pi-mesh implemented by injecting hidden slash commands into
//! the model's input lane (hidden slash commands). Here every intent maps to a host
//! runtime action, so it runs immediately, does not consume a model turn, and
//! reports a structured result.

use rpi_mesh_core::protocol::CommandIntent;
use rpi_plugin_sdk::RuntimeActionId;

use crate::mesh::Mesh;

pub const HELP: &str = "\
远程指令:
  status                 实例与会话状态
  new                    新会话
  fork                   从当前会话 fork
  switch <sessionId>     切换到指定会话
  compact                压缩上下文
  model <id>             切换模型
  thinking <level>       设置 thinking 级别
  name <文本>            设置会话名
  stop                   停止当前生成
  reload                 重载本实例扩展
  help                   本帮助";

pub fn execute(mesh: &Mesh, intent: &CommandIntent) -> Result<String, String> {
    match intent.name.as_str() {
        "help" => Ok(HELP.to_string()),
        "status" => status(mesh),
        "new" => {
            let result = mesh.action(RuntimeActionId::NewSession, serde_json::json!({}))?;
            Ok(format!(
                "{} 已开启新会话 {}",
                mesh.label(),
                session_id(&result)
            ))
        }
        "fork" => {
            let result = mesh.action(RuntimeActionId::Fork, serde_json::json!({}))?;
            Ok(format!(
                "{} 已 fork 出新会话 {}",
                mesh.label(),
                session_id(&result)
            ))
        }
        "switch" | "goto" | "session" => {
            let id = intent
                .arg(0)
                .ok_or_else(|| "用法: switch <sessionId>".to_string())?;
            let result = mesh.action(
                RuntimeActionId::SwitchSession,
                serde_json::json!({ "id": id }),
            )?;
            Ok(format!("已切换到会话 {}", session_id(&result)))
        }
        "compact" => {
            let custom = intent.rest();
            let args = if custom.is_empty() {
                serde_json::json!({})
            } else {
                serde_json::json!({ "customInstructions": custom })
            };
            mesh.action(RuntimeActionId::Compact, args)?;
            Ok("已完成压缩".to_string())
        }
        "model" => {
            let model = intent.rest();
            if model.is_empty() {
                let context = mesh.action(RuntimeActionId::GetExecutionContext, serde_json::json!({}))?;
                let current = context
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("(unknown)");
                return Ok(format!("当前模型: {current}\n用法: model <id>"));
            }
            let result = mesh.action(RuntimeActionId::SetModel, serde_json::json!({ "model": model }))?;
            let applied = result
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or(&model);
            Ok(format!("已切换模型: {applied}"))
        }
        "thinking" => {
            let level = intent.arg(0).ok_or_else(|| {
                "用法: thinking <off|minimal|low|medium|high|xhigh|max>".to_string()
            })?;
            mesh.action(
                RuntimeActionId::SetThinkingLevel,
                serde_json::json!({ "level": level }),
            )?;
            Ok(format!("thinking = {level}"))
        }
        "name" => {
            let name = intent.rest();
            if name.is_empty() {
                return Err("用法: name <会话名>".into());
            }
            mesh.action(RuntimeActionId::SetSessionName, serde_json::json!({ "name": name }))?;
            Ok(format!("会话名已设为 {name}"))
        }
        "stop" => {
            let result = mesh.action(RuntimeActionId::Abort, serde_json::json!({}))?;
            let aborted = result
                .get("aborted")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Ok(if aborted {
                "已停止当前生成".to_string()
            } else {
                "当前没有正在生成的内容".to_string()
            })
        }
        // Remote /start on THIS machine, requested by another instance via a
        // cmd envelope (cross-machine /start forward). Local-only spawn: the
        // presence confirmation runs against the shared store; placement is
        // decided here (this machine's tmux / detached).
        "spawn" => {
            let cwd = intent.arg(0).unwrap_or("");
            let spawn_ctx = crate::commands::spawn_ctx_from(mesh);
            rpi_mesh_core::instance_start::start(&spawn_ctx, (!cwd.is_empty()).then_some(cwd))
        }
        // Remote /stop on THIS machine: terminate the named local instance.
        // allow_self: the request may target this very instance (a remote
        // operator stopping the last instance on this machine) — that is a
        // normal, user-requested shutdown.
        "terminate" => {
            let query = intent.arg(0).ok_or("用法: terminate <实例|编号>")?;
            let spawn_ctx = crate::commands::spawn_ctx_from(mesh);
            rpi_mesh_core::instance_start::stop_with_options(
                &spawn_ctx,
                query,
                rpi_mesh_core::now_ms(),
                true,
            )
        }
        other => Err(format!("未知指令 {other:?}（可用 help 查看）")),
    }
}

fn status(mesh: &Mesh) -> Result<String, String> {
    let context = mesh
        .action(RuntimeActionId::GetExecutionContext, serde_json::json!({}))
        .unwrap_or(serde_json::Value::Null);
    let snapshot = mesh.session_snapshot();
    let session = context
        .get("sessionId")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or(snapshot.session_id)
        .unwrap_or_else(|| "(unknown)".to_string());
    let model = context
        .get("model")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or(snapshot.model)
        .unwrap_or_else(|| "(unknown)".to_string());
    let cwd = context
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or(snapshot.cwd)
        .unwrap_or_default();
    Ok(format!(
        "实例: {}\n会话: {session}\n模型: {model}\n目录: {cwd}\n状态: {}",
        mesh.label(),
        if mesh.busy() { "运行中" } else { "空闲" }
    ))
}

fn session_id(result: &serde_json::Value) -> String {
    result
        .get("sessionId")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)")
        .to_string()
}
