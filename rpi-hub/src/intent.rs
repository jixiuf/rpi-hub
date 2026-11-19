//! Command intents a remote instance executes against its **own** session.
//!
//! This is the part pi-hub implemented by injecting hidden slash commands into
//! the model's input lane (`/__hub_reload`). Here every intent maps to a host
//! runtime action, so it runs immediately, does not consume a model turn, and
//! reports a structured result.

use rpi_hub_core::protocol::CommandIntent;
use rpi_plugin_sdk::RuntimeActionId;

use crate::host::Hub;

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

pub fn execute(hub: &Hub, intent: &CommandIntent) -> Result<String, String> {
    match intent.name.as_str() {
        "help" => Ok(HELP.to_string()),
        "status" => status(hub),
        "new" => {
            let result = hub.action(RuntimeActionId::NewSession, serde_json::json!({}))?;
            Ok(format!(
                "{} 已开启新会话 {}",
                hub.label(),
                session_id(&result)
            ))
        }
        "fork" => {
            let result = hub.action(RuntimeActionId::Fork, serde_json::json!({}))?;
            Ok(format!(
                "{} 已 fork 出新会话 {}",
                hub.label(),
                session_id(&result)
            ))
        }
        "switch" | "goto" | "session" => {
            let id = intent
                .arg(0)
                .ok_or_else(|| "用法: switch <sessionId>".to_string())?;
            let result = hub.action(
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
            hub.action(RuntimeActionId::Compact, args)?;
            Ok("已完成压缩".to_string())
        }
        "model" => {
            let model = intent.rest();
            if model.is_empty() {
                let context = hub.action(RuntimeActionId::GetExecutionContext, serde_json::json!({}))?;
                let current = context
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("(unknown)");
                return Ok(format!("当前模型: {current}\n用法: model <id>"));
            }
            let result = hub.action(RuntimeActionId::SetModel, serde_json::json!({ "model": model }))?;
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
            hub.action(
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
            hub.action(RuntimeActionId::SetSessionName, serde_json::json!({ "name": name }))?;
            Ok(format!("会话名已设为 {name}"))
        }
        "stop" => {
            let result = hub.action(RuntimeActionId::Abort, serde_json::json!({}))?;
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
        other => Err(format!("未知指令 {other:?}（可用 help 查看）")),
    }
}

fn status(hub: &Hub) -> Result<String, String> {
    let context = hub
        .action(RuntimeActionId::GetExecutionContext, serde_json::json!({}))
        .unwrap_or(serde_json::Value::Null);
    let snapshot = hub.session_snapshot();
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
        hub.label(),
        if hub.busy() { "运行中" } else { "空闲" }
    ))
}

fn session_id(result: &serde_json::Value) -> String {
    result
        .get("sessionId")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)")
        .to_string()
}
