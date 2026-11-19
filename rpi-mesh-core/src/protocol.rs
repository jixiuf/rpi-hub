//! The hub wire format: envelopes (mailbox items) and command intents.
//!
//! An envelope is a self-contained JSON document stored in the `envelopes`
//! table. Keeping the whole document in `payload` means the schema never has to
//! change when a new kind arrives; only the indexed routing columns are fixed.

use serde::{Deserialize, Serialize};

use crate::{HubError, Result};

/// Upper bound for a single text body (agent input or command output).
pub const MAX_TEXT_BYTES: usize = 64 * 1024;
/// Upper bound for command arguments.
pub const MAX_COMMAND_ARGS: usize = 32;
/// Time-to-live for ordinary text deliveries.
pub const TEXT_TTL_MS: i64 = 2 * 60 * 1000;
/// Time-to-live for command deliveries.
pub const COMMAND_TTL_MS: i64 = 2 * 60 * 1000;
/// Time-to-live for correlated replies (the caller may still be waiting).
pub const REPLY_TTL_MS: i64 = 10 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EnvelopeKind {
    /// Text for the target agent's input lane.
    Text,
    /// A command intent the target executes against its own session.
    Cmd,
    /// The result of a correlated request.
    Reply,
    /// A notice that is displayed but never fed to the model.
    Notice,
    /// A capability lease was assigned TO this node (`/use <target>`).
    /// Displayed, and the channel plugin reacts by claiming + starting its
    /// bridge immediately instead of waiting for its lease tick.
    Assign,
    /// A capability lease was taken AWAY from this node. Displayed so the
    /// losing bridge learns about the hand-off from a fact, not from its
    /// next failed renewal.
    Unassign,
}

impl EnvelopeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EnvelopeKind::Text => "text",
            EnvelopeKind::Cmd => "cmd",
            EnvelopeKind::Reply => "reply",
            EnvelopeKind::Notice => "notice",
            EnvelopeKind::Assign => "assign",
            EnvelopeKind::Unassign => "unassign",
        }
    }
}

/// A command the target instance executes locally through runtime actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandIntent {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

impl CommandIntent {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into().to_ascii_lowercase(),
            args: Vec::new(),
        }
    }

    pub fn with_args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    /// Parse `"/model sonnet"` / `"stop"` / `"/cmd target /reload"`.
    ///
    /// The leading slash is optional: the same text arrives from a TUI command
    /// and from an IM message.
    pub fn parse(text: &str) -> Result<Self> {
        let trimmed = text.trim();
        let trimmed = trimmed.strip_prefix('/').unwrap_or(trimmed);
        let mut parts = trimmed.split_whitespace();
        let name = parts
            .next()
            .ok_or_else(|| HubError::invalid("empty command"))?;
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        {
            return Err(HubError::invalid(format!("invalid command name {name:?}")));
        }
        let args: Vec<String> = parts.map(str::to_string).collect();
        if args.len() > MAX_COMMAND_ARGS {
            return Err(HubError::invalid("too many command arguments"));
        }
        Ok(Self {
            name: name.to_ascii_lowercase(),
            args,
        })
    }

    pub fn arg(&self, index: usize) -> Option<&str> {
        self.args.get(index).map(String::as_str)
    }

    /// Everything after the command name, re-joined (for free-text args such as
    /// a session name).
    pub fn rest(&self) -> String {
        self.args.join(" ")
    }
}

/// One mailbox item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Envelope {
    pub id: String,
    pub kind: EnvelopeKind,
    pub from_node: String,
    pub from_name: String,
    pub to_node: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandIntent>,
    /// For `reply`: whether the target considered the request successful.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    /// For `assign`/`unassign`: which capability moved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    /// For `assign`/`unassign`: the lease generation after the move.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    pub created_at: i64,
    pub expires_at: i64,
}

impl Envelope {
    fn base(
        id: String,
        kind: EnvelopeKind,
        from_node: &str,
        from_name: &str,
        to_node: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Self {
        Self {
            id,
            kind,
            from_node: from_node.to_string(),
            from_name: from_name.to_string(),
            to_node: to_node.to_string(),
            correlation: None,
            text: None,
            command: None,
            ok: None,
            capability: None,
            generation: None,
            created_at: now,
            expires_at: now + ttl_ms,
        }
    }

    pub fn text(
        from_node: &str,
        from_name: &str,
        to_node: &str,
        text: impl Into<String>,
        now: i64,
    ) -> Result<Self> {
        let text = text.into();
        check_text(&text)?;
        let mut env = Self::base(
            new_id(),
            EnvelopeKind::Text,
            from_node,
            from_name,
            to_node,
            now,
            TEXT_TTL_MS,
        );
        env.text = Some(text);
        Ok(env)
    }

    pub fn command(
        from_node: &str,
        from_name: &str,
        to_node: &str,
        command: CommandIntent,
        now: i64,
    ) -> Self {
        let mut env = Self::base(
            new_id(),
            EnvelopeKind::Cmd,
            from_node,
            from_name,
            to_node,
            now,
            COMMAND_TTL_MS,
        );
        env.command = Some(command);
        env
    }

    pub fn reply(
        from_node: &str,
        from_name: &str,
        to_node: &str,
        correlation: impl Into<String>,
        text: impl Into<String>,
        ok: bool,
        now: i64,
    ) -> Self {
        let text = text.into();
        let mut env = Self::base(
            new_id(),
            EnvelopeKind::Reply,
            from_node,
            from_name,
            to_node,
            now,
            REPLY_TTL_MS,
        );
        env.correlation = Some(correlation.into());
        env.text = Some(truncate(&text, MAX_TEXT_BYTES));
        env.ok = Some(ok);
        env
    }

    pub fn notice(
        from_node: &str,
        from_name: &str,
        to_node: &str,
        text: impl Into<String>,
        now: i64,
    ) -> Self {
        let text = text.into();
        let mut env = Self::base(
            new_id(),
            EnvelopeKind::Notice,
            from_node,
            from_name,
            to_node,
            now,
            TEXT_TTL_MS,
        );
        env.text = Some(truncate(&text, MAX_TEXT_BYTES));
        env
    }

    /// `capability` was assigned to `to_node` (generation `generation`).
    /// Sent by the assign path (`LeaseAdmin`) so the target starts serving
    /// the channel immediately. `capability` is short ("wechat"), so no
    /// truncation concerns; the text mirrors it for non-upgraded readers.
    pub fn assign(
        from_node: &str,
        from_name: &str,
        to_node: &str,
        capability: &str,
        generation: i64,
        now: i64,
    ) -> Self {
        let mut env = Self::base(
            new_id(),
            EnvelopeKind::Assign,
            from_node,
            from_name,
            to_node,
            now,
            COMMAND_TTL_MS,
        );
        env.capability = Some(capability.to_string());
        env.generation = Some(generation);
        env.text = Some(format!("{capability} 已交给你（generation {generation}）"));
        env
    }

    /// Mirror of [`Envelope::assign`] for the losing owner: `capability`
    /// moved to `new_owner_name` (generation `generation`).
    pub fn unassign(
        from_node: &str,
        from_name: &str,
        to_node: &str,
        new_owner_name: &str,
        capability: &str,
        generation: i64,
        now: i64,
    ) -> Self {
        let mut env = Self::base(
            new_id(),
            EnvelopeKind::Unassign,
            from_node,
            from_name,
            to_node,
            now,
            COMMAND_TTL_MS,
        );
        env.capability = Some(capability.to_string());
        env.generation = Some(generation);
        env.text = Some(format!(
            "{capability} 已移交 {new_owner_name}（generation {generation}）"
        ));
        env
    }

    pub fn with_correlation(mut self, correlation: impl Into<String>) -> Self {
        self.correlation = Some(correlation.into());
        self
    }

    /// One-line summary for logs and `/instances`-style output.
    pub fn summary(&self) -> String {
        match self.kind {
            EnvelopeKind::Cmd => format!(
                "cmd {:?} {}",
                self.command.as_ref().map(|c| c.name.as_str()).unwrap_or(""),
                self.command
                    .as_ref()
                    .map(CommandIntent::rest)
                    .unwrap_or_default()
            ),
            EnvelopeKind::Assign | EnvelopeKind::Unassign => format!(
                "{} {} gen {}",
                self.kind.as_str(),
                self.capability.as_deref().unwrap_or(""),
                self.generation.unwrap_or(0)
            ),
            _ => format!(
                "{} {} bytes",
                self.kind.as_str(),
                self.text.as_deref().map(str::len).unwrap_or(0)
            ),
        }
    }
}

fn check_text(text: &str) -> Result<()> {
    if text.len() > MAX_TEXT_BYTES {
        return Err(HubError::invalid(format!(
            "text is {} bytes, limit is {MAX_TEXT_BYTES}",
            text.len()
        )));
    }
    Ok(())
}

fn truncate(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_string();
    }
    let mut end = limit;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// `time-rand`, good enough for a mailbox key: ids only need to be unique, and
/// the store rejects duplicates.
pub fn new_id() -> String {
    format!("{}-{}", crate::now_ms(), uuid::Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_the_shapes_a_user_types() {
        assert_eq!(CommandIntent::parse("/reload").unwrap().name, "reload");
        assert_eq!(CommandIntent::parse("Stop").unwrap().name, "stop");
        let model = CommandIntent::parse("/model sonnet").unwrap();
        assert_eq!(model.name, "model");
        assert_eq!(model.arg(0), Some("sonnet"));
        let name = CommandIntent::parse("/name my project").unwrap();
        assert_eq!(name.rest(), "my project");
    }

    #[test]
    fn parse_rejects_empty_and_bad_names() {
        assert!(CommandIntent::parse("   ").is_err());
        assert!(CommandIntent::parse("/a;rm -rf").is_err());
    }

    #[test]
    fn text_envelope_round_trips_through_json() {
        let env = Envelope::text("n1", "home", "n2", "hello", 1000).unwrap();
        assert_eq!(env.expires_at, 1000 + TEXT_TTL_MS);
        let json = serde_json::to_string(&env).unwrap();
        let back: Envelope = serde_json::from_str(&json).unwrap();
        assert_eq!(back, env);
    }

    #[test]
    fn text_envelope_enforces_the_size_limit() {
        let too_big = "x".repeat(MAX_TEXT_BYTES + 1);
        assert!(Envelope::text("n1", "home", "n2", too_big, 0).is_err());
    }

    #[test]
    fn reply_truncates_instead_of_failing() {
        let long = "y".repeat(MAX_TEXT_BYTES + 10);
        let env = Envelope::reply("n1", "home", "n2", "corr", long, true, 0);
        assert!(env.text.unwrap().len() <= MAX_TEXT_BYTES + 4);
    }

    #[test]
    fn ids_are_unique() {
        let a = new_id();
        let b = new_id();
        assert_ne!(a, b);
    }

    #[test]
    fn assign_envelope_round_trips_through_json() {
        let env = Envelope::assign("n1", "home", "n2", "wechat", 7, 1000);
        assert_eq!(env.kind, EnvelopeKind::Assign);
        assert_eq!(env.capability.as_deref(), Some("wechat"));
        assert_eq!(env.generation, Some(7));
        let json = serde_json::to_string(&env).unwrap();
        let back: Envelope = serde_json::from_str(&json).unwrap();
        assert_eq!(back, env);

        let out = Envelope::unassign("n1", "home", "n3", "beta", "wechat", 7, 1000);
        assert_eq!(out.kind, EnvelopeKind::Unassign);
        assert!(out.text.as_deref().is_some_and(|t| t.contains("beta")));
    }
}
