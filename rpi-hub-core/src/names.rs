//! Instance identity and display names.
//!
//! Two independent things are derived here:
//! - **`node_id`** is stable for a *running process* and changes on restart:
//!   `sha256(host || cwd || pid)`. It must stay stable across `/reload`
//!   (same pid, same cwd), because the lease is keyed on it.
//! - **`name`** is the human label (`home`, `models`), sanitized and made
//!   unique per host by the store, which appends `-<pid>` on a collision.

use sha2::{Digest, Sha256};
use std::path::Path;

const MAX_NAME_LEN: usize = 64;

/// Stable per-process node identity.
pub fn node_id(host: &str, cwd: &Path, pid: u32) -> String {
    let mut hasher = Sha256::new();
    hasher.update(host.as_bytes());
    hasher.update([0u8]);
    hasher.update(cwd.to_string_lossy().as_bytes());
    hasher.update([0u8]);
    hasher.update(pid.to_string().as_bytes());
    let digest = hasher.finalize();
    digest.iter().take(16).map(|b| format!("{b:02x}")).collect()
}

/// Turn arbitrary text into a name the protocol and shell prompts tolerate.
pub fn sanitize(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len().min(MAX_NAME_LEN));
    for ch in raw.chars() {
        let keep = ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-');
        let mapped = if keep { ch } else { '-' };
        if mapped == '-' && out.ends_with('-') {
            continue;
        }
        out.push(mapped);
        if out.len() >= MAX_NAME_LEN {
            break;
        }
    }
    let trimmed = out.trim_matches(&['-', '.'][..]).to_string();
    if trimmed.is_empty() {
        "rpi".to_string()
    } else {
        trimmed
    }
}

/// Strip a previously appended `-<pid>` (or `-<pid>-<rand>`) suffix so a
/// re-registered instance does not grow `home-1234-5678-...` forever.
pub fn base_name(name: &str) -> String {
    let mut current = name.to_string();
    loop {
        let Some((head, tail)) = current.rsplit_once('-') else {
            return current;
        };
        let shorten = !tail.is_empty() && tail.len() <= 7 && tail.chars().all(|c| c.is_ascii_digit());
        if shorten {
            current = head.to_string();
        } else {
            return current;
        }
        if current.is_empty() {
            return name.to_string();
        }
    }
}

/// Default display name: the project directory's basename.
pub fn default_name(cwd: &Path) -> String {
    cwd.file_name()
        .map(|n| sanitize(&n.to_string_lossy()))
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| "rpi".to_string())
}

/// Candidate names to try, in order, when `name` collides on the same host.
///
/// The **configured** name always wins first: stripping a trailing `-<digits>`
/// up front would silently rename an instance that the user named
/// `relayhost-8080` (it looks exactly like a collision suffix). The stripped
/// base is only a later fallback, and `resolve` still matches it.
pub fn candidates(name: &str, pid: u32) -> Vec<String> {
    let literal = sanitize(name);
    let base = sanitize(&base_name(name));
    let mut out = vec![literal.clone()];
    if base != literal {
        out.push(base.clone());
    }
    out.push(sanitize(&format!("{base}-{pid}")));
    for attempt in 0..6u32 {
        out.push(sanitize(&format!("{base}-{pid}-{attempt}")));
    }
    out
}

/// Resolve a user-typed target (`home`, `2`, `home-1234`, `home@mac`) against
/// display names. Returns the index of the unique match, if any.
///
/// Matching order: exact `name`, exact `name@host`, exact base name, then a
/// unique prefix. Ambiguous input returns `Ok(None)` rather than guessing.
pub fn resolve<'a, I>(query: &str, names: I) -> std::result::Result<Option<usize>, usize>
where
    I: IntoIterator<Item = &'a str>,
{
    let query = query.trim();
    if query.is_empty() {
        return Ok(None);
    }
    let names: Vec<&str> = names.into_iter().collect();
    if let Some(index) = names.iter().position(|n| *n == query) {
        return Ok(Some(index));
    }
    if let Some((wanted, _host)) = query.split_once('@') {
        if let Some(index) = names.iter().position(|n| *n == wanted) {
            return Ok(Some(index));
        }
    }
    let base = base_name(query);
    let base_matches: Vec<usize> = names
        .iter()
        .enumerate()
        .filter(|(_, n)| base_name(n) == base)
        .map(|(i, _)| i)
        .collect();
    match base_matches.len() {
        1 => return Ok(Some(base_matches[0])),
        0 => {}
        n => return Err(n),
    }
    let matches: Vec<usize> = names
        .iter()
        .enumerate()
        .filter(|(_, n)| n.starts_with(query))
        .map(|(i, _)| i)
        .collect();
    match matches.len() {
        0 => Ok(None),
        1 => Ok(Some(matches[0])),
        n => Err(n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_keeps_safe_characters_and_collapses_dashes() {
        assert_eq!(sanitize("my project/微信"), "my-project");
        assert_eq!(sanitize("  --  "), "rpi");
        assert_eq!(sanitize("a.b_c-d"), "a.b_c-d");
    }

    #[test]
    fn base_name_strips_a_trailing_pid_chain() {
        assert_eq!(base_name("models"), "models");
        assert_eq!(base_name("models-1313"), "models");
        assert_eq!(base_name("models-1313-1234"), "models");
        assert_eq!(base_name("v1-2"), "v1");
    }

    #[test]
    fn candidates_keep_a_configured_numeric_suffix() {
        let names = candidates("relayhost-68122", 42);
        assert_eq!(names[0], "relayhost-68122");
        assert!(names.iter().any(|n| n == "relayhost-42"));
    }

    #[test]
    fn node_id_is_stable_for_the_same_process_inputs() {
        let a = node_id("mac", Path::new("/p"), 42);
        let b = node_id("mac", Path::new("/p"), 42);
        assert_eq!(a, b);
        assert_ne!(a, node_id("mac", Path::new("/p"), 43));
    }

    #[test]
    fn resolve_prefers_exact_then_base_then_unique_prefix() {
        let names = ["home-1234", "models"];
        assert_eq!(resolve("models", names.iter().copied()).unwrap(), Some(1));
        assert_eq!(resolve("home", names.iter().copied()).unwrap(), Some(0));
        assert_eq!(resolve("hom", names.iter().copied()).unwrap(), Some(0));
        assert_eq!(resolve("zzz", names.iter().copied()).unwrap(), None);
    }

    #[test]
    fn resolve_reports_ambiguity() {
        let names = ["home-1", "home-2"];
        assert_eq!(resolve("home", names.iter().copied()), Err(2));
    }
}
