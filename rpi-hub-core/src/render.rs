//! Shared `/instances` rendering for the TUI command and the WeChat phone
//! commands. One implementation so the numbered list a user sees in the TUI
//! and the one they see on the phone always agree — `/use <编号>` on either
//! side anchors to THIS ordering (list_instances order: presence rows as the
//! store returns them, deduped; the store's ORDER BY is stable).

use crate::api::HubStore;
use crate::store::InstanceView;

/// One rendered row: `1. name@host（标注） 忙  /cwd`.
pub struct InstanceRow {
    /// 1-based position (matches `/use <编号>`).
    pub number: usize,
    pub view: InstanceView,
    pub line: String,
}

/// The instance the viewer IS (`node_id == viewer`); another viewer (a phone
/// or the other instance) sees different rows marked 当前.
pub struct RenderContext<'a> {
    pub viewer_node_id: &'a str,
    /// Holder node of the `wechat` capability, if any (drives the 微信 mark).
    pub wechat_holder_node: Option<&'a str>,
    /// Whether cross-machine mode is on (adds the host suffix always; in
    /// local mode the cwd line already disambiguates).
    pub remote: bool,
}

/// Render the numbered instance list. `instances` comes from
/// `store.list_instances(now, ALIVE_MS)` (the caller owns the TTL policy).
pub fn render_instances(
    instances: &[InstanceView],
    viewer: &RenderContext<'_>,
) -> Vec<InstanceRow> {
    instances
        .iter()
        .enumerate()
        .map(|(index, instance)| {
            let mut marks: Vec<&str> = Vec::new();
            if instance.node_id == viewer.viewer_node_id {
                marks.push("当前");
            }
            if instance.role == "subagent" {
                marks.push("subagent");
            }
            if viewer
                .wechat_holder_node
                .is_some_and(|holder| holder == instance.node_id)
            {
                marks.push("微信");
            }
            if let Some(title) = instance.title.as_deref().filter(|t| !t.is_empty()) {
                marks.push(title);
            }
            let busy = if instance.busy { " 忙" } else { "" };
            let mark = if marks.is_empty() {
                String::new()
            } else {
                format!("（{}）", marks.join("，"))
            };
            let line = format!(
                "{}. {}{}{}{}",
                index + 1,
                instance.label(),
                mark,
                busy,
                if instance.cwd.is_empty() {
                    String::new()
                } else {
                    format!("  {}", instance.cwd)
                }
            );
            InstanceRow {
                number: index + 1,
                view: instance.clone(),
                line,
            }
        })
        .collect()
}

/// The full `/instances` text: header + rows + the lease footer line.
/// `holder` is `Some((owner display name, generation, expires_at))`.
pub fn render_instances_text(
    instances: &[InstanceView],
    viewer: &RenderContext<'_>,
    now: i64,
    holder: Option<(String, i64, i64)>,
) -> String {
    if instances.is_empty() {
        return "没有登记的实例".to_string();
    }
    let mut lines: Vec<String> = vec!["实例列表：".to_string()];
    lines.extend(render_instances(instances, viewer).into_iter().map(|r| r.line));
    match holder {
        Some((owner, generation, expires_at)) => lines.push(format!(
            "微信接管: {}（generation {}，剩余 {}s）",
            owner,
            generation,
            (expires_at - now) / 1000
        )),
        None => lines.push("微信接管: (无)".to_string()),
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(name: &str, node: &str, cwd: &str, busy: bool) -> InstanceView {
        InstanceView {
            node_id: node.into(),
            name: name.into(),
            host: "h1".into(),
            pid: 1,
            cwd: cwd.into(),
            role: "normal".into(),
            session_id: None,
            title: None,
            model: Some("gpt-x".into()),
            busy,
            started_at: 0,
            last_seen: 0,
        }
    }

    #[test]
    fn rows_are_numbered_and_marked_with_viewer_and_holder() {
        let instances = vec![
            view("alpha", "node-a", "/work/a", false),
            view("beta", "node-b", "/work/b", true),
            view("gamma", "node-c", "", false),
        ];
        let viewer = RenderContext {
            viewer_node_id: "node-b",
            wechat_holder_node: Some("node-a"),
            remote: false,
        };
        let rows = render_instances(&instances, &viewer);
        assert_eq!(rows[0].number, 1);
        assert!(rows[0].line.contains("1. alpha@h1（微信）"));
        assert!(rows[1].line.contains("2. beta@h1（当前） 忙"));
        assert!(rows[2].line.contains("3. gamma@h1"));
        assert!(rows[2].line.ends_with("/work/a") == false);
    }

    #[test]
    fn text_renders_header_rows_and_lease_footer() {
        let instances = vec![view("alpha", "node-a", "/work/a", false)];
        let viewer = RenderContext {
            viewer_node_id: "node-a",
            wechat_holder_node: Some("node-a"),
            remote: false,
        };
        let text = render_instances_text(&instances, &viewer, 10_000, Some(("a@h1".into(), 3, 25_000)));
        assert!(text.starts_with("实例列表："));
        assert!(text.contains("1. alpha@h1（当前，微信）"));
        assert!(text.contains("微信接管: a@h1（generation 3，剩余 15s）"));

        let empty = render_instances_text(&[], &viewer, 0, None);
        assert_eq!(empty, "没有登记的实例");
    }

    #[test]
    fn holder_on_missing_instance_still_renders_footer() {
        let instances = vec![view("alpha", "node-a", "/a", false)];
        let viewer = RenderContext {
            viewer_node_id: "node-a",
            wechat_holder_node: None,
            remote: true,
        };
        let text = render_instances_text(&instances, &viewer, 0, None);
        assert!(text.contains("微信接管: (无)"));
    }
}
