//! Footer status helpers shared by the mesh and wechat plugins.
//!
//! The footer's wechat slot uses an emoji-bubble glyph family (see below).

/// The wechat-bubble emoji family: the SHAPE encodes the state (no ANSI —
/// emoji colors are fixed by the font, so shape vocabulary replaces color):
///
/// - 💬 speech balloon — held by THIS instance, connection healthy;
/// - 🗨️ left speech balloon (hollow/outlined) — held by ANOTHER instance;
/// - 💭 thought balloon — this instance holds the lease but the connection is
///   degraded (stale context token, waiting for the next inbound to refresh);
/// - 📴 mobile phone off — stopped / lost.
///
/// Width: 💬💭📴 are Wide (2 cols); 🗨️ needs VS16 for emoji presentation and
/// terminals disagree between 1–2 cols — accepted, the footer re-renders per
/// tick and a one-column drift only shifts the dim tail slightly.

/// text + 💬: held by this instance, connection healthy.
pub fn status_local(text: &str) -> String {
    format!("{text} 💬")
}

/// text + 🗨️: held by another instance; `holder` names the holder.
pub fn status_remote(holder: &str) -> String {
    format!("{holder} 🗨️")
}

/// text + 💭: this instance holds the channel but the connection is degraded
/// (stale context token — inbound messages refresh it).
pub fn status_degraded(text: &str) -> String {
    format!("{text} 💭")
}

/// text + 📴: stopped or lost.
pub fn status_stopped(text: &str) -> String {
    format!("{text} 📴")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glyphs_carry_the_shape_vocabulary() {
        let local = status_local("pi-rust@ljmacjxf");
        assert!(local.ends_with("💬"), "{local}");
        assert!(local.starts_with("pi-rust@"), "{local}");

        let remote = status_remote("jixiuf@ljmacjxf");
        assert!(remote.ends_with("🗨️"), "{remote}");

        let degraded = status_degraded("pi-rust@ljmacjxf");
        assert!(degraded.ends_with("💭"), "{degraded}");

        let stopped = status_stopped("停止");
        assert!(stopped.ends_with("📴"), "{stopped}");
    }
}
