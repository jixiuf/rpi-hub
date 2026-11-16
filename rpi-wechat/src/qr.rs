//! QR rendering for `/wechat login`: the iLink API returns a **login URL**
//! (`qrcode_img_content`), not an image. We render it as compact terminal
//! ASCII (half-block pairs, like `qrcode-terminal`'s small mode) so WeChat can
//! scan it straight off the screen.

use qrcode::types::Color;

/// Render `text` as terminal half-block QR (2 rows per character line).
pub fn render_terminal(text: &str) -> Result<String, String> {
    let code = qrcode::QrCode::with_error_correction_level(text.as_bytes(), qrcode::types::EcLevel::M)
        .map_err(|e| format!("qr encode failed: {e}"))?;
    let width = code.width();
    let colors: Vec<bool> = code.to_colors().into_iter().map(|c| c == Color::Dark).collect();
    let cell = |x: usize, y: usize| colors.get(y * width + x).copied().unwrap_or(false);

    let mut out = String::new();
    // Half-block: ▲ top cell dark / bottom light — combine two rows per line.
    let mut y = 0;
    while y < width {
        let y2 = y + 1;
        for x in 0..width {
            let top = cell(x, y);
            let bottom = y2 < width && cell(x, y2);
            let glyph = match (top, bottom) {
                (true, true) => '█',
                (true, false) => '▀',
                (false, true) => '▄',
                (false, false) => ' ',
            };
            out.push(glyph);
        }
        out.push('\n');
        y += 2;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_multiline_output_with_wellformed_width() {
        let rendered = render_terminal("https://example.com/login").unwrap();
        let lines: Vec<&str> = rendered.trim_end().split('\n').collect();
        assert!(lines.len() >= 10, "qr too small: {}", lines.len());
        let width = lines[0].chars().count();
        assert!(lines.iter().all(|l| l.chars().count() == width));
    }

    #[test]
    fn rejects_impossible_input() {
        // An empty payload still encodes, but a huge one must fail cleanly.
        let huge = "x".repeat(10_000);
        assert!(render_terminal(&huge).is_err());
    }
}
