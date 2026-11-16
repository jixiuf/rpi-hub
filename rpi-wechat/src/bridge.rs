//! The WeChat bridge: one thread that owns the iLink session while this
//! instance holds the `wechat` lease.
//!
//! Loop (per tick, all on this thread):
//! 1. `check_lease` — not the owner (or expired) ⇒ stop immediately; the
//!    worker thread that lost the lease must never touch WeChat again.
//! 2. `getupdates` long poll (cursor + `seen` dedupe in the hub store).
//! 3. Authorized inbound text ⇒ `SendUserMessage` (host runtime action) with
//!    `deliverAs:"steer"`; non-text kinds are surfaced as a notice.
//! 4. Outbound queue: replies pushed by the event thread (assistant text from
//!    `MessageEnd`) are sent as chunked text messages.
//!
//! Failure modes: session expiry (`errcode -14`) stops the bridge and asks for
//! `/wechat login`; transport errors back off exponentially; a `/reload` swaps
//! the plugin epoch, and the stale epoch's writes are fenced by the store.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rpi_hub_core::now_ms;

use crate::host::Hub;
use crate::error::IlinkError;
use crate::host;
use crate::ilink::{self, IlinkClient};

/// Backoff after a transport error.
const TRANSPORT_BACKOFF: Duration = Duration::from_secs(5);
/// Outbound chunk size: WeChat text messages past ~2k chars truncate
/// unreliably; the reference implementation caps chunks around 1.5k.
const CHUNK_CHARS: usize = 1_500;
/// Max chunks per assistant reply — the rest is dropped with a notice.
const MAX_CHUNKS: usize = 10;

/// Everything the bridge thread needs.
pub struct BridgeContext {
    pub client: IlinkClient,
    pub user_id: String,
    /// Initial cursor (persisted from the previous session, if any).
    pub cursor: String,
    /// Outbound queue: text chunks ready to send.
    pub outbound: std::sync::mpsc::Receiver<String>,
}

/// Result of one bridge run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// Lease lost — another instance took the channel.
    LeaseLost,
    /// Bot session expired — `/wechat login` required.
    SessionExpired,
    /// Shutdown requested (plugin unload, `/wechat stop`).
    Shutdown,
}

/// The chunking rule: split on paragraph boundaries when possible, hard-wrap
/// oversized paragraphs, and prefix continuation chunks `(i/n)`.
pub fn chunk_reply(text: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    for paragraph in text.split("\n\n") {
        let piece = paragraph.trim_end();
        if piece.is_empty() {
            continue;
        }
        // A paragraph that alone exceeds the cap is hard-wrapped.
        if piece.chars().count() > CHUNK_CHARS {
            if !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
            }
            let mut start = 0;
            let total = piece.chars().count();
            while start < total {
                let end = (start + CHUNK_CHARS).min(total);
                let slice: String = piece.chars().skip(start).take(end - start).collect();
                chunks.push(slice);
                start = end;
            }
            continue;
        }
        if current.chars().count() + piece.chars().count() + 2 > CHUNK_CHARS {
            chunks.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push_str("\n\n");
        }
        current.push_str(piece);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    if chunks.len() > MAX_CHUNKS {
        chunks.truncate(MAX_CHUNKS);
        chunks.push("…（后续内容已截断）".to_string());
    }
    // Number continuation chunks when there is more than one.
    if chunks.len() > 1 {
        let total = chunks.len();
        chunks = chunks
            .into_iter()
            .enumerate()
            .map(|(i, c)| format!("({}/{total})\n{c}", i + 1))
            .collect();
    }
    chunks
}

/// Filter: only messages from the bound user, deduped by message id.
pub fn authorized_and_new(
    msg: &ilink::Incoming,
    user_id: &str,
    seen: &mut HashSet<String>,
) -> bool {
    if msg.from_user != user_id {
        return false;
    }
    if msg.message_id.is_empty() {
        return true;
    }
    seen.insert(msg.message_id.clone())
}

/// Run the bridge loop until the lease is lost, the session expires, or
/// shutdown is requested. Returns why it stopped.
pub async fn run(
    hub: Arc<Hub>,
    ctx: BridgeContext,
    runtime: Arc<tokio::runtime::Runtime>,
) -> StopReason {
    let media_dir = |hub: &Arc<Hub>| -> PathBuf {
        let dir = crate::state::wechat_dir(&hub.dir).join("media");
        let _ = std::fs::create_dir_all(&dir);
        dir
    };
    let mut cursor = ctx.cursor;
    let mut seen: HashSet<String> = HashSet::new();
    let mut backoff = TRANSPORT_BACKOFF;
    let mut context_token: Option<String> = None;
    let mut typing_ticket: Option<String> = None;

    loop {
        if hub.shutdown.requested() {
            return StopReason::Shutdown;
        }

        // ---- lease gate + renewal: nothing outbound happens without ownership.
        // Renewal lives HERE (not on a side tick) so a running bridge keeps its
        // lease alive; losing it stops the bridge within one poll.
        let now = now_ms();
        let ttl = hub.config.lease.ttl_ms();
        let held = {
            let mut lease_slot = hub.lease.lock().unwrap_or_else(|p| p.into_inner());
            let Some(lease) = lease_slot.as_mut() else {
                return StopReason::LeaseLost;
            };
            let renewed = hub
                .store
                .renew_lease(
                    "wechat",
                    &hub.node_id,
                    &hub.epoch,
                    lease.generation,
                    now,
                    ttl,
                )
                .unwrap_or(false);
            if renewed {
                lease.expires_at = now + ttl;
                true
            } else {
                false
            }
        };
        if !held {
            hub.record_error("wechat: lease lost (fenced or expired)");
            return StopReason::LeaseLost;
        }
        hub.set_status(&format!("wx ✓ {}", hub.name()));

        // ---- inbound: one long poll ----
        match ctx.client.get_updates(&cursor).await {
            Ok(page) => {
                cursor = page.cursor;
                crate::state::save_cursor(&hub.dir, &cursor);
                backoff = TRANSPORT_BACKOFF;
                for raw in &page.msgs {
                    let Some(incoming) = ilink::normalize_msg(raw) else {
                        continue;
                    };
                    if !authorized_and_new(&incoming, &ctx.user_id, &mut seen) {
                        continue;
                    }
                    if !incoming.context_token.is_empty() {
                        context_token = Some(incoming.context_token.clone());
                    }
                    match incoming.kind.as_str() {
                        "text" => {
                            let text = incoming.text.clone();
                            // Transcript marker: the injected user message
                            // would otherwise read as a local prompt.
                            let _ = hub.notice(&format!("📨 微信: {text}"));
                            if let Err(error) =
                                hub.send_user_message(&text, Some("steer"))
                            {
                                hub.record_error(format!(
                                    "wechat: cannot inject message: {error}"
                                ));
                                let _ = ctx
                                    .client
                                    .send_text(
                                        &ctx.user_id,
                                        &format!("⚠ 消息投递失败: {error}"),
                                        context_token.as_deref(),
                                    )
                                    .await;
                                continue;
                            }
                            let _ = ctx
                                .client
                                .send_text(
                                    &ctx.user_id,
                                    "（已接收，处理中…）",
                                    context_token.as_deref(),
                                )
                                .await;
                        }
                        "image" => {
                            let Some(payload) = incoming.image.clone() else {
                                host::log("image branch: no payload");
                                continue;
                            };
                            host::log(&format!(
                                "image branch: url={} key={}",
                                payload.url,
                                payload.aes_key.is_some()
                            ));
                            let download = ctx.client.clone();
                            let url = payload.url.clone();
                            let key = payload.aes_key.clone();
                            let download_result = crate::media::download_image(
                                &download,
                                &url,
                                key.as_deref(),
                            )
                            .await;
                            let (bytes, _media_type) = match download_result {
                                Ok(pair) => pair,
                                Err(error) => {
                                    hub.record_error(format!("wechat image: {error}"));
                                    let _ = ctx
                                        .client
                                        .send_text(
                                            &ctx.user_id,
                                            "⚠ 图片下载失败",
                                            context_token.as_deref(),
                                        )
                                        .await;
                                    continue;
                                }
                            };
                            let image_result = crate::media::save_to_disk(
                                &media_dir(&hub),
                                "image.jpg",
                                &bytes,
                            );
                            match image_result {
                                Ok(path) => {
                                    host::log(&format!("image saved: {}", path.display()));
                                    let _ = hub.notice(&format!(
                                        "📨 微信图片已保存: {}",
                                        path.display()
                                    ));
                                    if let Err(error) = hub.send_user_message(
                                        &format!(
                                            "[微信图片已保存到 {}，可用 read 工具查看]",
                                            path.display()
                                        ),
                                        Some("steer"),
                                    ) {
                                        hub.record_error(format!(
                                            "wechat: inject image notice: {error}"
                                        ));
                                    }
                                }
                                Err(error) => {
                                    host::log(&format!("image download failed: {error}"));
                                    hub.record_error(format!("wechat image: {error}"));
                                    let _ = ctx
                                        .client
                                        .send_text(
                                            &ctx.user_id,
                                            "⚠ 图片下载失败",
                                            context_token.as_deref(),
                                        )
                                        .await;
                                }
                            }
                        }
                        "file" => {
                            let Some(payload) = incoming.file.clone() else {
                                continue;
                            };
                            let download = ctx.client.clone();
                            let param = payload.encrypt_param.clone();
                            let key = payload.aes_key.clone();
                            let name = payload.file_name.clone();
                            let download_result = crate::media::download_file(
                                &download,
                                &param,
                                key.as_deref(),
                            )
                            .await;
                            let bytes = match download_result {
                                Ok(bytes) => bytes,
                                Err(error) => {
                                    hub.record_error(format!("wechat file: {error}"));
                                    let _ = ctx
                                        .client
                                        .send_text(
                                            &ctx.user_id,
                                            "⚠ 文件下载失败",
                                            context_token.as_deref(),
                                        )
                                        .await;
                                    continue;
                                }
                            };
                            let file_result = crate::media::save_to_disk(
                                &media_dir(&hub),
                                &name,
                                &bytes,
                            );
                            match file_result {
                                Ok(path) => {
                                    let _ = hub.notice(&format!(
                                        "📨 微信文件已保存: {}",
                                        path.display()
                                    ));
                                    if let Err(error) = hub.send_user_message(
                                        &format!(
                                            "[微信文件已保存到 {}，文件名 {}]",
                                            path.display(),
                                            name
                                        ),
                                        Some("steer"),
                                    ) {
                                        hub.record_error(format!(
                                            "wechat: inject file notice: {error}"
                                        ));
                                    }
                                }
                                Err(error) => {
                                    hub.record_error(format!("wechat file: {error}"));
                                    let _ = ctx
                                        .client
                                        .send_text(
                                            &ctx.user_id,
                                            "⚠ 文件下载失败",
                                            context_token.as_deref(),
                                        )
                                        .await;
                                }
                            }
                        }
                        other => {
                            let _ = hub.notice(&format!("（微信收到{other}消息，暂不支持）"));
                        }
                    }
                }
            }
            Err(error) => {
                if error.is_session_expired() {
                    hub.record_error("wechat: session expired; run /wechat login");
                    return StopReason::SessionExpired;
                }
                hub.record_error(format!("wechat: getupdates failed: {error}"));
                hub.set_status("wx ⚠ 网络重试");
                tokio_time(backoff).await;
                continue;
            }
        }

        // ---- outbound: reply chunks queued by the event thread ----
        let mut sent_count = 0;
        while let Ok(chunk) = ctx.outbound.try_recv() {
            sent_count += 1;
            host::log(&format!("outbound sending chunk {} ({} chars)", sent_count, chunk.len()));
            if let Err(error) = ctx
                .client
                .send_text(&ctx.user_id, &chunk, context_token.as_deref())
                .await
            {
                hub.record_error(format!("wechat: send failed: {error}"));
            }
        }
        if sent_count > 0 {
            host::log(&format!("outbound drained {} chunks", sent_count));
        }

        // ---- typing ticket refresh (cheap, every loop) ----
        if let Some(token) = &context_token {
            if typing_ticket.is_none() {
                typing_ticket = ctx.client.typing_ticket(&ctx.user_id, token).await.ok().flatten();
            }
        }

        // No messages → short sleep before the next long poll.
        tokio_time(Duration::from_millis(300)).await;
    }
}

/// The bridge runs on its own thread with a tiny single-threaded runtime; this
/// helper keeps the await sites honest without dragging tokio into the crate.
async fn tokio_time(duration: Duration) {
    std::thread::sleep(duration);
}

/// Store-level lease hand-off used by `/wechat stop`: release and stop.
pub fn release_and_stop(hub: &Arc<Hub>) {
    if let Some(lease) = hub.lease.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
        let _ = hub.store.release_lease(
            "wechat",
            &hub.node_id,
            &hub.epoch,
            lease.generation,
        );
    }
    hub.request_shutdown();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_reply_keeps_short_replies_untouched() {
        assert_eq!(chunk_reply("hello"), vec!["hello"]);
        assert_eq!(chunk_reply("a\n\nb"), vec!["a\n\nb"]);
    }

    #[test]
    fn chunk_reply_splits_oversized_paragraphs_and_numbers() {
        let text = "x".repeat(CHUNK_CHARS + 500);
        let chunks = chunk_reply(&text);
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].starts_with("(1/2)\n"));
        assert!(chunks[1].starts_with("(2/2)\n"));
    }

    #[test]
    fn chunk_reply_splits_on_paragraph_boundaries_first() {
        let a = "a".repeat(CHUNK_CHARS - 10);
        let b = "b".repeat(CHUNK_CHARS - 10);
        let chunks = chunk_reply(&format!("{a}\n\n{b}"));
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].starts_with("(1/2)\naaaa"));
        assert!(chunks[1].starts_with("(2/2)\nbbbb"));
    }

    #[test]
    fn chunk_reply_caps_total_chunks() {
        // Paragraphs big enough to overflow MAX_CHUNKS individually.
        let unit = "x".repeat(CHUNK_CHARS);
        let text = std::iter::repeat(unit)
            .take(MAX_CHUNKS + 3)
            .collect::<Vec<_>>()
            .join("\n\n");
        let chunks = chunk_reply(&text);
        assert!(chunks.len() <= MAX_CHUNKS + 1, "got {}", chunks.len());
        assert!(chunks.last().unwrap().contains("截断"));
    }

    #[test]
    fn authorized_rejects_other_users_and_duplicates() {
        let msg = ilink::Incoming {
            message_id: "m1".into(),
            from_user: "u1".into(),
            context_token: String::new(),
            kind: "text".into(),
            text: "hi".into(),
            image: None,
            file: None,
        };
        let mut seen = HashSet::new();
        assert!(authorized_and_new(&msg, "u1", &mut seen));
        assert!(!authorized_and_new(&msg, "u2", &mut seen));
        assert!(!authorized_and_new(&msg, "u1", &mut seen));
    }
}
