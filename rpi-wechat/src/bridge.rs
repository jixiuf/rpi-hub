//! The WeChat bridge: one thread that owns the iLink session while this
//! instance holds the `wechat` lease.
//!
//! Loop (per tick, all on this thread):
//! 1. `check_lease` + renew — not the owner (or expired) ⇒ stop immediately.
//! 2. `getupdates` long poll (cursor + `seen` dedupe in the hub store).
//! 3. Authorized inbound routing (mirrors the reference implementation):
//!    - text ⇒ notice + inject (with accumulated files/images batch)
//!    - image ⇒ download, save, ack, start an 8s batch window
//!    - file ⇒ download, save, ack, accumulate (never auto-injected)
//! 4. Outbound queue: replies pushed by the event thread are chunked + sent.
//!
//! Failure modes: session expiry (`errcode -14`) stops the bridge and asks for
//! `/wechat login`; transport errors back off; stale context_token (-2)
//! requeues undelivered replies until the next inbound message refreshes it.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rpi_hub_core::now_ms;

use crate::error::IlinkError;
use crate::host;
use crate::host::Hub;
use crate::ilink::{self, IlinkClient};

/// Backoff after a transport error.
const TRANSPORT_BACKOFF: Duration = Duration::from_secs(5);
/// Outbound chunk size: WeChat text messages past ~2k chars truncate
/// unreliably; the reference implementation caps chunks around 1.5k.
const CHUNK_CHARS: usize = 1_500;
/// Max chunks per assistant reply — the rest is dropped with a notice.
const MAX_CHUNKS: usize = 10;
/// An inline image ready for model injection (base64 JPEG/PNG).
#[derive(Debug, Clone)]
pub struct ImageData {
    pub data: String,
    pub media_type: String,
}

/// A file accumulated from WeChat, waiting to be referenced by the next
/// text message.
#[derive(Debug, Clone)]
pub struct AccumulatedFile {
    pub name: String,
    pub size: usize,
    pub path: String,
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

fn media_dir_of(hub: &Hub) -> std::path::PathBuf {
    let dir = crate::state::wechat_dir(&hub.dir).join("media");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

/// Drain the accumulated batch: inject text + image blocks + file notes into
/// the agent lane as ONE user message (matching the reference implementation's
/// content-blocks delivery). Pure-file batches are never injected.
async fn drain_inbound(
    hub: Arc<Hub>,
    ctx: &BridgeContext,
    text: String,
    images: Vec<ImageData>,
    files: Vec<AccumulatedFile>,
) {
    let mut parts: Vec<String> = Vec::new();
    if !images.is_empty() && text.is_empty() {
        parts.push(if images.len() == 1 {
            "请帮我分析这张图片".to_string()
        } else {
            format!("请帮我分析这 {} 张图片", images.len())
        });
    }
    if !text.is_empty() {
        parts.push(text.clone());
    }
    if !files.is_empty() {
        let file_infos = files
            .iter()
            .map(|f| {
                format!(
                    "- 文件「{}」({:.1} KB)，已保存到: {}",
                    f.name,
                    f.size as f64 / 1024.0,
                    f.path
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        parts.push(format!(
            "用户通过微信发送了 {} 个文件：\n{file_infos}",
            files.len()
        ));
    }
    let combined = parts.join("\n");

    let _ = hub.notice(&format!("📨 微信: {combined}"));
    if let Err(error) = hub.send_user_content(&combined, &images, Some("steer")) {
        hub.record_error(format!("wechat: inject batch failed: {error}"));
        let _ = ctx
            .client
            .send_text(&ctx.user_id, &format!("⚠ 消息注入失败: {error}"), None)
            .await;
    }
}

/// Result of one bridge run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    LeaseLost,
    SessionExpired,
    Shutdown,
}

/// Everything the bridge thread needs.
pub struct BridgeContext {
    pub client: IlinkClient,
    pub user_id: String,
    pub cursor: String,
    pub outbound: std::sync::mpsc::Receiver<String>,
}

/// Run the bridge loop until the lease is lost, the session expires, or
/// shutdown is requested.
pub async fn run(hub: Arc<Hub>, ctx: BridgeContext) -> StopReason {
    // Shared cursor from the DB: a hand-off mid-poll never replays or splits
    // messages, because whoever polls next continues from the advanced cursor.
    let mut cursor = hub
        .store
        .get_cursor()
        .or(Some(ctx.cursor.clone()))
        .unwrap_or_default();
    let mut seen: HashSet<String> = HashSet::new();
    let mut context_token: Option<String> = None;
    let mut typing_ticket: Option<String> = None;
    let mut pending_chunks: Vec<String> = Vec::new();
    let mut accumulated_files: Vec<AccumulatedFile> = Vec::new();
    let mut batch_images: Vec<ImageData> = Vec::new();

    loop {
        if hub.shutdown.requested() {
            hub.abort_renew_task();
            return StopReason::Shutdown;
        }

        // ---- lease gate + renewal ----
        let now = now_ms();
        let ttl = hub.config.lease.ttl_ms();
        let held = {
            let mut lease_slot = hub.lease.lock().unwrap_or_else(|p| p.into_inner());
            let Some(lease) = lease_slot.as_mut() else {
                return StopReason::LeaseLost;
            };
            let renewed = hub
                .lease_owner
                .renew("wechat", lease.generation, now)
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
        hub.set_status(&rpi_hub_core::status::status_local(&hub.label()));

        // ---- pending (stale-token) chunks first ----
        if !pending_chunks.is_empty() && context_token.is_some() {
            let mut undelivered = Vec::new();
            for chunk in pending_chunks.drain(..) {
                if let Err(error) = ctx
                    .client
                    .send_text(&ctx.user_id, &chunk, context_token.as_deref())
                    .await
                {
                    hub.record_error(format!("wechat: pending resend failed: {error}"));
                    undelivered.push(chunk);
                    break;
                }
            }
            pending_chunks = undelivered;
        }

        // ---- inbound: one long poll ----
        match ctx.client.get_updates(&cursor).await {
            Ok(page) => {
                cursor = page.cursor;
                hub.store.set_cursor(&cursor);
                crate::state::save_cursor(&hub.dir, &cursor);
                for raw in &page.msgs {
                    let Some(incoming) = ilink::normalize_msg(raw) else {
                        continue;
                    };
                    if !authorized_and_new(&incoming, &ctx.user_id, &mut seen) {
                        continue;
                    }
                    if !incoming.context_token.is_empty() {
                        context_token = Some(incoming.context_token.clone());
                        hub.set_context_token(Some(&incoming.context_token));
                    }
                    // Re-check the lease BEFORE injecting: a /use hand-off may
                    // have moved the lease while this poll was in flight (the
                    // long poll cannot be interrupted mid-flight). Without
                    // this the old instance would inject a message that
                    // belongs to the new owner.
                    let still_owner = {
                        let generation = hub
                            .lease
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .as_ref()
                            .map(|l| l.generation)
                            .unwrap_or(0);
                        hub.lease_owner.check("wechat", generation, now_ms())
                    };
                    if !still_owner {
                        host::log("lease lost mid-poll; dropping message for the new owner");
                        return StopReason::LeaseLost;
                    }
                    match incoming.kind.as_str() {
                        "text" => {
                            let text = incoming.text.clone();
                            drain_inbound(
                                Arc::clone(&hub),
                                &ctx,
                                text,
                                std::mem::take(&mut batch_images),
                                std::mem::take(&mut accumulated_files),
                            )
                            .await;
                        }
                        "image" => {
                            let Some(payload) = incoming.image.clone() else {
                                continue;
                            };
                            let download = ctx.client.clone();
                            let url = payload.url.clone();
                            let key = payload.aes_key.clone();
                            let media_dir = media_dir_of(&hub);
                            let downloaded = async {
                                let (bytes, _media_type) =
                                    crate::media::download_image(&download, &url, key.as_deref())
                                        .await?;
                                let data = crate::ilink::base64_encode(&bytes);
                                crate::media::save_to_disk(&media_dir, "image.jpg", &bytes)?;
                                Ok::<String, IlinkError>(data)
                            }
                            .await;
                            match downloaded {
                                Ok(data) => {
                                    let _ = hub.notice("📨 微信图片已接收");
                                    let _ = ctx
                                        .client
                                        .send_text(
                                            &ctx.user_id,
                                            "✅ 图片已接收（发送文字说明需求后一并交给 agent）",
                                            context_token.as_deref(),
                                        )
                                        .await;
                                    batch_images.push(ImageData {
                                        data,
                                        media_type: "image/jpeg".into(),
                                    });
                                }
                                Err(error) => {
                                    hub.record_error(format!("wechat image: {error}"));
                                    let _ = ctx
                                        .client
                                        .send_text(
                                            &ctx.user_id,
                                            &format!("⚠ 图片处理失败: {error}"),
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
                            host::log(&format!(
                                "file branch: param_len={} key={} name={}",
                                payload.encrypt_param.len(),
                                payload.aes_key.is_some(),
                                payload.file_name
                            ));
                            let download = ctx.client.clone();
                            let full_url = payload.full_url.clone();
                            let key = payload.aes_key.clone();
                            let name = payload.file_name.clone();
                            let name_for_save = name.clone();
                            let media_dir = media_dir_of(&hub);
                            let downloaded: Result<(Vec<u8>, PathBuf), IlinkError> = async {
                                let bytes = crate::media::download_file(
                                    &download,
                                    &full_url,
                                    key.as_deref(),
                                )
                                .await?;
                                let path =
                                    crate::media::save_to_disk(&media_dir, &name_for_save, &bytes)?;
                                Ok((bytes, path))
                            }
                            .await;
                            match downloaded {
                                Ok((bytes, path)) => {
                                    accumulated_files.push(AccumulatedFile {
                                        name: name.clone(),
                                        size: bytes.len(),
                                        path: path.display().to_string(),
                                    });
                                    let _ = hub
                                        .notice(&format!("📨 微信文件已接收: {}", path.display()));
                                    let kb = (bytes.len() as f64 / 1024.0 * 10.0).round() / 10.0;
                                    let _ = ctx
                                        .client
                                        .send_text(
                                            &ctx.user_id,
                                            &format!(
                                                "✅ 已保存: {name} ({kb} KB)。下一条文字消息会连同文件一起交给 agent"
                                            ),
                                            context_token.as_deref(),
                                        )
                                        .await;
                                }
                                Err(error) => {
                                    hub.record_error(format!("wechat file: {error}"));
                                    let _ = ctx
                                        .client
                                        .send_text(
                                            &ctx.user_id,
                                            &format!("⚠ 文件处理失败: {error}"),
                                            context_token.as_deref(),
                                        )
                                        .await;
                                }
                            }
                        }
                        "voice" => {
                            // The iLink server transcribes voice messages;
                            // inject the transcript like any other text (with
                            // a marker so the agent knows the origin). Without
                            // a transcript there is nothing to act on — tell
                            // the user instead of silently dropping it.
                            match incoming.voice.as_deref() {
                                Some(transcript) => {
                                    drain_inbound(
                                        Arc::clone(&hub),
                                        &ctx,
                                        format!("[语音转文字] {transcript}"),
                                        std::mem::take(&mut batch_images),
                                        std::mem::take(&mut accumulated_files),
                                    )
                                    .await;
                                }
                                None => {
                                    let _ = hub.notice("📨 微信语音：无转写文本");
                                    let _ = ctx
                                        .client
                                        .send_text(
                                            &ctx.user_id,
                                            "⚠ 语音没能转成文字，请改用文字或图片说明需求",
                                            context_token.as_deref(),
                                        )
                                        .await;
                                }
                            }
                        }
                        "video" => {
                            let _ = hub.notice("📨 微信视频已接收（暂不支持查看）");
                            let _ = ctx
                                .client
                                .send_text(
                                    &ctx.user_id,
                                    "⚠ 视频暂不支持，请改用文字或图片说明需求",
                                    context_token.as_deref(),
                                )
                                .await;
                        }
                        other => {
                            let _ = hub.notice(&format!("（微信收到{other}消息，暂不支持）"));
                        }
                    }
                }
            }
            Err(error) => {
                if error.is_session_expired() {
                    hub.abort_renew_task();
                    hub.record_error("wechat: session expired; run /wechat login");
                    return StopReason::SessionExpired;
                }
                hub.record_error(format!("wechat: getupdates failed: {error}"));
                std::thread::sleep(TRANSPORT_BACKOFF);
                continue;
            }
        }

        // ---- outbound: reply chunks queued by the event thread ----
        let mut sent_count = 0;
        let mut stale_token = false;
        while let Ok(chunk) = ctx.outbound.try_recv() {
            match ctx
                .client
                .send_text(&ctx.user_id, &chunk, context_token.as_deref())
                .await
            {
                Ok(()) => sent_count += 1,
                Err(IlinkError::Api { code: Some(-2), .. }) => {
                    stale_token = true;
                    pending_chunks.push(chunk);
                }
                Err(error) => {
                    hub.record_error(format!("wechat: send failed: {error}"));
                }
            }
        }
        if stale_token {
            context_token = None;
            let _ = ctx
                .client
                .send_text(
                    &ctx.user_id,
                    "⚠ 微信会话已过期：请在微信里随便回复一句（如 ok）以恢复，未送达的回复会自动补发",
                    None,
                )
                .await;
            // Degraded, not stopped: the lease is still ours and the bridge
            // runs, but replies cannot go out until the user's next inbound
            // message refreshes the context token. 🟡 keeps the holder color
            // vocabulary (yellow = degraded) without a second glyph.
            hub.set_status(&rpi_hub_core::status::status_degraded(&hub.label()));
        }
        if sent_count > 0 {
            host::log(&format!("outbound drained {} chunks", sent_count));
        }

        // ---- typing ticket refresh (cheap, every loop) ----
        if let Some(token) = &context_token {
            if typing_ticket.is_none() {
                typing_ticket = ctx
                    .client
                    .typing_ticket(&ctx.user_id, token)
                    .await
                    .ok()
                    .flatten();
            }
        }

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
        let _ = hub.lease_owner.release("wechat", lease.generation);
    }
    hub.request_shutdown();
}
