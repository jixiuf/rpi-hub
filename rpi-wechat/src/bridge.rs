//! The WeChat bridge: one thread that owns the iLink session while this
//! instance holds the `wechat` lease.
//!
//! Loop (per tick, all on this thread):
//! 1. liveness gate — the OwnerLoop (owner.rs) holds the lease and renews
//!    it on its own clock; when it drops the flag, stop immediately.
//! 2. `getupdates` long poll (cursor + `seen` dedupe in the hub store).
//! 3. Authorized inbound routing (mirrors the reference implementation):
//!    - text ⇒ notice + inject (with accumulated files/images batch)
//!    - image ⇒ download, save, ack, start an 8s batch window
//!    - file ⇒ download, save, ack, accumulate (never auto-injected)
//! 4. Outbound queue: replies pushed by the event thread are chunked + sent.
//!
//! Failure modes: session expiry (`errcode -14`) stops the bridge and asks
//! for `/wechat login` (the OwnerLoop then releases the lease); transport
//! errors back off; stale context_token (-2) requeues undelivered replies
//! until the next inbound message refreshes it.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::error::IlinkError;
use crate::host;
use crate::host::Hub;
use crate::ilink::{self, FilePayload, IlinkClient, ImagePayload, UpdatesPage};
use crate::owner::FACT_POLL;
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
/// Mutable accumulation threaded through the inbound handlers: images/files
/// gathered while the user composes, flushed together on the next text.
#[derive(Default)]
pub struct BatchState {
    pub images: Vec<ImageData>,
    pub files: Vec<AccumulatedFile>,
}

// ---- per-kind inbound handlers (PR-3b): one async fn per message kind ----

/// Text flushes whatever accumulated with it (images/files) as one injection.
async fn handle_text(hub: Arc<Hub>, ctx: &BridgeContext, text: String, batch: &mut BatchState) {
    drain_inbound(
        hub,
        ctx,
        text,
        std::mem::take(&mut batch.images),
        std::mem::take(&mut batch.files),
    )
    .await;
}

/// Image: download, save, notify, accumulate for the next text message.
async fn handle_image(
    hub: Arc<Hub>,
    ctx: &BridgeContext,
    payload: ImagePayload,
    context_token: Option<&str>,
    batch: &mut BatchState,
) {
    let download = ctx.client.clone();
    let url = payload.url.clone();
    let key = payload.aes_key.clone();
    let media_dir = media_dir_of(&hub);
    let downloaded = async {
        let (bytes, _media_type) =
            crate::media::download_image(&download, &url, key.as_deref()).await?;
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
                    context_token,
                )
                .await;
            batch.images.push(ImageData {
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
                    context_token,
                )
                .await;
        }
    }
}

/// File: download, save, notify, accumulate (never auto-injected).
async fn handle_file(
    hub: Arc<Hub>,
    ctx: &BridgeContext,
    payload: FilePayload,
    context_token: Option<&str>,
    batch: &mut BatchState,
) {
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
    let media_dir = media_dir_of(&hub);
    let downloaded: Result<(Vec<u8>, PathBuf), IlinkError> = async {
        let bytes = crate::media::download_file(&download, &full_url, key.as_deref()).await?;
        let path = crate::media::save_to_disk(&media_dir, &name, &bytes)?;
        Ok((bytes, path))
    }
    .await;
    match downloaded {
        Ok((bytes, path)) => {
            batch.files.push(AccumulatedFile {
                name: name.clone(),
                size: bytes.len(),
                path: path.display().to_string(),
            });
            let _ = hub.notice(&format!("📨 微信文件已接收: {}", path.display()));
            let kb = (bytes.len() as f64 / 1024.0 * 10.0).round() / 10.0;
            let _ = ctx
                .client
                .send_text(
                    &ctx.user_id,
                    &format!("✅ 已保存: {name} ({kb} KB)。下一条文字消息会连同文件一起交给 agent"),
                    context_token,
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
                    context_token,
                )
                .await;
        }
    }
}

/// Voice: inject the server transcript (with accumulated media), or explain.
async fn handle_voice(
    hub: Arc<Hub>,
    ctx: &BridgeContext,
    transcript: Option<String>,
    context_token: Option<&str>,
    batch: &mut BatchState,
) {
    match transcript {
        Some(text) => handle_text(hub, ctx, format!("[语音转文字] {text}"), batch).await,
        None => {
            let _ = hub.notice("📨 微信语音：无转写文本");
            let _ = ctx
                .client
                .send_text(
                    &ctx.user_id,
                    "⚠ 语音没能转成文字，请改用文字或图片说明需求",
                    context_token,
                )
                .await;
        }
    }
}

/// Video: not supported; tell the user rather than dropping silently.
async fn handle_video(hub: Arc<Hub>, ctx: &BridgeContext, context_token: Option<&str>) {
    let _ = hub.notice("📨 微信视频已接收（暂不支持查看）");
    let _ = ctx
        .client
        .send_text(
            &ctx.user_id,
            "⚠ 视频暂不支持，请改用文字或图片说明需求",
            context_token,
        )
        .await;
}

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
    let mut batch = BatchState::default();
    // The mid-poll lease re-check is gone: the owner loop renews on its own
    // clock, so a 40s getupdates can no longer outlive the lease. The bridge
    // simply stops the moment the owner loop drops the liveness flag.
    let mut lease_alive = hub.lease_alive();

    loop {
        if hub.shutdown.requested() {
            return StopReason::Shutdown;
        }
        if !lease_alive {
            return StopReason::LeaseLost;
        }

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

        // ---- inbound: one abortable long poll ----
        // A mid-poll lease loss (a /use hand-off) aborts the HTTP request
        // immediately: the old owner exits at once and the cursor stays
        // put, so the new owner re-polls the same batch from the shared
        // cursor.
        match abortable_get_updates(&hub, &ctx.client, &cursor).await {
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
                    deliver(&hub, &ctx, incoming, context_token.as_deref(), &mut batch).await;
                }
            }
            Err(IlinkError::Cancelled) => {
                host::log("poll aborted: lease lost mid-poll; new owner re-polls the batch");
                return StopReason::LeaseLost;
            }
            Err(error) => {
                if error.is_session_expired() {
                    hub.record_error("wechat: session expired; run /wechat login");
                    // The owner loop must release the lease for us — otherwise
                    // it would keep renewing for a bridge that can never poll
                    // or send again ("actively holding while dead").
                    hub.set_bridge_stop(StopReason::SessionExpired);
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
            // message refreshes the context token. The OwnerLoop composes
            // the footer from this flag (💭 keeps the degraded shape).
            hub.set_degraded(true);
        } else if sent_count > 0 {
            // A successful send proves the token works again.
            hub.set_degraded(false);
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
        lease_alive = hub.lease_alive();
    }
}

/// Route one authorized inbound message (extracted from the poll loop so
/// the abortable-poll code stays readable).
async fn deliver(
    hub: &Arc<Hub>,
    ctx: &BridgeContext,
    incoming: crate::ilink::Incoming,
    context_token: Option<&str>,
    batch: &mut BatchState,
) {
    match incoming.kind.as_str() {
        "text" => {
            // Phone commands first (exact whitelist): they reply directly
            // over WeChat and never touch the agent lane or the batch.
            if let Some(command) = crate::wx_commands::parse(&incoming.text) {
                let reply = match crate::wx_commands::execute(hub, command) {
                    Ok(text) => text,
                    Err(error) => format!("❌ {error}"),
                };
                let _ = ctx
                    .client
                    .send_text(&ctx.user_id, &reply, context_token)
                    .await;
                return;
            }
            handle_text(Arc::clone(hub), ctx, incoming.text, batch).await;
        }
        "image" => {
            if let Some(payload) = incoming.image {
                handle_image(Arc::clone(hub), ctx, payload, context_token, batch).await;
            }
        }
        "file" => {
            if let Some(payload) = incoming.file {
                handle_file(Arc::clone(hub), ctx, payload, context_token, batch).await;
            }
        }
        "voice" => {
            handle_voice(Arc::clone(hub), ctx, incoming.voice, context_token, batch).await;
        }
        "video" => {
            handle_video(Arc::clone(hub), ctx, context_token).await;
        }
        _ => {}
    }
}

/// The bridge runs on its own thread with a tiny single-threaded runtime; this
/// helper keeps the await sites honest without dragging tokio into the crate.
async fn tokio_time(duration: Duration) {
    std::thread::sleep(duration);
}

/// One abortable `getupdates` long poll. The server holds the request up to
/// 40s; `lease_alive` can drop mid-poll (a `/use` hand-off), and the old
/// owner must stop IMMEDIATELY — not after the remaining poll time. The
/// watchdog is a real tokio sleep (registered waker, 200ms) — a bare
/// `poll_fn` without waker registration would only ever be polled as a
/// side effect of the HTTP future's IO events, with no timing guarantee.
/// Returns `Err(IlinkError::Cancelled)` when the lease died while waiting.
async fn abortable_get_updates(
    hub: &Arc<Hub>,
    client: &IlinkClient,
    cursor: &str,
) -> Result<UpdatesPage, IlinkError> {
    let request = client.get_updates(cursor);
    tokio::pin!(request);
    loop {
        tokio::select! {
            page = &mut request => return page,
            _ = tokio::time::sleep(FACT_POLL) => {
                if !hub.lease_alive() || hub.shutdown.requested() {
                    // The request future is dropped here: the HTTP call is
                    // aborted mid-flight. The cursor was NOT advanced, so
                    // the new owner re-polls from the shared cursor and
                    // receives this batch — no lost or misrouted messages.
                    return Err(IlinkError::Cancelled);
                }
                // Still alive: loop around and wait another slice.
            }
        }
    }
}

/// Store-level lease hand-off used by `/wechat stop`: release and stop.
/// The lease facade carries release; this is the only bridge-adjacent
/// release path and it runs outside the OwnerLoop (user request).
pub fn release_and_stop(hub: &Arc<Hub>) {
    if let Some(lease) = hub.lease.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
        let _ = hub.lease_owner.release("wechat", lease.generation);
    }
    hub.set_lease_alive(false);
    hub.request_shutdown();
}
