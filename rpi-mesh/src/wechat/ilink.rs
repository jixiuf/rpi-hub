//! WeChat iLink Bot HTTP API.
//!
//! Protocol reference (mirrors the proven TypeScript implementation):
//! - base URL `https://ilinkai.weixin.qq.com`, JSON bodies
//! - headers: `AuthorizationType: ilink_bot_token`,
//!   `Authorization: Bearer <token>`, `X-WECHAT-UIN: <base64>` (random, stable
//!   per process), `Content-Type: application/json`
//! - responses envelope errors as `ret != 0` or `errcode != 0`; `errcode -14`
//!   means the bot session expired (re-login required); `sendmessage` can
//!   answer `ret -2` for a stale `context_token` — retry once without it.
//!
//! Endpoints used here: `getupdates` (40 s long poll), `sendmessage`,
//! `getconfig` (typing ticket), `sendtyping`, `msg/notifystart|stop`,
//! `get_bot_qrcode`, `get_qrcode_status`, `getuploadurl` + CDN upload.
//! Media crypto (AES-128-ECB) lives in [`super::media`].

use std::time::Duration;

use serde_json::Value;

use crate::wechat::error::IlinkError;

pub const DEFAULT_BASE_URL: &str = "https://ilinkai.weixin.qq.com";
const CHANNEL_VERSION: &str = "1.0.0";
const CLIENT_VERSION_HEADER: &str = "iLink-App-ClientVersion";
/// Bounded CDN payloads (a runaway download must not exhaust memory).
const MAX_DOWNLOAD_BYTES: u64 = 20 * 1024 * 1024;

/// Transport + auth state for one bot session.
#[derive(Clone)]
pub struct IlinkClient {
    pub http: reqwest::Client,
    pub base_url: String,
    pub token: String,
    /// Random per-process UIN header (base64 of a random u32), stable across
    /// calls within this process like the reference implementation.
    uin: String,
}

impl std::fmt::Debug for IlinkClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IlinkClient")
            .field("base_url", &self.base_url)
            .field("token", &"***")
            .finish()
    }
}

impl IlinkClient {
    pub fn new(base_url: &str, token: &str) -> Result<Self, IlinkError> {
        let uin = {
            use rand::RngCore;
            let mut bytes = [0u8; 4];
            rand::thread_rng().fill_bytes(&mut bytes);
            base64_encode(&bytes)
        };
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(45))
                .build()
                .map_err(|e| IlinkError::Transport(e.to_string()))?,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
            uin,
        })
    }

    fn headers(&self, extra: &[(&str, &str)]) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert("AuthorizationType", "ilink_bot_token".parse().unwrap());
        headers.insert(
            "Authorization",
            format!("Bearer {}", self.token).parse().unwrap(),
        );
        headers.insert("X-WECHAT-UIN", self.uin.parse().unwrap());
        for (name, value) in extra {
            headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())
                    .expect("static header name"),
                value.parse().expect("static header value"),
            );
        }
        headers
    }

    async fn post(&self, path: &str, body: Value, timeout: Duration) -> Result<Value, IlinkError> {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .http
            .post(&url)
            .headers(self.headers(&[]))
            .json(&body)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| IlinkError::Transport(e.to_string()))?;
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .map_err(|e| IlinkError::Transport(e.to_string()))?;
        Self::parse_response(status, &text, path)
    }

    async fn get(&self, path: &str, extra: &[(&str, &str)]) -> Result<Value, IlinkError> {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .http
            .get(&url)
            .headers(self.headers(extra))
            .send()
            .await
            .map_err(|e| IlinkError::Transport(e.to_string()))?;
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .map_err(|e| IlinkError::Transport(e.to_string()))?;
        Self::parse_response(status, &text, path)
    }

    /// Envelope check shared by POST/GET: HTTP error, `ret != 0`, `errcode != 0`.
    fn parse_response(status: u16, text: &str, label: &str) -> Result<Value, IlinkError> {
        let value: Value = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(text)
                .map_err(|e| IlinkError::Protocol(format!("{label}: bad JSON ({e})")))?
        };
        if !(200..300).contains(&status) {
            let message = value
                .get("errmsg")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error")
                .to_string();
            return Err(IlinkError::Api {
                label: label.to_string(),
                status,
                code: value.get("errcode").and_then(|v| v.as_i64()),
                message,
            });
        }
        if let Some(ret) = value.get("ret").and_then(|v| v.as_i64()) {
            if ret != 0 {
                return Err(IlinkError::Api {
                    label: label.to_string(),
                    status,
                    code: Some(ret),
                    message: value
                        .get("errmsg")
                        .and_then(|v| v.as_str())
                        .unwrap_or("ret != 0")
                        .to_string(),
                });
            }
        }
        if let Some(code) = value.get("errcode").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(IlinkError::Api {
                    label: label.to_string(),
                    status,
                    code: Some(code),
                    message: value
                        .get("errmsg")
                        .and_then(|v| v.as_str())
                        .unwrap_or("errcode != 0")
                        .to_string(),
                });
            }
        }
        Ok(value)
    }

    fn base_info() -> Value {
        serde_json::json!({ "channel_version": CHANNEL_VERSION })
    }

    // ------------------------------------------------------------- lifecycle

    /// `get_bot_qrcode?bot_type=3` → (`qrcode`, png_base64).
    pub async fn login_qrcode(&self) -> Result<(String, String), IlinkError> {
        let value = self
            .get(
                "/ilink/bot/get_bot_qrcode?bot_type=3",
                &[(CLIENT_VERSION_HEADER, "1")],
            )
            .await?;
        let qrcode = value
            .get("qrcode")
            .and_then(|v| v.as_str())
            .ok_or_else(|| IlinkError::Protocol("qrcode missing".into()))?
            .to_string();
        let png = value
            .get("qrcode_img_content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| IlinkError::Protocol("qrcode_img_content missing".into()))?
            .to_string();
        Ok((qrcode, png))
    }

    /// `get_qrcode_status` — poll until confirmed/expired.
    pub async fn qrcode_status(&self, qrcode: &str) -> Result<QrStatus, IlinkError> {
        let path = format!("/ilink/bot/get_qrcode_status?qrcode={}", urlencode(qrcode));
        let value = self.get(&path, &[(CLIENT_VERSION_HEADER, "1")]).await?;
        let status = value
            .get("status")
            .and_then(|v| v.as_str())
            .ok_or_else(|| IlinkError::Protocol("qrcode status missing".into()))?
            .to_string();
        Ok(QrStatus {
            status: status.clone(),
            bot_token: value
                .get("bot_token")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            user_id: value
                .get("ilink_user_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            base_url: value
                .get("baseurl")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        })
    }

    // ------------------------------------------------------------ messaging

    /// GET a CDN URL with bounded size (media download).
    pub async fn fetch_cdn(&self, url: &str) -> Result<Vec<u8>, IlinkError> {
        let response = self
            .http
            .get(url)
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .map_err(|e| IlinkError::Transport(e.to_string()))?;
        if !response.status().is_success() {
            return Err(IlinkError::Api {
                label: "cdn".into(),
                status: response.status().as_u16(),
                code: None,
                message: "download failed".into(),
            });
        }
        let length = response.content_length().unwrap_or(0);
        if length > MAX_DOWNLOAD_BYTES {
            return Err(IlinkError::Protocol(format!(
                "cdn payload {length} exceeds {MAX_DOWNLOAD_BYTES}"
            )));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|e| IlinkError::Transport(e.to_string()))?;
        if bytes.len() as u64 > MAX_DOWNLOAD_BYTES {
            return Err(IlinkError::Protocol("cdn payload exceeds limit".into()));
        }
        Ok(bytes.to_vec())
    }

    /// `getupdates` — one long poll (up to 40 s server-side).
    pub async fn get_updates(&self, cursor: &str) -> Result<UpdatesPage, IlinkError> {
        let body = serde_json::json!({
            "get_updates_buf": cursor,
            "base_info": Self::base_info(),
        });
        let value = self
            .post("/ilink/bot/getupdates", body, Duration::from_secs(45))
            .await?;
        let next_cursor = value
            .get("get_updates_buf")
            .and_then(|v| v.as_str())
            .unwrap_or(cursor)
            .to_string();
        let msgs: Vec<Value> = value
            .get("msgs")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(UpdatesPage {
            cursor: next_cursor,
            msgs,
        })
    }

    /// `sendmessage` with one text item. `ret -2` retries once without the
    /// context token (the reference implementation's stale-token fallback).
    pub async fn send_text(
        &self,
        to_user: &str,
        text: &str,
        context_token: Option<&str>,
    ) -> Result<(), IlinkError> {
        let send = |token: Option<&str>| {
            let body = serde_json::json!({
                "msg": {
                    "from_user_id": "",
                    "to_user_id": to_user,
                    "client_id": uuid::Uuid::new_v4().to_string(),
                    "message_type": 2,
                    "message_state": 2,
                    "context_token": token,
                    "item_list": [{ "type": 1, "text_item": { "text": text } }],
                },
                "base_info": Self::base_info(),
            });
            self.post("/ilink/bot/sendmessage", body, Duration::from_secs(15))
        };
        match send(context_token).await {
            Err(IlinkError::Api { code: Some(-2), .. }) if context_token.is_some() => {
                send(None).await.map(|_| ())
            }
            other => other.map(|_| ()),
        }
    }

    /// `getconfig` → typing ticket for `sendtyping`.
    pub async fn typing_ticket(
        &self,
        user_id: &str,
        context_token: &str,
    ) -> Result<Option<String>, IlinkError> {
        let body = serde_json::json!({
            "ilink_user_id": user_id,
            "context_token": context_token,
            "base_info": Self::base_info(),
        });
        let value = self
            .post("/ilink/bot/getconfig", body, Duration::from_secs(15))
            .await?;
        Ok(value
            .get("config")
            .and_then(|c| c.get("typing_ticket"))
            .and_then(|v| v.as_str())
            .map(str::to_string))
    }

    /// Media/typing milestone; wired in a later phase.
    #[allow(dead_code)]
    pub async fn send_typing(
        &self,
        user_id: &str,
        ticket: &str,
        start: bool,
    ) -> Result<(), IlinkError> {
        let body = serde_json::json!({
            "ilink_user_id": user_id,
            "typing_ticket": ticket,
            "status": if start { 1 } else { 2 },
            "base_info": Self::base_info(),
        });
        self.post("/ilink/bot/sendtyping", body, Duration::from_secs(15))
            .await
            .map(|_| ())
    }

    /// `getuploadurl` for outbound media. Returns (upload_param, file_key)
    /// — the CDN base is fixed (`CDN_BASE`), the response carries either
    /// `upload_param` directly or `upload_full_url` (param embedded in query).
    pub async fn get_upload_url(
        &self,
        to_user: &str,
        file_key: &str,
        media_type: i64,
        raw_size: u64,
        raw_md5: &str,
        encrypted_size: u64,
        aes_key_hex: &str,
    ) -> Result<String, IlinkError> {
        let body = serde_json::json!({
            "filekey": file_key,
            "media_type": media_type,
            "to_user_id": to_user,
            "rawsize": raw_size,
            "rawfilemd5": raw_md5,
            "filesize": encrypted_size,
            "no_need_thumb": true,
            "aeskey": aes_key_hex,
            "base_info": Self::base_info(),
        });
        let value = self
            .post("/ilink/bot/getuploadurl", body, Duration::from_secs(20))
            .await?;
        if let Some(param) = value.get("upload_param").and_then(|v| v.as_str()) {
            return Ok(param.to_string());
        }
        if let Some(full_url) = value.get("upload_full_url").and_then(|v| v.as_str()) {
            // Extract encrypted_query_param from the full URL query.
            if let Some(query) = full_url.split('?').nth(1) {
                for pair in query.split('&') {
                    if let Some(value) = pair.strip_prefix("encrypted_query_param=") {
                        return Ok(percent_decode(value));
                    }
                }
            }
            return Err(IlinkError::Protocol(
                "upload_full_url missing encrypted_query_param".into(),
            ));
        }
        Err(IlinkError::Protocol(
            "getuploadurl missing upload_param/upload_full_url".into(),
        ))
    }

    /// CDN upload: POST ciphertext to the fixed CDN base, read
    /// `x-encrypted-param` back (the download param for sendmessage).
    pub async fn upload_to_cdn(
        &self,
        upload_param: &str,
        file_key: &str,
        ciphertext: &[u8],
    ) -> Result<String, IlinkError> {
        let url = format!(
            "{}/upload?encrypted_query_param={}&filekey={}",
            crate::wechat::media::CDN_BASE,
            urlencode(upload_param),
            urlencode(file_key)
        );
        let response = self
            .http
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(ciphertext.to_vec())
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .map_err(|e| IlinkError::Transport(e.to_string()))?;
        if !response.status().is_success() {
            return Err(IlinkError::Api {
                label: "cdn upload".into(),
                status: response.status().as_u16(),
                code: None,
                message: "upload failed".into(),
            });
        }
        response
            .headers()
            .get("x-encrypted-param")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| IlinkError::Protocol("cdn upload missing x-encrypted-param".into()))
    }

    /// `sendmessage` with arbitrary item list (media).
    pub async fn send_media_message(
        &self,
        to_user: &str,
        context_token: Option<&str>,
        items: Value,
    ) -> Result<(), IlinkError> {
        let body = serde_json::json!({
            "msg": {
                "from_user_id": "",
                "to_user_id": to_user,
                "client_id": uuid::Uuid::new_v4().to_string(),
                "message_type": 2,
                "message_state": 2,
                "context_token": context_token,
                "item_list": items,
            },
            "base_info": Self::base_info(),
        });
        self.post("/ilink/bot/sendmessage", body, Duration::from_secs(15))
            .await
            .map(|_| ())
    }

    /// Lifecycle notification used by the bridge spawn path (called from a
    /// blocking context, not from `async fn`s in this file).
    #[allow(dead_code)]
    pub async fn notify_lifecycle(&self, start: bool) -> Result<(), IlinkError> {
        let path = if start {
            "/ilink/bot/msg/notifystart"
        } else {
            "/ilink/bot/msg/notifystop"
        };
        let body = serde_json::json!({ "base_info": Self::base_info() });
        self.post(path, body, Duration::from_secs(10))
            .await
            .map(|_| ())
    }
}

/// One `getupdates` page.
#[derive(Debug, Clone)]
pub struct UpdatesPage {
    pub cursor: String,
    pub msgs: Vec<Value>,
}

/// `get_qrcode_status` answer.
#[derive(Debug, Clone)]
pub struct QrStatus {
    pub status: String,
    pub bot_token: Option<String>,
    pub user_id: Option<String>,
    pub base_url: Option<String>,
}

/// Normalize a raw `getupdates` msg into the fields the bridge needs.
/// Only `message_type == 1` (user) messages are accepted; the bridge checks
/// the sender against the bound user.
// ---------------------------------------------------------------- wire model
//
// Typed view of an update's `item_list`. The tag is a plain number on the
// wire (1 text, 2 image, 3 voice, 4 file, 5 video, …) and NEW types appear
// without notice, so the item is a struct of optional payloads rather than a
// tagged enum — an unknown tag deserializes with all-None fields and the
// normalization fold simply skips it.

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct WireItem {
    #[serde(rename = "type")]
    pub kind: i64,
    pub text_item: Option<TextBody>,
    pub image_item: Option<ImageBody>,
    pub voice_item: Option<VoiceBody>,
    pub file_item: Option<FileBody>,
    pub ref_msg: Option<RefMsg>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct TextBody {
    pub text: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct ImageBody {
    /// Download URL — `media.full_url` ONLY (other fields belong to other
    /// message styles; mixing yields the file endpoint and a wrong key).
    pub media: Option<CdnMedia>,
    /// Hex key (NOT the file item's base64-wrapped `aes_key`).
    pub aeskey: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct CdnMedia {
    pub full_url: Option<String>,
    pub encrypt_query_param: Option<String>,
    pub aes_key: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct VoiceBody {
    /// Server-side transcript, when transcription succeeded.
    pub text: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct FileBody {
    pub media: Option<CdnMedia>,
    pub file_name: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct RefMsg {
    /// The quoted message is a full item (type + text_item…), not a bare
    /// body; only its text matters here.
    pub message_item: Option<Box<WireItem>>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
struct WireUpdate {
    #[serde(rename = "message_type")]
    kind: i64,
    #[serde(rename = "from_user_id")]
    from: String,
    #[serde(rename = "msg_id")]
    msg_id: Option<String>,
    #[serde(rename = "client_id")]
    client_id: Option<String>,
    #[serde(rename = "context_token")]
    context_token: String,
    #[serde(rename = "item_list")]
    items: Vec<WireItem>,
}

pub fn normalize_msg(raw: &Value) -> Option<Incoming> {
    let wire: WireUpdate = serde_json::from_value(raw.clone()).ok()?;
    if wire.kind != 1 {
        return None;
    }
    let mut state = NormalizeState::default();
    for item in &wire.items {
        state.fold(item);
    }
    Some(Incoming {
        message_id: wire.msg_id.or(wire.client_id).unwrap_or_default(),
        from_user: wire.from,
        context_token: wire.context_token,
        kind: state.kind.to_string(),
        text: state.text.unwrap_or_default(),
        image: state.image,
        file: state.file,
        voice: state.voice,
    })
}

/// First-wins fold over the item list: text (with quoted prefix), image,
/// file, voice; the FIRST media kind seen names the message kind.
struct NormalizeState {
    text: Option<String>,
    kind: &'static str,
    image: Option<ImagePayload>,
    file: Option<FilePayload>,
    voice: Option<String>,
}

impl Default for NormalizeState {
    fn default() -> Self {
        Self {
            text: None,
            kind: "text",
            image: None,
            file: None,
            voice: None,
        }
    }
}

impl NormalizeState {
    fn fold(&mut self, item: &WireItem) {
        match item.kind {
            1 => {
                if self.text.is_none() {
                    let quoted = item
                        .ref_msg
                        .as_ref()
                        .and_then(|r| r.message_item.as_deref())
                        .and_then(|m| m.text_item.as_ref())
                        .and_then(|t| t.text.as_deref())
                        .map(str::trim)
                        .filter(|t| !t.is_empty())
                        .map(str::to_string);
                    let own = item.text_item.as_ref().and_then(|t| t.text.clone());
                    self.text = match (quoted, own) {
                        (Some(quoted), Some(own)) => {
                            Some(format!("[引用: {quoted}]\n{own}"))
                        }
                        (Some(quoted), None) => Some(format!("[引用: {quoted}]")),
                        (None, own) => own,
                    };
                }
            }
            2 => {
                // The tag names the kind even without a usable payload (the
                // old hand-written fold did the same).
                self.kind = "image";
                if self.image.is_none() {
                    if let Some(image) = item.image_item.as_ref() {
                        if let Some(url) = image.media.as_ref().and_then(|m| m.full_url.clone()) {
                            self.image = Some(ImagePayload {
                                url,
                                aes_key: image.aeskey.clone(),
                            });
                        }
                    }
                }
            }
            3 => {
                self.kind = "voice";
                if self.voice.is_none() {
                    self.voice = item
                        .voice_item
                        .as_ref()
                        .and_then(|v| v.text.clone())
                        .filter(|t| !t.trim().is_empty());
                }
            }
            4 => {
                self.kind = "file";
                if self.file.is_none() {
                    if let Some(file) = item.file_item.as_ref() {
                        let media = file.media.clone().unwrap_or_default();
                        let full_url = media
                            .full_url
                            .clone()
                            .unwrap_or_else(|| {
                                // Fallback: build from param (already encoded).
                                media
                                    .encrypt_query_param
                                    .as_deref()
                                    .map(|p| {
                                        format!(
                                            "{}?encrypted_query_param={p}",
                                            crate::wechat::media::CDN_BASE
                                        )
                                    })
                                    .unwrap_or_default()
                            });
                        self.file = Some(FilePayload {
                            encrypt_param: media.encrypt_query_param.unwrap_or_default(),
                            full_url,
                            aes_key: media.aes_key.clone(),
                            file_name: file
                                .file_name
                                .clone()
                                .unwrap_or_else(|| "file".to_string()),
                        });
                    }
                }
            }
            5 => self.kind = "video",
            _ => {} // unknown item type: tolerated, skipped
        }
    }
}

/// Normalized inbound message.
#[derive(Debug, Clone)]
pub struct Incoming {
    pub message_id: String,
    pub from_user: String,
    pub context_token: String,
    pub kind: String,
    pub text: String,
    /// Image: full-size download URL + hex key (from `image_item`).
    pub image: Option<ImagePayload>,
    /// File: CDN param + base64-wrapped key + original name.
    pub file: Option<FilePayload>,
    /// Voice transcript (`voice_item.text`), when the iLink server managed to
    /// transcribe the speech. `None` for voice messages it could not.
    pub voice: Option<String>,
}

/// An image attachment ready for download.
#[derive(Debug, Clone)]
pub struct ImagePayload {
    pub url: String,
    pub aes_key: Option<String>,
}

/// A file attachment ready for download. `full_url` is used as-is (the
/// embed ped_query_param is already URL-encoded — re-encoding breaks the CDN).
#[derive(Debug, Clone)]
pub struct FilePayload {
    pub encrypt_param: String,
    pub full_url: String,
    pub aes_key: Option<String>,
    pub file_name: String,
}

// ---------------------------------------------------------------- encoding

/// Minimal percent-encoding for query values (the QR code token is hex-safe
/// but be defensive).
/// Percent-decode a query value.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(
                std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("zz"),
                16,
            ) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Standard base64 via the `base64` crate (padding accepted; whitespace is
/// NOT — call sites trim before passing).
pub fn base64_decode(text: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(text.trim())
        .ok()
}

pub fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

// ------------------------------------------------------------------- serde
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trip_matches_known_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn normalize_accepts_user_text_and_rejects_bot_messages() {
        let user = serde_json::json!({
            "message_type": 1,
            "from_user_id": "u1",
            "context_token": "ctx",
            "item_list": [{"type": 1, "text_item": {"text": "hello"}}]
        });
        let incoming = normalize_msg(&user).unwrap();
        assert_eq!(incoming.from_user, "u1");
        assert_eq!(incoming.text, "hello");
        assert_eq!(incoming.kind, "text");

        let bot = serde_json::json!({"message_type": 2, "item_list": []});
        assert!(normalize_msg(&bot).is_none());
    }

    #[test]
    fn normalize_reports_media_kinds_without_text() {
        let image = serde_json::json!({
            "message_type": 1,
            "from_user_id": "u1",
            "item_list": [{"type": 2}]
        });
        let incoming = normalize_msg(&image).unwrap();
        assert_eq!(incoming.kind, "image");
        assert_eq!(incoming.text, "");
    }

    #[test]
    fn normalize_prefixes_the_quoted_message() {
        // Reply-with-quote: the original text arrives in ref_msg nested under
        // message_item; the agent must see what is being replied to.
        let quoted = serde_json::json!({
            "message_type": 1,
            "from_user_id": "u1",
            "context_token": "ctx",
            "item_list": [{
                "type": 1,
                "ref_msg": {"message_item": {"type": 1,
                    "text_item": {"text": " 明天几点开会 "}}},
                "text_item": {"text": "帮我加到日历"}
            }]
        });
        let incoming = normalize_msg(&quoted).unwrap();
        assert_eq!(incoming.kind, "text");
        assert_eq!(incoming.text, "[引用: 明天几点开会]\n帮我加到日历");

        // Quote without own text (empty reply): the quote alone survives.
        let quote_only = serde_json::json!({
            "message_type": 1,
            "from_user_id": "u1",
            "item_list": [{
                "type": 1,
                "ref_msg": {"message_item": {"type": 1,
                    "text_item": {"text": "昨天的问题"}}}
            }]
        });
        let incoming = normalize_msg(&quote_only).unwrap();
        assert_eq!(incoming.text, "[引用: 昨天的问题]");

        // Plain text is untouched (no ref_msg wrapper).
        let plain = serde_json::json!({
            "message_type": 1,
            "from_user_id": "u1",
            "item_list": [{"type": 1, "text_item": {"text": "hello"}}]
        });
        let incoming = normalize_msg(&plain).unwrap();
        assert_eq!(incoming.text, "hello");
    }

    #[test]
    fn normalize_extracts_the_voice_transcript() {
        // The iLink server ships the speech transcript in voice_item.text.
        let voiced = serde_json::json!({
            "message_type": 1,
            "from_user_id": "u1",
            "context_token": "ctx",
            "item_list": [
                {"type": 3, "voice_item": {"text": "帮我看看明天的日程"}}
            ]
        });
        let incoming = normalize_msg(&voiced).unwrap();
        assert_eq!(incoming.kind, "voice");
        assert_eq!(incoming.voice.as_deref(), Some("帮我看看明天的日程"));

        // No transcript (or a blank one): kind still marks voice, text stays
        // empty — the bridge tells the user instead of injecting nothing.
        let untranscribed = serde_json::json!({
            "message_type": 1,
            "from_user_id": "u1",
            "item_list": [
                {"type": 3, "voice_item": {"text": "   "}},
                {"type": 3}
            ]
        });
        let incoming = normalize_msg(&untranscribed).unwrap();
        assert_eq!(incoming.kind, "voice");
        assert_eq!(incoming.voice, None);
        assert_eq!(incoming.text, "");
    }

    #[test]
    fn urlencode_leaves_safe_characters() {
        assert_eq!(urlencode("abc123-_~"), "abc123-_~");
        assert_eq!(urlencode("a b"), "a%20b");
    }
}
