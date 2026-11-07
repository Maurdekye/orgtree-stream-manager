//! Helix request shapes (plan §3.1). Pure: builds method, path and JSON body; a separate
//! executor adds the Client-Id and bearer headers and sends them to api.twitch.tv.

use serde_json::{json, Value};

use super::eventsub::SubSpec;

pub const MAX_CHAT_CHARS: usize = 500;
pub const MAX_REASON_CHARS: usize = 500;
pub const MAX_TIMEOUT_S: u32 = 1_209_600;

#[derive(Debug, Clone, PartialEq)]
pub struct HelixRequest {
    pub method: &'static str,
    /// Path and query, relative to https://api.twitch.tv.
    pub path: String,
    pub body: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum HelixError {
    #[error("{0} is not a plain id")]
    BadId(&'static str),
    #[error("bad argument: {0}")]
    BadArg(&'static str),
    #[error("Twitch dropped the message: {0}")]
    Dropped(String),
    #[error("rate limited by Twitch")]
    RateLimited,
    #[error("HTTP status {0}")]
    Status(u16),
    #[error("unexpected response: {0}")]
    BadResponse(String),
}

/// Ids are placed in query strings, so only plain ids pass: letters, digits, '-' and '_'.
fn id<'a>(s: &'a str, what: &'static str) -> Result<&'a str, HelixError> {
    if !s.is_empty() && s.len() <= 64 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        Ok(s)
    } else {
        Err(HelixError::BadId(what))
    }
}

pub fn create_subscription(spec: &SubSpec, session_id: &str) -> HelixRequest {
    HelixRequest {
        method: "POST",
        path: "/helix/eventsub/subscriptions".into(),
        body: Some(json!({
            "type": spec.sub_type,
            "version": spec.version,
            "condition": spec.condition,
            "transport": {"method": "websocket", "session_id": session_id},
        })),
    }
}

pub fn send_chat(broadcaster_id: &str, sender_id: &str, text: &str, reply_to: Option<&str>) -> Result<HelixRequest, HelixError> {
    let b = id(broadcaster_id, "broadcaster_id")?;
    let s = id(sender_id, "sender_id")?;
    let n = text.chars().count();
    if n == 0 || n > MAX_CHAT_CHARS {
        return Err(HelixError::BadArg("message must be 1-500 characters"));
    }
    let mut body = json!({"broadcaster_id": b, "sender_id": s, "message": text});
    if let Some(r) = reply_to {
        body["reply_parent_message_id"] = json!(id(r, "reply_parent_message_id")?);
    }
    Ok(HelixRequest { method: "POST", path: "/helix/chat/messages".into(), body: Some(body) })
}

/// A timeout when `duration_s` is set, a permanent ban otherwise.
pub fn ban(broadcaster_id: &str, moderator_id: &str, user_id: &str, duration_s: Option<u32>, reason: &str) -> Result<HelixRequest, HelixError> {
    let b = id(broadcaster_id, "broadcaster_id")?;
    let m = id(moderator_id, "moderator_id")?;
    let u = id(user_id, "user_id")?;
    if reason.chars().count() > MAX_REASON_CHARS {
        return Err(HelixError::BadArg("reason is over 500 characters"));
    }
    let mut data = json!({"user_id": u, "reason": reason});
    if let Some(d) = duration_s {
        if !(1..=MAX_TIMEOUT_S).contains(&d) {
            return Err(HelixError::BadArg("duration must be 1-1209600 seconds"));
        }
        data["duration"] = json!(d);
    }
    Ok(HelixRequest {
        method: "POST",
        path: format!("/helix/moderation/bans?broadcaster_id={b}&moderator_id={m}"),
        body: Some(json!({ "data": data })),
    })
}

pub fn unban(broadcaster_id: &str, moderator_id: &str, user_id: &str) -> Result<HelixRequest, HelixError> {
    let b = id(broadcaster_id, "broadcaster_id")?;
    let m = id(moderator_id, "moderator_id")?;
    let u = id(user_id, "user_id")?;
    Ok(HelixRequest { method: "DELETE", path: format!("/helix/moderation/bans?broadcaster_id={b}&moderator_id={m}&user_id={u}"), body: None })
}

/// Deletes ONE message. A message id is required: without one, Helix clears the whole chat.
pub fn delete_message(broadcaster_id: &str, moderator_id: &str, message_id: &str) -> Result<HelixRequest, HelixError> {
    let b = id(broadcaster_id, "broadcaster_id")?;
    let m = id(moderator_id, "moderator_id")?;
    let msg = id(message_id, "message_id")?;
    Ok(HelixRequest { method: "DELETE", path: format!("/helix/moderation/chat?broadcaster_id={b}&moderator_id={m}&message_id={msg}"), body: None })
}

/// 0 turns slow mode off; otherwise 3-120 seconds.
pub fn slow_mode(broadcaster_id: &str, moderator_id: &str, seconds: u32) -> Result<HelixRequest, HelixError> {
    let b = id(broadcaster_id, "broadcaster_id")?;
    let m = id(moderator_id, "moderator_id")?;
    let body = match seconds {
        0 => json!({"slow_mode": false}),
        3..=120 => json!({"slow_mode": true, "slow_mode_wait_time": seconds}),
        _ => return Err(HelixError::BadArg("slow mode must be 0 or 3-120 seconds")),
    };
    Ok(HelixRequest { method: "PATCH", path: format!("/helix/chat/settings?broadcaster_id={b}&moderator_id={m}"), body: Some(body) })
}

/// Get Streams for one channel [doc]: used at startup to learn whether it is already live
/// (review round 1, finding 3).
pub fn get_stream(broadcaster_id: &str) -> Result<HelixRequest, HelixError> {
    let b = id(broadcaster_id, "broadcaster_id")?;
    Ok(HelixRequest { method: "GET", path: format!("/helix/streams?user_id={b}"), body: None })
}

/// True when Get Streams lists a live stream.
pub fn parse_stream_live(status: u16, body: &str) -> Result<bool, HelixError> {
    if status == 429 {
        return Err(HelixError::RateLimited);
    }
    if !(200..300).contains(&status) {
        return Err(HelixError::Status(status));
    }
    let v: Value = serde_json::from_str(body).map_err(|e| HelixError::BadResponse(e.to_string()))?;
    let data = v.get("data").and_then(Value::as_array).ok_or_else(|| HelixError::BadResponse("no data".into()))?;
    Ok(data.iter().any(|s| s.get("type").and_then(Value::as_str).unwrap_or("live") == "live"))
}

/// Send Chat Message response: Ok(message id) when sent; Dropped(code) when Twitch
/// accepted the call but dropped the message.
pub fn parse_send_chat(status: u16, body: &str) -> Result<String, HelixError> {
    if status == 429 {
        return Err(HelixError::RateLimited);
    }
    if !(200..300).contains(&status) {
        return Err(HelixError::Status(status));
    }
    let v: Value = serde_json::from_str(body).map_err(|e| HelixError::BadResponse(e.to_string()))?;
    let d = v.get("data").and_then(|d| d.get(0)).ok_or_else(|| HelixError::BadResponse("no data".into()))?;
    if d.get("is_sent").and_then(Value::as_bool) == Some(true) {
        Ok(d.get("message_id").and_then(Value::as_str).unwrap_or("").to_string())
    } else {
        let code = d.pointer("/drop_reason/code").and_then(Value::as_str).unwrap_or("unknown");
        Err(HelixError::Dropped(code.to_string()))
    }
}
