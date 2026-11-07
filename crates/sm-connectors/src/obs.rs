//! obs-websocket v5 client (plan §3.4): Hello (op 0) → Identify (op 1) → Identified
//! (op 2); requests are op 6, answered by op 7 with the same requestId.
//! Auth: secret = base64(sha256(password + salt)); auth = base64(sha256(secret + challenge)).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// obs-websocket close code for a failed Identify.
const CLOSE_AUTH_FAILED: u16 = 4009;

#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    #[error("could not reach OBS: {0}")]
    Connect(String),
    #[error("OBS asks for a password and none is configured")]
    AuthRequired,
    #[error("OBS refused the password")]
    AuthFailed,
    #[error("unexpected message from OBS: {0}")]
    Protocol(String),
    #[error("OBS refused the request (code {code}): {comment}")]
    Request { code: i64, comment: String },
    #[error("OBS did not answer in time")]
    Timeout,
    #[error("the OBS connection closed")]
    Closed,
}

pub fn obs_auth(password: &str, salt: &str, challenge: &str) -> String {
    let secret = B64.encode(&Sha256::digest(format!("{password}{salt}").as_bytes())[..]);
    B64.encode(&Sha256::digest(format!("{secret}{challenge}").as_bytes())[..])
}

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>;

pub struct ObsClient {
    writer: tokio::sync::Mutex<SplitSink<Ws, Message>>,
    pending: Pending,
    next_id: AtomicU64,
    reader: JoinHandle<()>,
}

impl Drop for ObsClient {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

async fn read_json(stream: &mut SplitStream<Ws>) -> Result<Value, ObsError> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => {
                return serde_json::from_str(t.as_str()).map_err(|e| ObsError::Protocol(e.to_string()));
            }
            Some(Ok(Message::Close(frame))) => {
                return Err(match frame {
                    Some(f) if u16::from(f.code) == CLOSE_AUTH_FAILED => ObsError::AuthFailed,
                    _ => ObsError::Closed,
                });
            }
            Some(Ok(_)) => {}
            Some(Err(_)) | None => return Err(ObsError::Closed),
        }
    }
}

impl ObsClient {
    /// Connects and identifies (with no event subscriptions).
    pub async fn connect(url: &str, password: Option<&str>) -> Result<ObsClient, ObsError> {
        let (ws, _) = connect_async(url).await.map_err(|e| ObsError::Connect(e.to_string()))?;
        let (mut sink, mut stream) = ws.split();
        let hello = tokio::time::timeout(HANDSHAKE_TIMEOUT, read_json(&mut stream)).await.map_err(|_| ObsError::Timeout)??;
        if hello["op"] != 0 {
            return Err(ObsError::Protocol("expected Hello (op 0)".into()));
        }
        let mut identify = json!({"op": 1, "d": {"rpcVersion": 1, "eventSubscriptions": 0}});
        if let Some(a) = hello["d"].get("authentication").filter(|a| !a.is_null()) {
            let pw = password.ok_or(ObsError::AuthRequired)?;
            let (Some(salt), Some(challenge)) = (a["salt"].as_str(), a["challenge"].as_str()) else {
                return Err(ObsError::Protocol("authentication without salt/challenge".into()));
            };
            identify["d"]["authentication"] = json!(obs_auth(pw, salt, challenge));
        }
        sink.send(Message::text(identify.to_string())).await.map_err(|_| ObsError::Closed)?;
        let ident = tokio::time::timeout(HANDSHAKE_TIMEOUT, read_json(&mut stream)).await.map_err(|_| ObsError::Timeout)??;
        if ident["op"] != 2 {
            return Err(ObsError::Protocol("expected Identified (op 2)".into()));
        }
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let p2 = pending.clone();
        let reader = tokio::spawn(async move {
            while let Some(Ok(msg)) = stream.next().await {
                let Message::Text(t) = msg else { continue };
                let Ok(v) = serde_json::from_str::<Value>(t.as_str()) else { continue };
                if v["op"] != 7 {
                    continue;
                }
                if let Some(id) = v["d"]["requestId"].as_str() {
                    if let Some(tx) = p2.lock().unwrap_or_else(|e| e.into_inner()).remove(id) {
                        let _ = tx.send(v["d"].clone());
                    }
                }
            }
            // Closed: dropping the senders tells every waiting request.
            p2.lock().unwrap_or_else(|e| e.into_inner()).clear();
        });
        Ok(ObsClient { writer: tokio::sync::Mutex::new(sink), pending, next_id: AtomicU64::new(1), reader })
    }

    /// Sends one request; returns its `responseData` (an empty object when absent).
    pub async fn request(&self, request_type: &str, data: Value) -> Result<Value, ObsError> {
        let id = format!("sm-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap_or_else(|e| e.into_inner()).insert(id.clone(), tx);
        let msg = json!({"op": 6, "d": {"requestType": request_type, "requestId": id, "requestData": data}});
        if self.writer.lock().await.send(Message::text(msg.to_string())).await.is_err() {
            self.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
            return Err(ObsError::Closed);
        }
        let d = match tokio::time::timeout(REQUEST_TIMEOUT, rx).await {
            Err(_) => {
                self.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                return Err(ObsError::Timeout);
            }
            Ok(Err(_)) => return Err(ObsError::Closed),
            Ok(Ok(d)) => d,
        };
        let st = &d["requestStatus"];
        if st["result"].as_bool() == Some(true) {
            Ok(d.get("responseData").cloned().unwrap_or_else(|| json!({})))
        } else {
            Err(ObsError::Request {
                code: st["code"].as_i64().unwrap_or(0),
                comment: st["comment"].as_str().unwrap_or("").to_string(),
            })
        }
    }

    pub async fn stop_stream(&self) -> Result<(), ObsError> {
        self.request("StopStream", json!({})).await.map(|_| ())
    }

    pub async fn set_program_scene(&self, scene: &str) -> Result<(), ObsError> {
        self.request("SetCurrentProgramScene", json!({"sceneName": scene})).await.map(|_| ())
    }

    /// Sets the transition and its duration (used for the slow AFK fade, §5).
    pub async fn set_transition(&self, name: &str, duration_ms: u64) -> Result<(), ObsError> {
        self.request("SetCurrentSceneTransition", json!({"transitionName": name})).await?;
        self.request("SetCurrentSceneTransitionDuration", json!({"transitionDuration": duration_ms})).await.map(|_| ())
    }

    /// Plays a media source from the start (a sound effect, §18).
    pub async fn restart_media(&self, input: &str) -> Result<(), ObsError> {
        self.request("TriggerMediaInputAction", json!({"inputName": input, "mediaAction": "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_RESTART"}))
            .await
            .map(|_| ())
    }
}
