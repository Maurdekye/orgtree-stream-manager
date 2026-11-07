//! A small HTTPS client: hyper + rustls (ring) with the OS root store (coordinator
//! 2026-11-02: native roots). Plain http:// works too, for loopback test fakes.

use std::time::Duration;

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Bytes;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

const TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BODY: usize = 1 << 20;

pub struct HttpReq {
    pub method: &'static str,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

pub struct HttpClient {
    client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    pub fn new() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("the OS root certificate store could not be read")
            .https_or_http()
            .enable_http1()
            .build();
        HttpClient { client: Client::builder(TokioExecutor::new()).build(https) }
    }

    /// Like [`send`](Self::send) for a response that keeps arriving (YouTube `streamList`): the status,
    /// and the body in chunks as text. The 15 s timeout covers only the wait for the headers. The
    /// receiver closes when the server ends the stream or the connection breaks.
    pub async fn stream(&self, r: HttpReq) -> Result<(u16, tokio::sync::mpsc::Receiver<String>), String> {
        let mut b = hyper::Request::builder().method(r.method).uri(&r.url);
        for (k, v) in &r.headers {
            b = b.header(k.as_str(), v.as_str());
        }
        let req = b.body(Full::new(Bytes::from(r.body.unwrap_or_default()))).map_err(|_| "bad request".to_string())?;
        let resp = tokio::time::timeout(TIMEOUT, self.client.request(req))
            .await
            .map_err(|_| "request timed out".to_string())?
            .map_err(|e| format!("request failed: {e}"))?;
        let status = resp.status().as_u16();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let mut body = resp.into_body();
        tokio::spawn(async move {
            let mut carry: Vec<u8> = Vec::new();
            while let Some(Ok(frame)) = body.frame().await {
                let Some(data) = frame.data_ref() else { continue };
                carry.extend_from_slice(data);
                let text = take_utf8(&mut carry);
                if !text.is_empty() && tx.send(text).await.is_err() {
                    return;
                }
            }
        });
        Ok((status, rx))
    }

    /// Returns (status, body). Errors never include the URL or headers (they can carry
    /// tokens).
    pub async fn send(&self, r: HttpReq) -> Result<(u16, String), String> {
        let mut b = hyper::Request::builder().method(r.method).uri(&r.url);
        for (k, v) in &r.headers {
            b = b.header(k.as_str(), v.as_str());
        }
        let req = b.body(Full::new(Bytes::from(r.body.unwrap_or_default()))).map_err(|_| "bad request".to_string())?;
        let resp = tokio::time::timeout(TIMEOUT, self.client.request(req))
            .await
            .map_err(|_| "request timed out".to_string())?
            .map_err(|e| format!("request failed: {e}"))?;
        let status = resp.status().as_u16();
        let body = Limited::new(resp.into_body(), MAX_BODY).collect().await.map_err(|_| "response body too large or broken".to_string())?.to_bytes();
        Ok((status, String::from_utf8_lossy(&body).into_owned()))
    }
}

/// The valid UTF-8 at the front of `carry`, removed from it; an incomplete character at the end
/// stays for the next chunk.
fn take_utf8(carry: &mut Vec<u8>) -> String {
    match std::str::from_utf8(carry) {
        Ok(s) => {
            let out = s.to_string();
            carry.clear();
            out
        }
        Err(e) if e.error_len().is_none() => {
            let n = e.valid_up_to();
            let out = String::from_utf8_lossy(&carry[..n]).into_owned();
            carry.drain(..n);
            out
        }
        Err(_) => {
            let out = String::from_utf8_lossy(carry).into_owned();
            carry.clear();
            out
        }
    }
}
