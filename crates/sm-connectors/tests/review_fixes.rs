//! Review round 1, connector findings 3 and 4.

use serde_json::json;
use sm_connectors::twitch::helix;
use sm_connectors::youtube::find_chat;

// Finding 3: a startup check of whether the channel is already live (Get Streams [doc]).
#[test]
fn get_streams_request_and_liveness() {
    let r = helix::get_stream("100").unwrap();
    assert_eq!((r.method, r.path.as_str(), r.body.is_none()), ("GET", "/helix/streams?user_id=100", true));
    assert!(helix::get_stream("1&x").is_err());
    assert_eq!(helix::parse_stream_live(200, r#"{"data":[{"id":"1","user_id":"100","type":"live"}]}"#), Ok(true));
    assert_eq!(helix::parse_stream_live(200, r#"{"data":[]}"#), Ok(false));
    assert!(helix::parse_stream_live(401, "{}").is_err());
}

// Finding 4: the broadcast lookup also yields the channel's own id.
#[test]
fn find_chat_returns_the_channel_id() {
    let body = json!({"items":[{"id":"B1","snippet":{"liveChatId":"L1","channelId":"UCself"}}]}).to_string();
    assert_eq!(find_chat(&body), Some(("B1".into(), "L1".into(), Some("UCself".into()))));
    let body = json!({"items":[{"id":"B2","snippet":{"liveChatId":"L2"}}]}).to_string();
    assert_eq!(find_chat(&body), Some(("B2".into(), "L2".into(), None)));
}
