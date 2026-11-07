//! Plan §11: the stats.json snapshot the Orgtree app panel reads (bounded lists, size cap,
//! atomic write).

use serde_json::{json, Value};
use sm_server::stats::{LimitRow, Stats, MAX_BYTES};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("sm-stats-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn lists_are_bounded_newest_kept() {
    let mut s = Stats::new();
    for i in 0..200 {
        s.chat(i as f64, "twitch", &format!("v{i}"), &format!("msg {i}"), "forwarded", vec![]);
    }
    for i in 0..60 {
        s.donation(i as f64, Some("youtube"), "youtube_superchat", "fan", Some(1.0), Some("hi"));
    }
    for i in 0..60 {
        s.moderation(i as f64, "timeout", &format!("twitch:{i}"), "agent:coordinator-opus", "spam");
    }
    let snap = s.snapshot(1000.0);
    assert_eq!(snap["v"], 1);
    assert_eq!(snap["at"], "1970-01-01T00:16:40.000Z");
    let chat = snap["chat"].as_array().unwrap();
    assert_eq!(chat.len(), 150);
    assert_eq!(chat.last().unwrap()["text"], "msg 199");
    assert_eq!(chat[0]["text"], "msg 50");
    assert_eq!(snap["donations"].as_array().unwrap().len(), 50);
    assert_eq!(snap["moderation"].as_array().unwrap().len(), 50);
    assert_eq!(snap["totals"]["session_usd"], 60.0);
    assert_eq!(snap["totals"]["by_source"]["youtube_superchat"], 60.0);
}

#[test]
fn unvalued_donations_are_listed_but_not_totalled() {
    let mut s = Stats::new();
    s.donation(1.0, None, "streamlabs", "someone", None, None);
    s.donation(2.0, Some("twitch"), "twitch_bits", "b", Some(5.0), None);
    let snap = s.snapshot(3.0);
    assert_eq!(snap["donations"].as_array().unwrap().len(), 2);
    assert_eq!(snap["donations"][0]["usd"], Value::Null);
    assert_eq!(snap["donations"][0]["platform"], Value::Null);
    assert_eq!(snap["totals"]["session_usd"], 5.0);
}

#[test]
fn state_gate_limits_health_pass_through() {
    let mut s = Stats::new();
    s.set_state(json!({"combined":"live","twitch":"live"}));
    s.set_gate(json!({"min_interval_s":120,"min_donation_usd":0}));
    s.gate_change(5.0, "min_interval", 120.0, 180.0, "agent:coordinator-opus", "busy");
    s.set_health(json!({"twitch":"up"}));
    s.set_limits(vec![LimitRow {
        viewer: "twitch:1".into(),
        name: "v".into(),
        platform: "twitch".into(),
        credit_usd: 2.5,
        cooldown_s: 34.0,
        max_chars: 187,
        next_allowed_at: Some(40.0),
        held_until: None,
    }]);
    let snap = s.snapshot(10.0);
    assert_eq!(snap["state"]["combined"], "live");
    assert_eq!(snap["gate"]["min_interval_s"], 120);
    assert_eq!(snap["gate_history"][0], json!({"ts":"1970-01-01T00:00:05.000Z","value":"min_interval","from":120.0,"to":180.0,"by":"agent:coordinator-opus","reason":"busy"}));
    assert_eq!(snap["health"]["twitch"], "up");
    let l = &snap["limits"][0];
    assert_eq!(l["next_allowed_at"], "1970-01-01T00:00:40.000Z");
    assert_eq!(l["held_until"], Value::Null);
    assert_eq!(l["max_chars"], 187);
}

#[test]
fn snapshot_stays_under_the_size_cap_by_dropping_oldest() {
    let mut s = Stats::new();
    let long = "x".repeat(500);
    for i in 0..150 {
        s.chat(i as f64, "twitch", "v", &long, "forwarded", vec!["link".into()]);
    }
    for i in 0..50 {
        s.donation(i as f64, Some("twitch"), "twitch_bits", "d", Some(1.0), Some(&long));
    }
    let snap = s.snapshot(1.0);
    let bytes = serde_json::to_vec(&snap).unwrap().len();
    assert!(bytes <= MAX_BYTES, "{bytes} > {MAX_BYTES}");
    let chat = snap["chat"].as_array().unwrap();
    assert!(chat.len() < 150 && !chat.is_empty());
    assert_eq!(chat.last().unwrap()["ts"], "1970-01-01T00:02:29.000Z", "newest kept");
    assert_eq!(snap["truncated"], true);
}

#[test]
fn write_is_atomic_and_leaves_no_temp_file() {
    let dir = tmp_dir("atomic");
    let path = dir.join("stats.json");
    let mut s = Stats::new();
    s.chat(1.0, "twitch", "v", "first", "forwarded", vec![]);
    let n = s.write_atomic(2.0, &path).unwrap();
    assert!(n > 0);
    s.chat(3.0, "twitch", "v", "second", "held:cooldown", vec![]);
    s.write_atomic(4.0, &path).unwrap();
    let v: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(v["chat"].as_array().unwrap().len(), 2);
    assert_eq!(v["chat"][1]["status"], "held:cooldown");
    let names: Vec<String> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
    assert_eq!(names, vec!["stats.json".to_string()], "no temp files left: {names:?}");
    std::fs::remove_dir_all(&dir).unwrap();
}
