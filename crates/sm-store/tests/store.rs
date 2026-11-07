//! Plan §2 (data on disk, 7-day chat retention) and §4.1 (replay cursors).

use serde_json::json;
use sm_store::Store;

fn temp_db(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("sm-store-test-{}-{}.db", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

#[test]
fn events_get_increasing_seq_and_read_back_in_order() {
    let s = Store::open_in_memory().unwrap();
    assert_eq!(s.head_seq().unwrap(), 0);
    let a = s.append_event(10.0, "chat", &json!({"text":"a"})).unwrap();
    let b = s.append_event(11.0, "donation", &json!({"amount":1})).unwrap();
    let c = s.append_event(12.0, "chat", &json!({"text":"c"})).unwrap();
    assert!(a < b && b < c);
    assert_eq!(s.head_seq().unwrap(), c);
    let all = s.events_after(0, 100).unwrap();
    assert_eq!(all.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![a, b, c]);
    assert_eq!(all[1].kind, "donation");
    assert_eq!(all[1].ts, 11.0);
    assert_eq!(all[1].body, json!({"amount":1}));
    let tail = s.events_after(a, 1).unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].seq, b);
}

#[test]
fn cursors_only_move_forward() {
    let s = Store::open_in_memory().unwrap();
    assert_eq!(s.cursor("listener-chat").unwrap(), None);
    s.set_cursor("listener-chat", 5).unwrap();
    s.set_cursor("listener-chat", 3).unwrap();
    assert_eq!(s.cursor("listener-chat").unwrap(), Some(5));
    s.set_cursor("listener-chat", 9).unwrap();
    assert_eq!(s.cursor("listener-chat").unwrap(), Some(9));
    assert_eq!(s.cursor("other").unwrap(), None);
}

#[test]
fn data_survives_reopen() {
    let p = temp_db("reopen");
    {
        let s = Store::open(&p).unwrap();
        s.append_event(1.0, "chat", &json!({"text":"x"})).unwrap();
        s.set_cursor("sub", 1).unwrap();
        s.upsert_viewer("twitch:1", 100.0, true).unwrap();
        s.add_credit("twitch:1", 50.0, 5.0).unwrap();
    }
    let s = Store::open(&p).unwrap();
    assert_eq!(s.head_seq().unwrap(), 1);
    assert_eq!(s.cursor("sub").unwrap(), Some(1));
    let v = s.viewer("twitch:1").unwrap().unwrap();
    assert_eq!((v.first_seen, v.paid_sub), (100.0, true));
    assert_eq!(s.credit_grants("twitch:1").unwrap(), vec![(50.0, 5.0)]);
    drop(s);
    let _ = std::fs::remove_file(&p);
}

#[test]
fn viewer_upsert_keeps_first_seen_and_updates_sub_status() {
    let s = Store::open_in_memory().unwrap();
    assert!(s.viewer("youtube:abc").unwrap().is_none());
    s.upsert_viewer("youtube:abc", 10.0, false).unwrap();
    s.upsert_viewer("youtube:abc", 99.0, true).unwrap();
    let v = s.viewer("youtube:abc").unwrap().unwrap();
    assert_eq!(v.first_seen, 10.0);
    assert!(v.paid_sub);
}

#[test]
fn prune_deletes_old_chat_and_redacts_old_donation_text_only() {
    let s = Store::open_in_memory().unwrap();
    let day = 86_400.0;
    s.append_event(0.0, "chat", &json!({"text":"old chat"})).unwrap();
    let old_don = s.append_event(0.0, "donation", &json!({"text":"old note","amount":{"usd":5.0}})).unwrap();
    let old_mod = s.append_event(0.0, "moderation", &json!({"action":"timeout"})).unwrap();
    s.append_event(8.0 * day, "chat", &json!({"text":"new chat"})).unwrap();
    let r = s.prune(8.0 * day + 1.0, 7.0 * day).unwrap();
    assert_eq!(r.chat_deleted, 1);
    assert_eq!(r.donation_texts_redacted, 1);
    let all = s.events_after(0, 100).unwrap();
    assert_eq!(all.len(), 3);
    let don = all.iter().find(|e| e.seq == old_don).unwrap();
    assert_eq!(don.body["text"], serde_json::Value::Null);
    assert_eq!(don.body["amount"]["usd"], 5.0);
    assert!(all.iter().any(|e| e.seq == old_mod));
    // Head seq never goes backwards after a prune.
    let next = s.append_event(9.0 * day, "chat", &json!({})).unwrap();
    assert!(next > old_mod);
}

#[test]
fn audit_is_append_only() {
    let s = Store::open_in_memory().unwrap();
    s.audit(1.0, "agent:coordinator-opus", "gate.tighten", &json!({"to":[180,0]})).unwrap();
    s.audit(2.0, "operator", "chat_say_enabled", &json!({"on":false})).unwrap();
    let rows = s.audit_rows(10).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].actor, "agent:coordinator-opus");
    assert_eq!(rows[1].action, "chat_say_enabled");
    assert!(s.try_tamper_audit_for_tests().is_err(), "UPDATE/DELETE on audit must be refused");
}
