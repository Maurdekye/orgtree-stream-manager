//! Plan §9.2 + §16 (outbound filter, repeat guard, rate), §3.3/§6.3 (donations), §4.3 (commands).

use sm_core::donation::{streamlabs_counts, Source, ValueTable};
use sm_core::outbound::{OutboundFilter, RepeatGuard};
use sm_core::pipeline::RateWindow;
use sm_core::protocol::{parse_command, CmdCache, Command, GateValueArg};

fn filter() -> OutboundFilter {
    OutboundFilter::new(
        vec!["s3cr3t-T0ken-value-123456".to_string()],
        vec!["Springfield".to_string(), "acme-pc".to_string()],
    )
}

#[test]
fn outbound_filter_blocks_secrets_and_identifying_shapes() {
    let f = filter();
    assert_eq!(f.check("thanks for the follow!"), Ok(()));
    let cases = [
        ("here: s3cr3t-T0ken-value-123456", "secret"),
        ("partial T0ken-va leak", "secret"), // 8+ char fragment
        ("key sk-abcdefghijklmnop1234", "key_shape"),
        ("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghij", "jwt"),
        ("look in C:\\Users\\alex\\Desktop", "path"),
        ("my home is /home/alex", "path"),
        ("connect to 192.168.1.20", "ip"),
        ("mail me at someone@example.com", "email"),
        ("I live in springfield", "deny_list"),
        ("the box is ACME-PC", "deny_list"),
        ("dGhpcyBpcyBhIHZlcnkgbG9uZyBiYXNlNjQgc3RyaW5n", "long_token"),
    ];
    for (text, rule) in cases {
        assert_eq!(f.check(text), Err(rule), "{text}");
    }
}

#[test]
fn repeat_guard_blocks_near_identical_replies_within_ten_minutes() {
    let mut g = RepeatGuard::new(600.0, 0.8);
    assert_eq!(g.check(0.0, "Welcome to the stream!"), Ok(()));
    g.record(0.0, "Welcome to the stream!");
    assert_eq!(g.check(10.0, "welcome to the stream!!"), Err("outbound_repeat"));
    assert_eq!(g.check(10.0, "Thanks for the bits, much appreciated"), Ok(()));
    assert_eq!(g.check(601.0, "Welcome to the stream!"), Ok(()));
}

#[test]
fn outbound_rate_one_per_ten_seconds_four_per_minute() {
    let mut r = RateWindow::new(10.0, 4, 60.0);
    assert!(r.try_acquire(0.0).is_ok());
    assert_eq!(r.try_acquire(5.0), Err(5.0));
    assert!(r.try_acquire(10.0).is_ok());
    assert!(r.try_acquire(20.0).is_ok());
    assert!(r.try_acquire(30.0).is_ok());
    assert_eq!(r.try_acquire(40.0), Err(20.0)); // 5th in the window waits until t=60
    assert!(r.try_acquire(60.0).is_ok());
}

#[test]
fn donation_values_follow_the_table() {
    let mut t = ValueTable::default();
    t.set_rate("EUR", 1.10);
    assert_eq!(t.usd(&Source::TwitchBits { bits: 500 }), Some(5.0));
    assert_eq!(t.usd(&Source::TwitchSub { tier: 1, count: 1 }), Some(5.0));
    assert_eq!(t.usd(&Source::TwitchSub { tier: 3, count: 2 }), Some(50.0));
    assert_eq!(t.usd(&Source::TwitchSub { tier: 4, count: 1 }), None);
    assert_eq!(t.usd(&Source::YouTubeSuperChat { amount_micros: 5_000_000, currency: "USD".into() }), Some(5.0));
    let eur = t.usd(&Source::YouTubeSuperChat { amount_micros: 10_000_000, currency: "eur".into() }).unwrap();
    assert!((eur - 11.0).abs() < 1e-9);
    assert_eq!(t.usd(&Source::YouTubeSuperChat { amount_micros: 1, currency: "XYZ".into() }), None);
    assert_eq!(t.usd(&Source::Streamlabs { amount: "13.37".into(), currency: "USD".into() }), Some(13.37));
    assert_eq!(t.usd(&Source::Streamlabs { amount: "abc".into(), currency: "USD".into() }), None);
    assert_eq!(t.usd(&Source::Streamlabs { amount: "-5".into(), currency: "USD".into() }), None);
}

#[test]
fn streamlabs_relays_are_not_double_counted_and_follows_never_count() {
    assert!(streamlabs_counts("donation", None, true, true));
    assert!(!streamlabs_counts("bits", Some("twitch_account"), true, true));
    assert!(streamlabs_counts("bits", Some("twitch_account"), false, true));
    assert!(!streamlabs_counts("superchat", Some("youtube_account"), true, true));
    assert!(streamlabs_counts("superchat", Some("youtube_account"), true, false));
    assert!(!streamlabs_counts("follow", Some("twitch_account"), false, false));
    assert!(!streamlabs_counts("follow", Some("youtube_account"), false, false));
    assert!(!streamlabs_counts("host", Some("twitch_account"), false, false));
}

#[test]
fn commands_parse_and_validate() {
    let (id, c) = parse_command(r#"{"v":1,"type":"cmd","cmd_id":"c1","cmd":"mod.timeout","args":{"viewer":"twitch:123","duration_s":600,"reason":"spam"}}"#).unwrap();
    assert_eq!(id, "c1");
    assert!(matches!(c, Command::ModTimeout { duration_s: 600, .. }));

    let (_, c) = parse_command(r#"{"v":1,"type":"cmd","cmd_id":"c2","cmd":"gate.tighten","args":{"value":"min_donation","reason":"busy"}}"#).unwrap();
    assert!(matches!(c, Command::GateTighten { value: GateValueArg::MinDonation, hold_min: None, .. }));

    let (_, c) = parse_command(r#"{"v":1,"type":"cmd","cmd_id":"c3","cmd":"ping"}"#).unwrap();
    assert_eq!(c, Command::Ping);

    let bad = [
        (r#"not json"#, "bad_json"),
        (r#"{"v":2,"type":"cmd","cmd_id":"x","cmd":"ping"}"#, "unsupported_version"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"rm.rf"}"#, "unknown_cmd"),
        (r#"{"v":1,"type":"cmd","cmd_id":"","cmd":"ping"}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"stream.end","args":{"confirm":"yes","reason":"done"}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"mod.timeout","args":{"viewer":"twitch:1","duration_s":0,"reason":"r"}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"mod.timeout","args":{"viewer":"twitch:1","duration_s":1209601,"reason":"r"}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"mod.ban","args":{"viewer":"bob","reason":"r"}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"mod.ban","args":{"viewer":"twitch:1","reason":""}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"mod.slow_mode","args":{"seconds":2}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"chat.say","args":{"text":""}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"gate.set","args":{"min_interval_s":-5,"reason":"r"}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"gate.set","args":{"reason":"r"}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"gate.tighten","args":{"value":"min_interval","reason":"r","hold_min":241}}"#, "bad_args"),
        (r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"mod.delete","args":{}}"#, "bad_args"),
    ];
    for (raw, code) in bad {
        let e = parse_command(raw).unwrap_err();
        assert_eq!(e.code, code, "{raw}: {e:?}");
    }
    // The cmd_id is still reported when the rest is bad, so the result can be correlated.
    assert_eq!(parse_command(r#"{"v":1,"type":"cmd","cmd_id":"k9","cmd":"nope"}"#).unwrap_err().cmd_id.as_deref(), Some("k9"));

    let long = "x".repeat(401);
    let raw = format!(r#"{{"v":1,"type":"cmd","cmd_id":"x","cmd":"chat.say","args":{{"text":"{long}"}}}}"#);
    assert_eq!(parse_command(&raw).unwrap_err().code, "bad_args");
}

#[test]
fn command_cache_returns_the_first_result_and_evicts_oldest() {
    let mut c = CmdCache::new(2);
    c.put("a".into(), serde_json::json!({"ok":true}));
    c.put("a".into(), serde_json::json!({"ok":false}));
    assert_eq!(c.get("a"), Some(&serde_json::json!({"ok":true})));
    c.put("b".into(), serde_json::json!(1));
    c.put("c".into(), serde_json::json!(2));
    assert_eq!(c.get("a"), None);
    assert!(c.get("b").is_some() && c.get("c").is_some());
}
