//! config.toml and secrets.toml (plan §2, §10, §17: TOML).

use sm_bin::config::{load_config, Config};
use sm_bin::secrets::load_secrets;

const EXAMPLE: &str = include_str!("../config.example.toml");

#[test]
fn the_example_config_parses_and_maps_to_the_bridge() {
    let c: Config = load_config(EXAMPLE).unwrap();
    assert_eq!(c.server.events_addr, "0.0.0.0:8787");
    assert!(c.twitch.enabled);
    assert_eq!(c.twitch.broadcaster_id, "100000001");
    assert_eq!(c.twitch.eventsub_url, "wss://eventsub.wss.twitch.tv/ws");
    assert_eq!(c.twitch.api_base, "https://api.twitch.tv");
    assert!(!c.youtube.enabled);
    let b = c.bridge_config(vec!["s3cret-value-1".into()]);
    assert_eq!(b.gate.start_interval_s, 120.0);
    assert_eq!(b.gate.start_donation_usd, 0.0);
    assert!(b.gate.auto_relax);
    assert_eq!(b.afk.quiet_s, 1800.0);
    assert_eq!(b.afk.fade_s, 120.0);
    assert!(!b.afk.drops_campaign);
    assert_eq!(b.effects.get("airhorn"), Some(&30.0));
    assert_eq!(b.donation_effects.len(), 2);
    assert_eq!(b.donation_effects[1].effect, "airhorn");
    assert_eq!(b.values.rates.get("EUR"), Some(&1.08));
    assert_eq!(b.values.rates.get("USD"), Some(&1.0));
    assert_eq!(b.deny, vec!["Springfield".to_string(), "acme-pc".to_string()]);
    assert_eq!(b.secrets, vec!["s3cret-value-1".to_string()]);
    assert_eq!(b.afk_scene, "AFK");
    assert_eq!(b.bot_viewer_ids, vec!["twitch:100000002".to_string()]);
    assert_eq!(c.effect_media_inputs().get("airhorn").map(String::as_str), Some("SFX Airhorn"));
}

#[test]
fn defaults_fill_a_minimal_config() {
    let c = load_config("[twitch]\nenabled = false\n").unwrap();
    assert_eq!(c.server.events_addr, "127.0.0.1:8787", "review 13b: loopback by default");
    assert_eq!(c.server.stats_path, "/out/stats.json");
    assert_eq!(c.bridge.gate_start_interval_s, 120.0);
    assert!(c.bridge.chat_say_enabled);
}

#[test]
fn unknown_keys_and_bad_values_are_refused() {
    assert!(load_config("[server]\nevents_adr = \"x\"\n").unwrap_err().contains("events_adr"));
    assert!(load_config("[bridge]\ngate_start_interval_s = -5\n").is_err());
    assert!(load_config("[[donation_effect]]\nmin_usd = 1.0\neffect = \"nope\"\n").unwrap_err().contains("nope"));
    assert!(load_config("[twitch]\nenabled = true\n").unwrap_err().contains("broadcaster_id"));
}

const SECRETS: &str = r#"
[twitch]
client_secret = "tw-client-secret-xyz"
refresh_token = "tw-refresh-abc123"

[obs]
password = "obs-password-123"

[[bridge_token]]
name = "listener-read"
token = "read-token-0123456789abcdef0123456789"

[[bridge_token]]
name = "listener-control"
token = "control-token-0123456789abcdef01234567"
control = true
"#;

#[test]
fn secrets_parse_redact_and_feed_the_filter() {
    let s = load_secrets(SECRETS).unwrap();
    assert_eq!(s.twitch.as_ref().unwrap().refresh_token, "tw-refresh-abc123");
    let dbg = format!("{s:?}");
    assert!(!dbg.contains("tw-client-secret-xyz") && !dbg.contains("obs-password-123") && !dbg.contains("read-token"), "{dbg}");
    let all = s.all_values();
    for v in ["tw-client-secret-xyz", "tw-refresh-abc123", "obs-password-123", "read-token-0123456789abcdef0123456789"] {
        assert!(all.contains(&v.to_string()), "{v}");
    }
    let table = s.token_table();
    assert_eq!(table.check("control-token-0123456789abcdef01234567").map(|x| x.1.control), Some(true));
    assert_eq!(table.check("read-token-0123456789abcdef0123456789").map(|x| x.1.control), Some(false));
    assert!(load_secrets("[[bridge_token]]\nname = \"x\"\ntoken = \"short\"\n").unwrap_err().contains("at least 32"));
}

// Phase 7 trial guard (coordinator m0c328f887e68): missing key refuses stream.end; only an explicit true enables it.
#[test]
fn stream_end_is_disabled_unless_the_config_enables_it() {
    let c = load_config(EXAMPLE).unwrap();
    assert!(!c.bridge.stream_end_enabled);
    assert!(!c.bridge_config(vec![]).stream_end_enabled);
    let on = EXAMPLE.replace("[bridge]\n", "[bridge]\nstream_end_enabled = true\n");
    let c = load_config(&on).unwrap();
    assert!(c.bridge_config(vec![]).stream_end_enabled);
}
