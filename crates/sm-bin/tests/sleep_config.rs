//! Auto-sleep config and the read-only OBS startup check (plan rev 2).

use sm_bin::config::load_config;
use sm_bin::sleep_check::{sleep_check, ObsSleepFacts};

#[test]
fn sleep_is_off_unless_the_config_turns_it_on() {
    let c = load_config("").unwrap();
    let b = c.bridge_config(vec![]);
    assert!(!b.sleep.enabled);
    assert_eq!(b.sleep_scene, "Sleeping");
    assert_eq!(b.sleep.idle_s, 600.0);
    assert_eq!(b.sleep.min_sleep_s, 15.0);
}

#[test]
fn the_sleep_section_maps_to_the_bridge() {
    let c = load_config("[sleep]\nenabled = true\nscene = \"Zzz\"\nidle_min = 12\nmin_sleep_s = 6\n").unwrap();
    let b = c.bridge_config(vec![]);
    assert!(b.sleep.enabled);
    assert_eq!(b.sleep_scene, "Zzz");
    assert_eq!(b.sleep.idle_s, 720.0);
    assert_eq!(b.sleep.min_sleep_s, 6.0);
}

#[test]
fn bad_sleep_values_are_refused() {
    assert!(load_config("[sleep]\nidle_min = 0\n").unwrap_err().contains("sleep.idle_min"));
    assert!(load_config("[sleep]\nmin_sleep_s = -1\n").unwrap_err().contains("sleep.min_sleep_s"));
    assert!(load_config("[sleep]\nscene = \"\"\n").unwrap_err().contains("sleep.scene"));
    assert!(load_config("[sleep]\nbogus = 1\n").is_err());
}

fn facts(scenes: &[&str], ov: Option<(&str, u64)>, studio: bool) -> ObsSleepFacts {
    ObsSleepFacts {
        scenes: scenes.iter().map(|s| s.to_string()).collect(),
        override_name: ov.map(|o| o.0.to_string()),
        override_ms: ov.map(|o| o.1),
        studio_mode: studio,
    }
}

#[test]
fn the_startup_check_passes_with_scene_override_and_no_studio_mode() {
    let r = sleep_check("Sleeping", 15.0, &facts(&["App", "AFK", "Sleeping"], Some(("Fade", 5000)), false));
    assert!(r.usable);
    assert!(r.warnings.is_empty(), "{:?}", r.warnings);
}

#[test]
fn a_missing_scene_makes_sleep_unusable() {
    let r = sleep_check("Sleeping", 15.0, &facts(&["App", "AFK"], None, false));
    assert!(!r.usable);
    assert_eq!(r.warnings[0].0, "sleep_scene_missing");
}

#[test]
fn no_override_studio_mode_and_a_long_fade_only_warn() {
    let r = sleep_check("Sleeping", 15.0, &facts(&["Sleeping"], None, true));
    assert!(r.usable);
    let codes: Vec<&str> = r.warnings.iter().map(|w| w.0).collect();
    assert_eq!(codes, vec!["sleep_no_override", "sleep_studio_mode"]);
    let r = sleep_check("Sleeping", 15.0, &facts(&["Sleeping"], Some(("Fade", 20_000)), false));
    assert_eq!(r.warnings.iter().map(|w| w.0).collect::<Vec<_>>(), vec!["sleep_fade_longer_than_min_sleep"]);
}
