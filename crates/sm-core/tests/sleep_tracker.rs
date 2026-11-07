//! Auto-sleep (USER 2026-11-05 15:04:39Z m66df61bea159): "any agents that are active should keep the
//! stream alive, the stream should only die if there are no agents running a turn for an extended
//! period of time". "Die" is the Sleeping scene, never stream.end. Plan rev 2 (auto-sleep-plan.md).

use sm_core::protocol::{parse_command, Command};
use sm_core::sleep::{SleepAction, SleepParams, SleepTracker, TurnsReport};

fn on() -> SleepParams {
    SleepParams { enabled: true, idle_s: 600.0, min_sleep_s: 15.0 }
}

#[test]
fn ships_off_by_default() {
    let p = SleepParams::default();
    assert!(!p.enabled);
    assert_eq!(p.idle_s, 600.0);
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    assert_eq!(t.tick(&p, 10_000.0, false), vec![]);
    assert_eq!(t.state_label(&p), "off");
}

#[test]
fn unknown_state_never_sleeps() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    assert_eq!(t.tick(&p, 100_000.0, false), vec![]);
    assert_eq!(t.state_label(&p), "awake");
}

#[test]
fn sleeps_only_after_idle_min_straight_with_no_turn_running() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    assert_eq!(t.report(&p, 100.0, TurnsReport::Idle), vec![]);
    assert_eq!(t.tick(&p, 699.0, false), vec![]);
    assert_eq!(t.tick(&p, 700.0, false), vec![SleepAction::Sleep]);
    assert!(t.is_asleep());
    assert_eq!(t.state_label(&p), "asleep");
    assert_eq!(t.tick(&p, 2000.0, false), vec![], "sleep is emitted once");
}

#[test]
fn a_turn_running_keeps_it_awake_however_long() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    t.report(&p, 300.0, TurnsReport::Active);
    assert_eq!(t.tick(&p, 50_000.0, false), vec![], "a long turn keeps the stream awake for its whole length");
    t.report(&p, 50_000.0, TurnsReport::Idle);
    assert_eq!(t.tick(&p, 50_599.0, false), vec![]);
    assert_eq!(t.tick(&p, 50_600.0, false), vec![SleepAction::Sleep], "the idle clock starts at the idle edge");
}

#[test]
fn any_turn_start_wakes_it() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    t.tick(&p, 600.0, false);
    assert_eq!(t.report(&p, 700.0, TurnsReport::Active), vec![SleepAction::Wake]);
    assert!(!t.is_asleep());
}

#[test]
fn a_wake_during_the_slow_fade_waits_for_min_sleep() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    assert_eq!(t.tick(&p, 600.0, false), vec![SleepAction::Sleep]);
    assert_eq!(t.report(&p, 605.0, TurnsReport::Active), vec![], "deferred: the 5 s fade may still be running");
    assert_eq!(t.tick(&p, 614.0, false), vec![]);
    assert_eq!(t.tick(&p, 615.0, false), vec![SleepAction::Wake]);
}

#[test]
fn a_wake_that_is_cancelled_before_min_sleep_stays_asleep() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    t.tick(&p, 600.0, false);
    t.report(&p, 602.0, TurnsReport::Active);
    t.report(&p, 604.0, TurnsReport::Idle);
    assert_eq!(t.tick(&p, 615.0, false), vec![SleepAction::Wake], "a turn did run: wake, then the idle clock restarts");
    assert_eq!(t.tick(&p, 1203.0, false), vec![]);
    assert_eq!(t.tick(&p, 1215.0, false), vec![SleepAction::Sleep]);
}

#[test]
fn resync_counts_as_a_turn_running() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    t.tick(&p, 600.0, false);
    assert_eq!(t.report(&p, 700.0, TurnsReport::Resync), vec![SleepAction::Wake]);
    assert_eq!(t.tick(&p, 100_000.0, false), vec![], "stays awake until the next idle edge");
}

#[test]
fn the_relays_own_turns_count_separately() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    t.report(&p, 10.0, TurnsReport::SelfActive);
    assert_eq!(t.tick(&p, 5000.0, false), vec![], "the relay agent is in a turn");
    t.report(&p, 5000.0, TurnsReport::SelfIdle);
    assert_eq!(t.tick(&p, 5600.0, false), vec![SleepAction::Sleep]);
    assert_eq!(t.report(&p, 6000.0, TurnsReport::SelfActive), vec![SleepAction::Wake]);
    t.report(&p, 6001.0, TurnsReport::Active);
    t.report(&p, 6002.0, TurnsReport::SelfIdle);
    assert_eq!(t.tick(&p, 9000.0, false), vec![], "the org still has a turn running");
}

#[test]
fn agent_commands_restart_the_idle_clock_and_wake() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    assert_eq!(t.activity(&p, 500.0), vec![]);
    assert_eq!(t.tick(&p, 1099.0, false), vec![]);
    assert_eq!(t.tick(&p, 1100.0, false), vec![SleepAction::Sleep]);
    assert_eq!(t.activity(&p, 1200.0), vec![SleepAction::Wake]);
}

#[test]
fn manual_afk_wins() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    t.afk_changed(100.0, true);
    assert_eq!(t.tick(&p, 5000.0, true), vec![], "no sleep while AFK is on");
    assert_eq!(t.report(&p, 5001.0, TurnsReport::Active), vec![]);
    t.report(&p, 5002.0, TurnsReport::Idle);
    t.afk_changed(6000.0, false);
    assert_eq!(t.tick(&p, 6599.0, false), vec![], "the idle clock restarts when AFK ends");
    assert_eq!(t.tick(&p, 6600.0, false), vec![SleepAction::Sleep]);
}

#[test]
fn afk_on_while_asleep_hands_the_scene_to_afk() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    t.tick(&p, 600.0, false);
    t.afk_changed(700.0, true);
    assert!(!t.is_asleep(), "AFK took the scene; AFK off returns to the main scene");
    assert_eq!(t.report(&p, 800.0, TurnsReport::Active), vec![]);
}

#[test]
fn disabled_never_acts_but_keeps_state() {
    let p = SleepParams { enabled: false, ..on() };
    let mut t = SleepTracker::new(0.0);
    assert_eq!(t.report(&p, 0.0, TurnsReport::Idle), vec![]);
    assert_eq!(t.tick(&p, 100_000.0, false), vec![]);
    assert_eq!(t.activity(&p, 100_001.0), vec![]);
}

#[test]
fn org_turns_parses_and_refuses_unknown_states() {
    let ok = |s: &str| parse_command(&format!(r#"{{"v":1,"type":"cmd","cmd_id":"x","cmd":"org.turns","args":{{"state":"{s}"}}}}"#));
    assert_eq!(ok("active").unwrap().1, Command::OrgTurns { state: TurnsReport::Active });
    assert_eq!(ok("idle").unwrap().1, Command::OrgTurns { state: TurnsReport::Idle });
    assert_eq!(ok("resync").unwrap().1, Command::OrgTurns { state: TurnsReport::Resync });
    assert_eq!(ok("self_active").unwrap().1, Command::OrgTurns { state: TurnsReport::SelfActive });
    assert_eq!(ok("self_idle").unwrap().1, Command::OrgTurns { state: TurnsReport::SelfIdle });
    assert_eq!(ok("busy").unwrap_err().code, "bad_args");
    let extra = parse_command(r#"{"v":1,"type":"cmd","cmd_id":"x","cmd":"org.turns","args":{"state":"idle","n":1}}"#);
    assert_eq!(extra.unwrap_err().code, "bad_args");
}

// Live feed (item stream-bridge-switch-from-the-liaison-s-relay-wa): a lost feed holds the
// current state. It never wakes a sleeping stream, and never lets an awake one fall asleep.
#[test]
fn feed_lost_holds_awake_without_an_idle_clock() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    assert_eq!(t.report(&p, 100.0, TurnsReport::FeedLost), vec![]);
    assert_eq!(t.tick(&p, 10_000.0, false), vec![], "no idle clock while the feed is lost");
    assert!(!t.is_asleep());
    t.report(&p, 10_000.0, TurnsReport::Idle);
    assert_eq!(t.tick(&p, 10_599.0, false), vec![]);
    assert_eq!(t.tick(&p, 10_600.0, false), vec![SleepAction::Sleep], "the idle clock starts again on resync");
}

#[test]
fn feed_lost_keeps_a_sleeping_stream_asleep() {
    let p = on();
    let mut t = SleepTracker::new(0.0);
    t.report(&p, 0.0, TurnsReport::Idle);
    assert_eq!(t.tick(&p, 600.0, false), vec![SleepAction::Sleep]);
    assert_eq!(t.report(&p, 700.0, TurnsReport::FeedLost), vec![]);
    assert_eq!(t.tick(&p, 5_000.0, false), vec![], "no wake from a lost feed");
    assert!(t.is_asleep());
    assert_eq!(t.report(&p, 5_000.0, TurnsReport::Active), vec![SleepAction::Wake], "a real count wakes it");
}
