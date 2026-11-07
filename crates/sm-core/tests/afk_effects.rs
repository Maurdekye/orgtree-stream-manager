//! Plan §5 rev 6 (AFK fades in after quiet, exits at once on activity, never ends the
//! stream) and §18 (donation-triggered effects).

use sm_core::afk::{Activity, AfkAction, AfkKind, AfkParams, AfkTracker};
use sm_core::effects::{choose_donation_effect, Choice, DonationRule, EffectCooldowns};

fn params() -> AfkParams {
    AfkParams { quiet_s: 1800.0, fade_s: 120.0, drops_campaign: false }
}

#[test]
fn fades_in_after_quiet_and_exits_immediately_on_activity() {
    let p = params();
    let mut t = AfkTracker::new(0.0);
    assert_eq!(t.tick(&p, 1799.0), vec![]);
    assert_eq!(t.tick(&p, 1800.0), vec![AfkAction::FadeIn { fade_s: 120.0 }]);
    assert_eq!(t.kind(), Some(AfkKind::Auto));
    assert_eq!(t.tick(&p, 5000.0), vec![], "fades in only once");
    // Any activity: chat (even held), donation or agent command.
    assert_eq!(t.activity(&p, 5001.0, Activity::ViewerChat), vec![AfkAction::CutBack]);
    assert_eq!(t.kind(), None);
    // The quiet clock restarts from the activity.
    assert_eq!(t.tick(&p, 5001.0 + 1799.0), vec![]);
    assert_eq!(t.tick(&p, 5001.0 + 1800.0), vec![AfkAction::FadeIn { fade_s: 120.0 }]);
    assert_eq!(t.activity(&p, 9000.0, Activity::Donation), vec![AfkAction::CutBack]);
    t.tick(&p, 9000.0 + 1800.0);
    assert_eq!(t.activity(&p, 20000.0, Activity::AgentCommand), vec![AfkAction::CutBack]);
}

#[test]
fn activity_resets_the_quiet_clock_while_live() {
    let p = params();
    let mut t = AfkTracker::new(0.0);
    assert_eq!(t.activity(&p, 1000.0, Activity::ViewerChat), vec![]);
    assert_eq!(t.tick(&p, 1800.0), vec![]);
    assert_eq!(t.tick(&p, 2800.0), vec![AfkAction::FadeIn { fade_s: 120.0 }]);
}

#[test]
fn manual_afk_is_sticky_and_only_a_command_ends_it() {
    let p = params();
    let mut t = AfkTracker::new(0.0);
    assert_eq!(t.set_manual(&p, 10.0, true), vec![AfkAction::CutToAfk]);
    assert_eq!(t.kind(), Some(AfkKind::Manual));
    assert_eq!(t.activity(&p, 20.0, Activity::ViewerChat), vec![]);
    assert_eq!(t.activity(&p, 30.0, Activity::Donation), vec![]);
    assert_eq!(t.tick(&p, 1e9), vec![], "never ends the stream on a timer");
    assert_eq!(t.set_manual(&p, 40.0, false), vec![AfkAction::CutBack]);
    assert_eq!(t.kind(), None);
    // Turning AFK off when not AFK does nothing.
    assert_eq!(t.set_manual(&p, 50.0, false), vec![]);
}

#[test]
fn manual_on_during_auto_converts_to_manual_without_a_new_cut() {
    let p = params();
    let mut t = AfkTracker::new(0.0);
    t.tick(&p, 1800.0);
    assert_eq!(t.set_manual(&p, 1900.0, true), vec![]);
    assert_eq!(t.kind(), Some(AfkKind::Manual));
    assert_eq!(t.activity(&p, 1901.0, Activity::ViewerChat), vec![]);
}

#[test]
fn drops_campaign_warns_and_never_ends_the_stream() {
    let p = AfkParams { drops_campaign: true, ..params() };
    let mut t = AfkTracker::new(0.0);
    assert_eq!(t.tick(&p, 1800.0), vec![AfkAction::FadeIn { fade_s: 120.0 }, AfkAction::WarnDrops]);
    t.activity(&p, 1801.0, Activity::ViewerChat);
    assert_eq!(t.set_manual(&p, 1802.0, true), vec![AfkAction::CutToAfk, AfkAction::WarnDrops]);
}

fn rules() -> Vec<DonationRule> {
    vec![
        DonationRule { min_usd: 1.0, effect: "chime".into(), sources: vec![] },
        DonationRule { min_usd: 5.0, effect: "airhorn".into(), sources: vec![] },
        DonationRule { min_usd: 20.0, effect: "confetti".into(), sources: vec!["youtube_superchat".into()] },
    ]
}

#[test]
fn highest_matching_rule_wins_and_sources_filter_applies() {
    let cd = EffectCooldowns::default();
    assert_eq!(choose_donation_effect(&rules(), &cd, 0.0, 0.99, "twitch_bits"), Choice::None);
    assert_eq!(choose_donation_effect(&rules(), &cd, 0.0, 1.0, "twitch_bits"), Choice::Play("chime".into()));
    assert_eq!(choose_donation_effect(&rules(), &cd, 0.0, 7.0, "twitch_bits"), Choice::Play("airhorn".into()));
    // $25 bits: confetti is superchat-only, so airhorn.
    assert_eq!(choose_donation_effect(&rules(), &cd, 0.0, 25.0, "twitch_bits"), Choice::Play("airhorn".into()));
    assert_eq!(choose_donation_effect(&rules(), &cd, 0.0, 25.0, "youtube_superchat"), Choice::Play("confetti".into()));
}

#[test]
fn cooling_effect_falls_back_to_the_next_lower_rule() {
    let mut cd = EffectCooldowns::default();
    cd.started("airhorn", 0.0, 30.0);
    assert_eq!(choose_donation_effect(&rules(), &cd, 10.0, 7.0, "streamlabs"), Choice::Play("chime".into()));
    cd.started("chime", 10.0, 10.0);
    assert_eq!(choose_donation_effect(&rules(), &cd, 15.0, 7.0, "streamlabs"), Choice::Cooldown);
    assert_eq!(choose_donation_effect(&rules(), &cd, 30.0, 7.0, "streamlabs"), Choice::Play("airhorn".into()));
}
