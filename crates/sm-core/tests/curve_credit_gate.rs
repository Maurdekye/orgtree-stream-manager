//! Plan §6 (credit and curve) and §7 (the two-value gate).

use sm_core::credit::{Credit, CreditParams, EffectiveClock};
use sm_core::curve::{cooldown_s, max_chars, CurveParams};
use sm_core::gate::{Actor, Gate, GateError, GateParams, GateValue};

fn close(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() <= tol
}

// §6.4 table: M = 120 s (the gate's starting value).
#[test]
fn curve_matches_plan_table_at_m_120() {
    let p = CurveParams::default();
    let rows: &[(f64, f64, u32)] = &[
        (0.0, 120.0, 100),
        (0.5, 80.0, 122),
        (1.0, 60.0, 141),
        (2.0, 40.0, 173),
        (5.0, 20.0, 245),
        (10.0, 10.909, 332),
        (20.0, 5.714, 458),
        (25.0, 4.615, 500),
        (59.0, 2.0, 500),
        (500.0, 2.0, 500),
    ];
    for &(c, t, l) in rows {
        assert!(close(cooldown_s(&p, 120.0, c), t, 0.01), "cooldown at C={c}: {}", cooldown_s(&p, 120.0, c));
        assert_eq!(max_chars(&p, c), l, "max chars at C={c}");
    }
}

// §7.1 examples at M = 600 s.
#[test]
fn curve_matches_plan_examples_at_m_600() {
    let p = CurveParams::default();
    for &(c, t) in &[(0.0, 600.0), (1.0, 300.0), (5.0, 100.0), (20.0, 28.571), (59.0, 10.0)] {
        assert!(close(cooldown_s(&p, 600.0, c), t, 0.01), "C={c}");
    }
}

#[test]
fn zero_interval_means_no_wait_and_tiny_interval_is_not_raised_by_floor() {
    let p = CurveParams::default();
    assert_eq!(cooldown_s(&p, 0.0, 0.0), 0.0);
    assert_eq!(cooldown_s(&p, 0.0, 50.0), 0.0);
    // T_min never makes the wait longer than M itself.
    assert!(close(cooldown_s(&p, 1.0, 0.0), 1.0, 1e-9));
}

#[test]
fn credit_decays_with_half_life_and_floor_applies_to_paid_subs() {
    let p = CreditParams::default(); // half-life 1800 s, floor $1
    let mut c = Credit::new(false);
    c.add(0.0, 5.0);
    assert!(close(c.value(&p, 0.0), 5.0, 1e-9));
    assert!(close(c.value(&p, 1800.0), 2.5, 1e-9));
    assert!(close(c.value(&p, 3600.0), 1.25, 1e-9));
    assert!(close(c.value(&p, 7200.0), 0.3125, 1e-9));

    let mut s = Credit::new(true);
    assert!(close(s.value(&p, 0.0), 1.0, 1e-9));
    s.add(0.0, 5.0); // 500 bits from a subscriber: §6.4 example C = $6
    assert!(close(s.value(&p, 0.0), 6.0, 1e-9));
    assert!(close(s.value(&p, 7200.0), 1.3125, 1e-9));
}

#[test]
fn afk_pauses_decay_through_the_effective_clock() {
    let p = CreditParams::default();
    let mut clock = EffectiveClock::default();
    let mut c = Credit::new(false);
    c.add(clock.effective(0.0), 4.0);
    clock.pause(100.0);
    clock.resume(1900.0); // 30 min of AFK: no decay
    assert!(close(c.value(&p, clock.effective(1900.0)), 4.0 * 2f64.powf(-100.0 / 1800.0), 1e-9));
    assert!(close(c.value(&p, clock.effective(3700.0)), 2.0 * 2f64.powf(-100.0 / 1800.0), 1e-9));
}

#[test]
fn gate_starts_at_plan_defaults() {
    let p = GateParams::default();
    let g = Gate::new(&p);
    assert_eq!(g.min_interval_s, 120.0);
    assert_eq!(g.min_donation_usd, 0.0);
    assert_eq!(g.relax_at, None);
}

#[test]
fn gate_standard_steps() {
    let p = GateParams::default();
    let mut g = Gate::new(&p);
    let mut t = 0.0;
    let mut step = |g: &mut Gate, v: GateValue, up: bool| {
        t += 61.0;
        if up {
            g.tighten(&p, v, t, None, Actor::Agent).unwrap()
        } else {
            g.loosen(&p, v, t, Actor::Agent).unwrap()
        }
    };
    step(&mut g, GateValue::MinInterval, true);
    assert!(close(g.min_interval_s, 180.0, 1e-9));
    step(&mut g, GateValue::MinDonation, true);
    assert_eq!(g.min_donation_usd, 1.0);
    step(&mut g, GateValue::MinDonation, true);
    assert_eq!(g.min_donation_usd, 2.0);
    step(&mut g, GateValue::MinDonation, false);
    assert_eq!(g.min_donation_usd, 1.0);
    step(&mut g, GateValue::MinDonation, false);
    assert_eq!(g.min_donation_usd, 0.0); // $0.50 is below $1, so it becomes $0
    // Interval down: 180 -> 120 -> 80 -> 53.3 -> 35.6 -> 23.7 => 0
    for _ in 0..5 {
        step(&mut g, GateValue::MinInterval, false);
    }
    assert_eq!(g.min_interval_s, 0.0);
    // From 0, one tighten goes straight to 30 s.
    step(&mut g, GateValue::MinInterval, true);
    assert_eq!(g.min_interval_s, 30.0);
}

// Q15 (USER m23757a8a2fc7): no caps on M or D in either direction; only negatives
// and non-finite values are refused.
#[test]
fn gate_has_no_caps_but_refuses_negative_or_non_finite() {
    let p = GateParams::default();
    let mut g = Gate::new(&p);
    let ch = g.set(&p, Some(86_400.0 * 7.0), Some(10_000.0), 0.0, None, Actor::Operator).unwrap();
    assert!(ch.changed);
    g.tighten(&p, GateValue::MinInterval, 1.0, None, Actor::Operator).unwrap();
    assert!(close(g.min_interval_s, 86_400.0 * 7.0 * 1.5, 1e-6));
    g.tighten(&p, GateValue::MinDonation, 2.0, None, Actor::Operator).unwrap();
    assert_eq!(g.min_donation_usd, 20_000.0);
    for bad in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(matches!(g.set(&p, Some(bad), None, 3.0, None, Actor::Operator), Err(GateError::OutOfRange(_))));
        assert!(matches!(g.set(&p, None, Some(bad), 3.0, None, Actor::Operator), Err(GateError::OutOfRange(_))));
    }
    // Loosening at zero does nothing.
    g.set(&p, Some(0.0), Some(0.0), 4.0, None, Actor::Operator).unwrap();
    assert!(!g.loosen(&p, GateValue::MinInterval, 5.0, Actor::Operator).unwrap().changed);
    assert!(!g.loosen(&p, GateValue::MinDonation, 6.0, Actor::Operator).unwrap().changed);
}

#[test]
fn agent_changes_are_damped_to_one_per_minute_but_noops_do_not_count() {
    let p = GateParams::default();
    let mut g = Gate::new(&p);
    g.tighten(&p, GateValue::MinInterval, 0.0, None, Actor::Agent).unwrap();
    match g.loosen(&p, GateValue::MinInterval, 30.0, Actor::Agent) {
        Err(GateError::Damped { retry_after_s }) => assert!(close(retry_after_s, 30.0, 1e-9)),
        other => panic!("expected damping, got {other:?}"),
    }
    // A no-op set is allowed and is not a change.
    let ch = g.set(&p, Some(g.min_interval_s), None, 31.0, None, Actor::Agent).unwrap();
    assert!(!ch.changed);
    // The operator is never damped.
    g.loosen(&p, GateValue::MinInterval, 32.0, Actor::Operator).unwrap();
    g.loosen(&p, GateValue::MinInterval, 61.0, Actor::Agent).unwrap();
}

#[test]
fn auto_relax_steps_donation_first_then_interval_never_below_start() {
    let p = GateParams::default();
    let mut g = Gate::new(&p);
    g.set(&p, Some(270.0), Some(2.0), 0.0, Some(20), Actor::Agent).unwrap();
    assert_eq!(g.relax_at, Some(1200.0));
    // Not yet due; and not while chat is still arriving.
    assert!(g.auto_relax(&p, 1199.0, 0).is_none());
    assert!(g.auto_relax(&p, 1200.0, 3).is_none());
    let ch = g.auto_relax(&p, 1200.0, 0).unwrap();
    assert_eq!(ch.to, (270.0, 1.0));
    assert_eq!(g.relax_at, Some(2400.0));
    g.auto_relax(&p, 2400.0, 0).unwrap();
    assert_eq!(g.min_donation_usd, 0.0);
    g.auto_relax(&p, 3600.0, 0).unwrap();
    assert!(close(g.min_interval_s, 180.0, 1e-9));
    g.auto_relax(&p, 4800.0, 0).unwrap();
    assert_eq!(g.min_interval_s, 120.0, "never below the starting value");
    assert_eq!(g.relax_at, None);
    assert!(g.auto_relax(&p, 9999.0, 0).is_none());
}

#[test]
fn auto_relax_can_be_turned_off() {
    let p = GateParams { auto_relax: false, ..GateParams::default() };
    let mut g = Gate::new(&p);
    g.tighten(&p, GateValue::MinDonation, 0.0, None, Actor::Agent).unwrap();
    assert!(g.auto_relax(&p, 1e9, 0).is_none());
}
