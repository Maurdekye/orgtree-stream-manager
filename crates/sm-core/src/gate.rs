//! §7: the stream-wide message gate. Two continuous values, uncapped (Q15):
//! M = minimum time between one viewer's messages, D = minimum donation credit.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GateParams {
    pub start_interval_s: f64,
    pub start_donation_usd: f64,
    pub interval_factor: f64,
    /// Tightening M from below this goes straight to it; loosening below it gives 0.
    pub interval_first_step_s: f64,
    pub donation_factor: f64,
    /// Tightening D from below this goes straight to it; loosening below it gives 0.
    pub donation_first_step_usd: f64,
    /// At most one agent change per this many seconds.
    pub damping_s: f64,
    pub default_hold_min: u32,
    pub auto_relax: bool,
}

impl Default for GateParams {
    fn default() -> Self {
        GateParams {
            start_interval_s: 120.0,
            start_donation_usd: 0.0,
            interval_factor: 1.5,
            interval_first_step_s: 30.0,
            donation_factor: 2.0,
            donation_first_step_usd: 1.0,
            damping_s: 60.0,
            default_hold_min: 20,
            auto_relax: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateValue {
    MinInterval,
    MinDonation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Actor {
    Agent,
    Operator,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GateChange {
    pub changed: bool,
    /// (min_interval_s, min_donation_usd) before and after.
    pub from: (f64, f64),
    pub to: (f64, f64),
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum GateError {
    #[error("gate changes are limited to one a minute; retry in {retry_after_s:.0} s")]
    Damped { retry_after_s: f64 },
    #[error("{0} must be a finite number of at least 0")]
    OutOfRange(&'static str),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Gate {
    pub min_interval_s: f64,
    pub min_donation_usd: f64,
    /// When auto-relax may take its next step, if the gate is above its starting values.
    pub relax_at: Option<f64>,
    last_agent_change_at: Option<f64>,
}

impl Gate {
    pub fn new(p: &GateParams) -> Self {
        Gate {
            min_interval_s: p.start_interval_s,
            min_donation_usd: p.start_donation_usd,
            relax_at: None,
            last_agent_change_at: None,
        }
    }

    fn values(&self) -> (f64, f64) {
        (self.min_interval_s, self.min_donation_usd)
    }

    pub fn tighten(
        &mut self,
        p: &GateParams,
        v: GateValue,
        now: f64,
        hold_min: Option<u32>,
        actor: Actor,
    ) -> Result<GateChange, GateError> {
        let (m, d) = self.values();
        let target = match v {
            GateValue::MinInterval => (tighten_interval(p, m), d),
            GateValue::MinDonation => (m, tighten_donation(p, d)),
        };
        self.apply(p, target, now, hold_min, actor)
    }

    pub fn loosen(&mut self, p: &GateParams, v: GateValue, now: f64, actor: Actor) -> Result<GateChange, GateError> {
        let (m, d) = self.values();
        let target = match v {
            GateValue::MinInterval => (loosen_interval(p, m), d),
            GateValue::MinDonation => (m, loosen_donation(p, d)),
        };
        self.apply(p, target, now, None, actor)
    }

    pub fn set(
        &mut self,
        p: &GateParams,
        min_interval_s: Option<f64>,
        min_donation_usd: Option<f64>,
        now: f64,
        hold_min: Option<u32>,
        actor: Actor,
    ) -> Result<GateChange, GateError> {
        if let Some(m) = min_interval_s {
            check_value(m, "min_interval_s")?;
        }
        if let Some(d) = min_donation_usd {
            check_value(d, "min_donation_usd")?;
        }
        let target = (min_interval_s.unwrap_or(self.min_interval_s), min_donation_usd.unwrap_or(self.min_donation_usd));
        self.apply(p, target, now, hold_min, actor)
    }

    fn apply(
        &mut self,
        p: &GateParams,
        to: (f64, f64),
        now: f64,
        hold_min: Option<u32>,
        actor: Actor,
    ) -> Result<GateChange, GateError> {
        let from = self.values();
        if to == from {
            return Ok(GateChange { changed: false, from, to });
        }
        if actor == Actor::Agent {
            if let Some(last) = self.last_agent_change_at {
                let elapsed = now - last;
                if elapsed < p.damping_s {
                    return Err(GateError::Damped { retry_after_s: p.damping_s - elapsed });
                }
            }
            self.last_agent_change_at = Some(now);
        }
        self.min_interval_s = to.0;
        self.min_donation_usd = to.1;
        let raised = to.0 > from.0 || to.1 > from.1;
        if raised {
            let hold = hold_min.unwrap_or(p.default_hold_min) as f64 * 60.0;
            self.relax_at = Some(now + hold);
        }
        if !self.above_start(p) {
            self.relax_at = None;
        }
        Ok(GateChange { changed: true, from, to })
    }

    fn above_start(&self, p: &GateParams) -> bool {
        self.min_interval_s > p.start_interval_s || self.min_donation_usd > p.start_donation_usd
    }

    /// §7.3 auto-relax: once `relax_at` has passed and nothing was forwarded in the last
    /// 15 minutes, take one loosen step (D first, then M), never below the starting values.
    pub fn auto_relax(&mut self, p: &GateParams, now: f64, interruptions_15: u32) -> Option<GateChange> {
        if !p.auto_relax || interruptions_15 > 0 {
            return None;
        }
        let due = self.relax_at?;
        if now < due {
            return None;
        }
        let from = self.values();
        if self.min_donation_usd > p.start_donation_usd {
            self.min_donation_usd = loosen_donation(p, self.min_donation_usd).max(p.start_donation_usd);
        } else if self.min_interval_s > p.start_interval_s {
            self.min_interval_s = loosen_interval(p, self.min_interval_s).max(p.start_interval_s);
        } else {
            self.relax_at = None;
            return None;
        }
        self.relax_at = if self.above_start(p) { Some(now + p.default_hold_min as f64 * 60.0) } else { None };
        Some(GateChange { changed: true, from, to: self.values() })
    }

    /// Moves the relax deadline later, for time spent in AFK (the clock pauses there).
    pub fn shift_relax(&mut self, by_s: f64) {
        if let Some(t) = self.relax_at.as_mut() {
            *t += by_s.max(0.0);
        }
    }
}

fn check_value(x: f64, name: &'static str) -> Result<(), GateError> {
    if x.is_finite() && x >= 0.0 {
        Ok(())
    } else {
        Err(GateError::OutOfRange(name))
    }
}

fn tighten_interval(p: &GateParams, m: f64) -> f64 {
    if m < p.interval_first_step_s {
        p.interval_first_step_s
    } else {
        m * p.interval_factor
    }
}

fn loosen_interval(p: &GateParams, m: f64) -> f64 {
    let y = m / p.interval_factor;
    if y < p.interval_first_step_s {
        0.0
    } else {
        y
    }
}

fn tighten_donation(p: &GateParams, d: f64) -> f64 {
    if d < p.donation_first_step_usd {
        p.donation_first_step_usd
    } else {
        d * p.donation_factor
    }
}

fn loosen_donation(p: &GateParams, d: f64) -> f64 {
    let y = d / p.donation_factor;
    if y < p.donation_first_step_usd {
        0.0
    } else {
        y
    }
}
