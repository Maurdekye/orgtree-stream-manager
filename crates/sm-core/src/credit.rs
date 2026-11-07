//! §6.1: a viewer's decaying donation credit, C(t) = F + Σ uᵢ · 2^(−(t − tᵢ)/H).
//!
//! Credit is computed in *effective* time, which stops while the stream is AFK, so
//! donors don't lose value while nobody is listening (§5).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CreditParams {
    pub half_life_s: f64,
    /// Floor F for an active PAID subscriber or channel member (never free follows, §16).
    pub sub_floor_usd: f64,
}

impl Default for CreditParams {
    fn default() -> Self {
        CreditParams { half_life_s: 1800.0, sub_floor_usd: 1.0 }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Credit {
    grants: Vec<(f64, f64)>,
    pub paid_sub: bool,
}

const NEGLIGIBLE_USD: f64 = 1e-4;

impl Credit {
    pub fn new(paid_sub: bool) -> Self {
        Credit { grants: Vec::new(), paid_sub }
    }

    /// Adds a donation of `usd` at effective time `t_eff`. Non-positive or non-finite
    /// values are ignored.
    pub fn add(&mut self, t_eff: f64, usd: f64) {
        if usd.is_finite() && usd > 0.0 && t_eff.is_finite() {
            self.grants.push((t_eff, usd));
        }
    }

    pub fn value(&self, p: &CreditParams, t_eff: f64) -> f64 {
        let floor = if self.paid_sub { p.sub_floor_usd } else { 0.0 };
        floor + self.grants.iter().map(|&(ti, u)| decayed(p, u, t_eff - ti)).sum::<f64>()
    }

    /// Drops grants that have decayed to nothing.
    pub fn prune(&mut self, p: &CreditParams, t_eff: f64) {
        self.grants.retain(|&(ti, u)| decayed(p, u, t_eff - ti) >= NEGLIGIBLE_USD);
    }
}

fn decayed(p: &CreditParams, u: f64, age_s: f64) -> f64 {
    if age_s <= 0.0 {
        u
    } else {
        u * 2f64.powf(-age_s / p.half_life_s)
    }
}

/// Wall time minus the time spent paused (AFK).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EffectiveClock {
    paused_total: f64,
    paused_since: Option<f64>,
}

impl EffectiveClock {
    pub fn effective(&self, now: f64) -> f64 {
        let running_pause = self.paused_since.map_or(0.0, |s| (now - s).max(0.0));
        now - self.paused_total - running_pause
    }

    pub fn pause(&mut self, now: f64) {
        if self.paused_since.is_none() {
            self.paused_since = Some(now);
        }
    }

    pub fn resume(&mut self, now: f64) {
        if let Some(s) = self.paused_since.take() {
            self.paused_total += (now - s).max(0.0);
        }
    }

    pub fn is_paused(&self) -> bool {
        self.paused_since.is_some()
    }
}
