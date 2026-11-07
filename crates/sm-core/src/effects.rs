//! §18: donation-triggered effects. Each donation triggers at most one effect: the rule
//! with the highest `min_usd` at or below its value (and a matching `sources` filter),
//! falling back to lower rules while the chosen effect cools down.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DonationRule {
    pub min_usd: f64,
    pub effect: String,
    /// Empty means every source; otherwise names from `Source::name`.
    #[serde(default)]
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct EffectCooldowns {
    until: HashMap<String, f64>,
}

impl EffectCooldowns {
    pub fn started(&mut self, effect: &str, now: f64, cooldown_s: f64) {
        self.until.insert(effect.to_string(), now + cooldown_s.max(0.0));
    }

    pub fn is_cooling(&self, effect: &str, now: f64) -> bool {
        self.until.get(effect).is_some_and(|&t| t > now)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Choice {
    Play(String),
    /// A rule matched but every matching effect is cooling down.
    Cooldown,
    /// No rule matched (below the lowest `min_usd`, or filtered out).
    None,
}

pub fn choose_donation_effect(rules: &[DonationRule], cd: &EffectCooldowns, now: f64, usd: f64, source: &str) -> Choice {
    let mut matching: Vec<&DonationRule> = rules
        .iter()
        .filter(|r| usd >= r.min_usd && (r.sources.is_empty() || r.sources.iter().any(|s| s == source)))
        .collect();
    if matching.is_empty() {
        return Choice::None;
    }
    matching.sort_by(|a, b| b.min_usd.total_cmp(&a.min_usd));
    for r in matching {
        if !cd.is_cooling(&r.effect, now) {
            return Choice::Play(r.effect.clone());
        }
    }
    Choice::Cooldown
}
