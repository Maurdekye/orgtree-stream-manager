//! §6.3 value table and §3.3 Streamlabs relay rule.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    TwitchBits { bits: u64 },
    /// Paid subs; a gift batch has `count` > 1 and is credited to the gifter.
    TwitchSub { tier: u8, count: u32 },
    YouTubeSuperChat { amount_micros: u64, currency: String },
    YouTubeMembership { amount_micros: u64, currency: String },
    Streamlabs { amount: String, currency: String },
}

impl Source {
    /// The source name used in events and in effect rules (§18).
    pub fn name(&self) -> &'static str {
        match self {
            Source::TwitchBits { .. } => "twitch_bits",
            Source::TwitchSub { .. } => "twitch_sub",
            Source::YouTubeSuperChat { .. } => "youtube_superchat",
            Source::YouTubeMembership { .. } => "youtube_membership",
            Source::Streamlabs { .. } => "streamlabs",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ValueTable {
    pub bits_per_usd: f64,
    /// Tier 1, 2, 3.
    pub sub_tier_usd: [f64; 3],
    /// USD per one unit of each currency (operator-maintained static table, Q9).
    pub rates: HashMap<String, f64>,
}

impl Default for ValueTable {
    fn default() -> Self {
        ValueTable { bits_per_usd: 100.0, sub_tier_usd: [5.0, 10.0, 25.0], rates: HashMap::from([("USD".to_string(), 1.0)]) }
    }
}

impl ValueTable {
    pub fn set_rate(&mut self, currency: &str, usd_per_unit: f64) {
        self.rates.insert(currency.to_uppercase(), usd_per_unit);
    }

    fn convert(&self, amount: f64, currency: &str) -> Option<f64> {
        if !amount.is_finite() || amount <= 0.0 {
            return None;
        }
        let rate = self.rates.get(&currency.to_uppercase())?;
        Some(amount * rate)
    }

    /// The USD value, or None when it can't be valued (unknown tier or currency, bad
    /// amount). Unvalued donations are still recorded but give no credit.
    pub fn usd(&self, s: &Source) -> Option<f64> {
        match s {
            Source::TwitchBits { bits } => (*bits > 0).then(|| *bits as f64 / self.bits_per_usd),
            Source::TwitchSub { tier, count } => {
                let price = *self.sub_tier_usd.get((*tier as usize).checked_sub(1)?)?;
                (*count > 0).then(|| price * *count as f64)
            }
            Source::YouTubeSuperChat { amount_micros, currency } | Source::YouTubeMembership { amount_micros, currency } => {
                self.convert(*amount_micros as f64 / 1_000_000.0, currency)
            }
            Source::Streamlabs { amount, currency } => self.convert(amount.trim().parse::<f64>().ok()?, currency),
        }
    }
}

/// §3.3: whether a Streamlabs socket event counts as a donation.
/// - its own `donation` events always count;
/// - relays of Twitch bits/subs or YouTube Super Chats/memberships count only when the
///   native connector for that platform is off (otherwise they'd be counted twice);
/// - follows, hosts and raids never count (§16: no rewards for follows).
pub fn streamlabs_counts(kind: &str, relay_for: Option<&str>, native_twitch: bool, native_youtube: bool) -> bool {
    match relay_for {
        None => kind == "donation",
        Some("twitch_account") => !native_twitch && matches!(kind, "bits" | "subscription"),
        Some("youtube_account") => !native_youtube && matches!(kind, "superchat" | "subscription"),
        Some(_) => false,
    }
}
