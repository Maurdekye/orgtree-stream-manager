//! §6.2: limits from credit. Smooth, no tiers.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CurveParams {
    /// Shortest wait the curve produces (never longer than M itself).
    pub t_min_s: f64,
    /// Credit scale c₀ in USD.
    pub c0_usd: f64,
    /// Base size L₀ in graphemes.
    pub l0_chars: u32,
    /// Size cap L_max in graphemes (Twitch's own cap is 500).
    pub l_max_chars: u32,
}

impl Default for CurveParams {
    fn default() -> Self {
        CurveParams { t_min_s: 2.0, c0_usd: 1.0, l0_chars: 100, l_max_chars: 500 }
    }
}

/// T(C) = max(min(T_min, M), M / (1 + C/c₀)). M is the gate's minimum time (§7.1).
pub fn cooldown_s(p: &CurveParams, min_interval_s: f64, credit_usd: f64) -> f64 {
    if !(min_interval_s > 0.0) {
        return 0.0;
    }
    let c = if credit_usd.is_finite() { credit_usd.max(0.0) } else { 0.0 };
    let scaled = min_interval_s / (1.0 + c / p.c0_usd);
    scaled.max(p.t_min_s.min(min_interval_s))
}

/// L(C) = min(L_max, round(L₀ · √(1 + C/c₀))).
pub fn max_chars(p: &CurveParams, credit_usd: f64) -> u32 {
    let c = if credit_usd.is_finite() { credit_usd.max(0.0) } else { 0.0 };
    let l = (p.l0_chars as f64 * (1.0 + c / p.c0_usd).sqrt()).round();
    if l >= p.l_max_chars as f64 {
        p.l_max_chars
    } else {
        l as u32
    }
}
