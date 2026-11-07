//! §7.2 / §8.1: what happens to each viewer message and each donation's text.
//!
//! Order for chat: bot/empty drop → personal hold → AFK → minimum donation (D) →
//! per-viewer wait from M scaled by credit → size → flags → global cap → forward.

use std::collections::VecDeque;

use crate::curve::{cooldown_s, max_chars, CurveParams};
use crate::gate::Gate;
use crate::metrics::InterruptionMeter;
use crate::text::{flags_for, normalize, truncate_graphemes, Flag, FlagContext};

const RECENT_KEEP: usize = 3;

#[derive(Debug, Clone, PartialEq)]
pub struct ViewerState {
    pub next_allowed_at: f64,
    pub held_until: Option<f64>,
    pub first_seen: f64,
    /// The viewer's last messages, normalized and lowercased (for the repeat flag).
    pub recent: VecDeque<String>,
}

impl ViewerState {
    pub fn new(first_seen: f64) -> Self {
        ViewerState { next_allowed_at: f64::NEG_INFINITY, held_until: None, first_seen, recent: VecDeque::new() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldReason {
    Hold,
    Afk,
    Gate,
    Cooldown,
    GlobalCap,
}

impl HoldReason {
    pub fn as_str(self) -> &'static str {
        match self {
            HoldReason::Hold => "hold",
            HoldReason::Afk => "afk",
            HoldReason::Gate => "gate",
            HoldReason::Cooldown => "cooldown",
            HoldReason::GlobalCap => "global_cap",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    Empty,
    Bot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Forwarded {
    pub text: String,
    pub truncated: bool,
    pub orig_chars: usize,
    pub flags: Vec<Flag>,
    pub cooldown_s: f64,
    pub max_chars: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Forward(Forwarded),
    Held(HoldReason),
    Dropped(DropReason),
}

pub struct ChatCtx<'a> {
    pub now: f64,
    pub afk: bool,
    pub is_bot: bool,
    pub gate: &'a Gate,
    pub curve: &'a CurveParams,
    /// The viewer's current credit C(t), floor included.
    pub credit_usd: f64,
    pub viewer: &'a mut ViewerState,
    pub global_cap: &'a mut RateWindow,
    pub meter: &'a mut InterruptionMeter,
}

pub fn process_chat(ctx: &mut ChatCtx<'_>, raw: &str) -> Outcome {
    if ctx.is_bot {
        return Outcome::Dropped(DropReason::Bot);
    }
    let text = normalize(raw);
    if text.is_empty() {
        return Outcome::Dropped(DropReason::Empty);
    }
    let now = ctx.now;
    let viewer = &mut *ctx.viewer;
    let prior: Vec<String> = viewer.recent.iter().cloned().collect();
    viewer.recent.push_back(text.to_lowercase());
    while viewer.recent.len() > RECENT_KEEP {
        viewer.recent.pop_front();
    }

    match viewer.held_until {
        Some(h) if now < h => return Outcome::Held(HoldReason::Hold),
        Some(_) => viewer.held_until = None,
        None => {}
    }
    if ctx.afk {
        return Outcome::Held(HoldReason::Afk);
    }
    if ctx.credit_usd < ctx.gate.min_donation_usd {
        return Outcome::Held(HoldReason::Gate);
    }
    if now < viewer.next_allowed_at {
        return Outcome::Held(HoldReason::Cooldown);
    }
    let max = max_chars(ctx.curve, ctx.credit_usd);
    let (cut, truncated, orig_chars) = truncate_graphemes(&text, max as usize);
    let flags = flags_for(&cut, &FlagContext { now, first_seen: viewer.first_seen, recent: &prior });
    if ctx.global_cap.try_acquire(now).is_err() {
        return Outcome::Held(HoldReason::GlobalCap);
    }
    let wait = cooldown_s(ctx.curve, ctx.gate.min_interval_s, ctx.credit_usd);
    viewer.next_allowed_at = now + wait;
    ctx.meter.record(now);
    Outcome::Forward(Forwarded { text: cut, truncated, orig_chars, flags, cooldown_s: wait, max_chars: max })
}

/// §6.2: a donation shortens a pending wait at once; it never lengthens one.
pub fn on_donation(viewer: &mut ViewerState, now: f64, curve: &CurveParams, gate: &Gate, credit_after: f64) {
    let t = cooldown_s(curve, gate.min_interval_s, credit_after);
    viewer.next_allowed_at = viewer.next_allowed_at.min(now + t);
}

#[derive(Debug, Clone, PartialEq)]
pub enum DonationText {
    Forward { text: String, truncated: bool, orig_chars: usize },
    Held,
    Empty,
}

/// §7.2: a donation's text passes when the donor's credit after it is at least D.
/// It is never held by M. The donation *event* itself always goes out.
pub fn process_donation_text(gate: &Gate, curve: &CurveParams, credit_after: f64, raw: &str) -> DonationText {
    let text = normalize(raw);
    if text.is_empty() {
        return DonationText::Empty;
    }
    if credit_after < gate.min_donation_usd {
        return DonationText::Held;
    }
    let (text, truncated, orig_chars) = truncate_graphemes(&text, max_chars(curve, credit_after) as usize);
    DonationText::Forward { text, truncated, orig_chars }
}

/// A sliding-window limiter: at least `min_gap_s` between acquisitions and at most
/// `max_per_window` within any `window_s`. Used for the global cap (§8.3) and the
/// outbound chat limit (§8.4).
#[derive(Debug, Clone, PartialEq)]
pub struct RateWindow {
    min_gap_s: f64,
    max_per_window: usize,
    window_s: f64,
    sent: VecDeque<f64>,
}

impl RateWindow {
    pub fn new(min_gap_s: f64, max_per_window: usize, window_s: f64) -> Self {
        RateWindow { min_gap_s, max_per_window, window_s, sent: VecDeque::new() }
    }

    /// Whether an acquisition would be allowed now, without recording one.
    pub fn check(&self, now: f64) -> Result<(), f64> {
        let live: Vec<f64> = self.sent.iter().copied().filter(|&t| t > now - self.window_s).collect();
        if let Some(&last) = live.last() {
            let gap = now - last;
            if gap < self.min_gap_s {
                return Err(self.min_gap_s - gap);
            }
        }
        if live.len() >= self.max_per_window {
            return Err(live[0] + self.window_s - now);
        }
        Ok(())
    }

    /// Ok if allowed now (and records it); otherwise the seconds to wait.
    pub fn try_acquire(&mut self, now: f64) -> Result<(), f64> {
        while self.sent.front().is_some_and(|&t| t <= now - self.window_s) {
            self.sent.pop_front();
        }
        if let Some(&last) = self.sent.back() {
            let gap = now - last;
            if gap < self.min_gap_s {
                return Err(self.min_gap_s - gap);
            }
        }
        if self.sent.len() >= self.max_per_window {
            let oldest = *self.sent.front().expect("non-empty when full");
            return Err(oldest + self.window_s - now);
        }
        self.sent.push_back(now);
        Ok(())
    }
}
