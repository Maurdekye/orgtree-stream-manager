//! Auto-sleep (USER 2026-11-05 15:04:39Z m66df61bea159): "any agents that are active should keep
//! the stream alive, the stream should only die if there are no agents running a turn for an
//! extended period of time". "Die" is the Sleeping scene, never stream.end.
//!
//! The org's turn state arrives as `org.turns` reports from the relay's watchdogs (plan rev 2):
//! `active`/`idle` on the org-wide running-turn count's edges, `resync` after an Orgtree restart,
//! and `self_active`/`self_idle` for the relay agent's own turns (the count excludes its watcher).
//! The tracker leans awake: an unknown state never sleeps, and a missed idle edge only keeps the
//! stream awake. Manual AFK wins: while it is on, sleep never touches scenes.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SleepParams {
    /// The kill switch; ships off.
    pub enabled: bool,
    /// `idle_min` x 60: how long no turn may run before the stream sleeps.
    pub idle_s: f64,
    /// A wake that arrives sooner than this after falling asleep waits for it (the slow fade).
    pub min_sleep_s: f64,
}

impl Default for SleepParams {
    fn default() -> Self {
        SleepParams { enabled: false, idle_s: 600.0, min_sleep_s: 15.0 }
    }
}

/// One `org.turns` report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnsReport {
    Active,
    Idle,
    Resync,
    SelfActive,
    SelfIdle,
    /// The live feed is lost or silent: hold the current state (no wake, no idle clock).
    FeedLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleepAction {
    /// Switch to the sleep scene (a plain scene switch; the scene's own transition override fades).
    Sleep,
    /// Switch back to the main scene.
    Wake,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SleepTracker {
    /// None until the first report: unknown counts as a turn running.
    org_active: Option<bool>,
    self_active: bool,
    /// When the org was last seen with no turn running and nothing since; None while busy.
    idle_since: Option<f64>,
    asleep_since: Option<f64>,
    wake_pending: bool,
    afk: bool,
}

impl SleepTracker {
    pub fn new(_now: f64) -> Self {
        SleepTracker { org_active: None, self_active: false, idle_since: None, asleep_since: None, wake_pending: false, afk: false }
    }

    pub fn is_asleep(&self) -> bool {
        self.asleep_since.is_some()
    }

    /// "off", "awake" or "asleep", for the stats.
    pub fn state_label(&self, p: &SleepParams) -> &'static str {
        if !p.enabled {
            "off"
        } else if self.is_asleep() {
            "asleep"
        } else {
            "awake"
        }
    }

    fn busy(&self) -> bool {
        self.org_active != Some(false) || self.self_active
    }

    /// A turn is (or may be) running: stop the idle clock and wake, or queue the wake.
    fn stir(&mut self, p: &SleepParams, now: f64) -> Vec<SleepAction> {
        if let Some(t) = self.asleep_since {
            if !p.enabled || self.afk {
                return Vec::new();
            }
            if now - t >= p.min_sleep_s {
                self.asleep_since = None;
                self.wake_pending = false;
                return vec![SleepAction::Wake];
            }
            self.wake_pending = true;
        }
        Vec::new()
    }

    pub fn report(&mut self, p: &SleepParams, now: f64, r: TurnsReport) -> Vec<SleepAction> {
        if r == TurnsReport::FeedLost {
            // Asleep stays asleep; awake stays awake with no idle clock until a real count.
            if !self.is_asleep() {
                self.org_active = None;
                self.idle_since = None;
            }
            return Vec::new();
        }
        match r {
            TurnsReport::Active | TurnsReport::Resync => self.org_active = Some(true),
            TurnsReport::Idle => self.org_active = Some(false),
            TurnsReport::SelfActive => self.self_active = true,
            TurnsReport::SelfIdle => self.self_active = false,
            TurnsReport::FeedLost => {}
        }
        if self.busy() {
            self.idle_since = None;
            self.stir(p, now)
        } else {
            if self.idle_since.is_none() {
                self.idle_since = Some(now);
            }
            Vec::new()
        }
    }

    /// An agent command (it came from an agent's turn): restart the idle clock and wake.
    pub fn activity(&mut self, p: &SleepParams, now: f64) -> Vec<SleepAction> {
        if self.idle_since.is_some() {
            self.idle_since = Some(now);
        }
        self.stir(p, now)
    }

    /// Manual AFK on or off. On hands the scene to AFK (sleep is over); off restarts the idle clock.
    pub fn afk_changed(&mut self, now: f64, on: bool) {
        self.afk = on;
        self.asleep_since = None;
        self.wake_pending = false;
        if !on && self.idle_since.is_some() {
            self.idle_since = Some(now);
        }
    }

    /// Call periodically.
    pub fn tick(&mut self, p: &SleepParams, now: f64, afk: bool) -> Vec<SleepAction> {
        if !p.enabled || afk || self.afk {
            return Vec::new();
        }
        if let Some(t) = self.asleep_since {
            if self.wake_pending && now - t >= p.min_sleep_s {
                self.asleep_since = None;
                self.wake_pending = false;
                if !self.busy() {
                    self.idle_since = Some(now);
                }
                return vec![SleepAction::Wake];
            }
            return Vec::new();
        }
        if self.idle_since.is_some_and(|t| now - t >= p.idle_s) {
            self.asleep_since = Some(now);
            return vec![SleepAction::Sleep];
        }
        Vec::new()
    }
}
