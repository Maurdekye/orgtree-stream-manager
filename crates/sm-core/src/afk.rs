//! §5 (rev 6): AFK fades in by itself after an extended quiet period and ends at once on
//! the first activity. An AFK an agent or the operator set stays until it is turned off.
//! Nothing here ever ends the stream.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AfkParams {
    /// `afk_quiet_min` × 60.
    pub quiet_s: f64,
    /// `afk_fade_s`: length of the OBS fade into the AFK scene.
    pub fade_s: f64,
    pub drops_campaign: bool,
}

impl Default for AfkParams {
    fn default() -> Self {
        AfkParams { quiet_s: 1800.0, fade_s: 120.0, drops_campaign: false }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AfkKind {
    Auto,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activity {
    /// Any viewer message, held or not.
    ViewerChat,
    Donation,
    AgentCommand,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AfkAction {
    /// Fade OBS into the AFK scene over `fade_s` and post the AFK line once.
    FadeIn { fade_s: f64 },
    /// Switch to the AFK scene at once (an explicit `stream.afk on`).
    CutToAfk,
    /// Cut straight back to the remembered scene.
    CutBack,
    /// Send `system.warning` `afk_during_drops` to the listener.
    WarnDrops,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AfkTracker {
    last_activity: f64,
    kind: Option<AfkKind>,
}

impl AfkTracker {
    pub fn new(now: f64) -> Self {
        AfkTracker { last_activity: now, kind: None }
    }

    pub fn kind(&self) -> Option<AfkKind> {
        self.kind
    }

    pub fn is_afk(&self) -> bool {
        self.kind.is_some()
    }

    fn entered(&self, p: &AfkParams, first: AfkAction) -> Vec<AfkAction> {
        let mut v = vec![first];
        if p.drops_campaign {
            v.push(AfkAction::WarnDrops);
        }
        v
    }

    /// Call periodically. Starts the fade after `quiet_s` without activity.
    pub fn tick(&mut self, p: &AfkParams, now: f64) -> Vec<AfkAction> {
        if self.kind.is_none() && now - self.last_activity >= p.quiet_s {
            self.kind = Some(AfkKind::Auto);
            return self.entered(p, AfkAction::FadeIn { fade_s: p.fade_s });
        }
        Vec::new()
    }

    /// Something happened. Ends an AFK that faded in by itself; restarts the quiet clock.
    pub fn activity(&mut self, _p: &AfkParams, now: f64, _what: Activity) -> Vec<AfkAction> {
        self.last_activity = now;
        if self.kind == Some(AfkKind::Auto) {
            self.kind = None;
            return vec![AfkAction::CutBack];
        }
        Vec::new()
    }

    /// `stream.afk on|off` from an agent or the operator.
    pub fn set_manual(&mut self, p: &AfkParams, now: f64, on: bool) -> Vec<AfkAction> {
        if on {
            match self.kind {
                None => {
                    self.kind = Some(AfkKind::Manual);
                    self.entered(p, AfkAction::CutToAfk)
                }
                Some(AfkKind::Auto) => {
                    self.kind = Some(AfkKind::Manual);
                    Vec::new()
                }
                Some(AfkKind::Manual) => Vec::new(),
            }
        } else if self.kind.is_some() {
            self.kind = None;
            self.last_activity = now;
            vec![AfkAction::CutBack]
        } else {
            Vec::new()
        }
    }
}
