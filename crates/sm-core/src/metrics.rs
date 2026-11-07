//! §7.1 interruption metrics: forwarded viewer chat only (donations don't count).

use std::collections::VecDeque;

#[derive(Debug, Clone, Default)]
pub struct InterruptionMeter {
    times: VecDeque<f64>,
    last: Option<f64>,
}

const KEEP_S: f64 = 900.0;

impl InterruptionMeter {
    pub fn record(&mut self, now: f64) {
        self.times.push_back(now);
        self.last = Some(now);
        while self.times.front().is_some_and(|&t| t < now - KEEP_S) {
            self.times.pop_front();
        }
    }

    fn count_since(&self, since: f64, now: f64) -> usize {
        self.times.iter().filter(|&&t| t >= since && t <= now).count()
    }

    /// `interruptions_per_min_5`: forwarded messages per minute over the last 5 minutes.
    pub fn per_min_5(&self, now: f64) -> f64 {
        self.count_since(now - 300.0, now) as f64 / 5.0
    }

    /// `interruptions_15`: forwarded messages in the last 15 minutes.
    pub fn last_15(&self, now: f64) -> u32 {
        self.count_since(now - KEEP_S, now) as u32
    }

    pub fn last_at(&self) -> Option<f64> {
        self.last
    }
}
