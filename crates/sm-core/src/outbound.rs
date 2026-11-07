//! §9.2 + §16: checks on every agent `chat.say` before it reaches a platform.
//! A match blocks the message; the rule name is returned, never the matched text.

use regex::Regex;
use std::collections::VecDeque;
use std::sync::OnceLock;

use crate::text::normalize;

const SECRET_FRAGMENT: usize = 8;

pub struct OutboundFilter {
    secrets: Vec<Vec<char>>,
    deny: Vec<String>,
}

fn patterns() -> &'static [(&'static str, Regex)] {
    static P: OnceLock<Vec<(&'static str, Regex)>> = OnceLock::new();
    P.get_or_init(|| {
        [
            ("key_shape", r"\b(?:sk-[A-Za-z0-9_-]{16,}|ghp_[A-Za-z0-9]{20,}|gho_[A-Za-z0-9]{20,}|xox[abprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{30,})"),
            ("jwt", r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}"),
            ("path", r"(?i)\b[a-z]:\\|/home/[^\s/]+|/users/[^\s/]+"),
            ("ip", r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
            ("email", r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
            ("long_token", r"[A-Za-z0-9+/=_-]{32,}"),
        ]
        .into_iter()
        .map(|(n, p)| (n, Regex::new(p).expect("static regex")))
        .collect()
    })
}

/// The shapes checked on the compacted copy (no word boundaries: spacing is gone).
fn compact_patterns() -> &'static [(&'static str, Regex)] {
    static P: OnceLock<Vec<(&'static str, Regex)>> = OnceLock::new();
    P.get_or_init(|| {
        [
            ("key_shape", r"(?:sk-[A-Za-z0-9_-]{16,}|ghp_[A-Za-z0-9]{20,}|gho_[A-Za-z0-9]{20,}|xox[abprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{30,})"),
            ("jwt", r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}"),
            ("path", r"(?i)[a-z]:\\users\\|/home/[^/]+|/users/[^/]+"),
            ("ip", r"(?:\d{1,3}\.){3}\d{1,3}"),
            ("email", r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
        ]
        .into_iter()
        .map(|(n, p)| (n, Regex::new(p).expect("static regex")))
        .collect()
    })
}

impl OutboundFilter {
    /// `secrets`: every secret value the bridge holds. `deny`: the operator's deny list
    /// (owner names, city, org and mail hub names, the machine name), matched
    /// case-insensitively.
    pub fn new(secrets: Vec<String>, deny: Vec<String>) -> Self {
        OutboundFilter {
            secrets: secrets.into_iter().filter(|s| s.chars().count() >= SECRET_FRAGMENT).map(|s| s.chars().collect()).collect(),
            deny: deny.into_iter().map(|d| d.trim().to_lowercase()).filter(|d| !d.is_empty()).collect(),
        }
    }

    /// Checks the text as written, then a compacted copy (normalized, invisible characters
    /// and all whitespace removed), so "s e c r e t" or a zero-width split can't slip a
    /// secret, key, email, IP or deny-listed name past (review round 1, finding 6).
    /// `long_token` is checked on the original only: removing spaces from ordinary prose
    /// makes long runs of letters.
    pub fn check(&self, text: &str) -> Result<(), &'static str> {
        let normal = normalize(text);
        let compact: String = normal.chars().filter(|c| !c.is_whitespace()).collect();
        for s in &self.secrets {
            for w in s.windows(SECRET_FRAGMENT) {
                let frag: String = w.iter().collect();
                if text.contains(&frag) || normal.contains(&frag) || compact.contains(&frag) {
                    return Err("secret");
                }
            }
        }
        for (name, re) in patterns() {
            if re.is_match(text) || re.is_match(&normal) {
                return Err(name);
            }
        }
        for (name, re) in compact_patterns() {
            if re.is_match(&compact) {
                return Err(name);
            }
        }
        let lower = normal.to_lowercase();
        let lower_compact = compact.to_lowercase();
        if self.deny.iter().any(|d| {
            let dc: String = d.chars().filter(|c| !c.is_whitespace()).collect();
            lower.contains(d.as_str()) || (!dc.is_empty() && lower_compact.contains(dc.as_str()))
        }) {
            return Err("deny_list");
        }
        Ok(())
    }
}

/// Blocks an agent reply too similar to one of its own replies in the last window.
pub struct RepeatGuard {
    window_s: f64,
    threshold: f64,
    sent: VecDeque<(f64, String)>,
}

impl RepeatGuard {
    pub fn new(window_s: f64, threshold: f64) -> Self {
        RepeatGuard { window_s, threshold, sent: VecDeque::new() }
    }

    pub fn check(&self, now: f64, text: &str) -> Result<(), &'static str> {
        let t = normalize(text).to_lowercase();
        let hit = self
            .sent
            .iter()
            .any(|(at, prev)| now - at < self.window_s && strsim::normalized_levenshtein(prev, &t) >= self.threshold);
        if hit {
            Err("outbound_repeat")
        } else {
            Ok(())
        }
    }

    pub fn record(&mut self, now: f64, text: &str) {
        while self.sent.front().is_some_and(|(at, _)| now - at >= self.window_s) {
            self.sent.pop_front();
        }
        self.sent.push_back((now, normalize(text).to_lowercase()));
    }
}
