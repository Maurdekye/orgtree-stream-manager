//! §8.1: text normalization, grapheme-safe truncation and the inbound flags.
//! Flags are labels for the listener; they never block a message on their own.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

/// NFKC, drop invisible/bidi-control characters, turn other control characters and
/// whitespace into single spaces, and trim.
pub fn normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_space = true;
    for c in s.nfkc() {
        if is_invisible(c) {
            continue;
        }
        if c.is_whitespace() || c.is_control() {
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.push(c);
            last_space = false;
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

fn is_invisible(c: char) -> bool {
    matches!(
        c,
        '\u{00AD}' | '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2060}'..='\u{2064}' | '\u{2066}'..='\u{2069}' | '\u{FEFF}'
    )
}

/// Cuts to at most `max` graphemes. Returns (text, truncated, original grapheme count).
pub fn truncate_graphemes(s: &str, max: usize) -> (String, bool, usize) {
    let g: Vec<&str> = s.graphemes(true).collect();
    let orig = g.len();
    if orig <= max {
        (s.to_string(), false, orig)
    } else {
        (g[..max].concat(), true, orig)
    }
}

pub fn grapheme_count(s: &str) -> usize {
    s.graphemes(true).count()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Flag {
    Link,
    InjectionSuspect,
    PiiProbe,
    Repeat,
    CapsFlood,
    NewAccount,
}

pub struct FlagContext<'a> {
    pub now: f64,
    pub first_seen: f64,
    /// The viewer's previous messages, normalized and lowercased.
    pub recent: &'a [String],
}

pub const NEW_ACCOUNT_S: f64 = 600.0;
pub const REPEAT_SIMILARITY: f64 = 0.8;

fn re(cell: &'static OnceLock<Regex>, pat: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pat).expect("static regex"))
}

fn link_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"(?i)\b(?:https?://|www\.)\S+|\b[a-z0-9-]+\.(?:com|net|org|io|gg|tv|ly|me|xyz|ru|co|app|dev|link)\b")
}

fn injection_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(
        &R,
        r#"(?i)ignore\s+(?:all\s+|the\s+|your\s+|any\s+)?(?:previous|prior|above|earlier)|disregard\s+(?:all\s+|the\s+|your\s+|any\s+)?(?:instructions|rules|prompt)|system\s+prompt|you\s+are\s+now|developer\s+mode|jailbreak|new\s+instructions|</?\s*(?:system|assistant|user)\s*>|\[/?(?:system|inst)\]|"(?:tool|function)_?call""#,
    )
}

fn pii_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(
        &R,
        r"(?i)where\s+(?:do|does)\s+(?:you|the\s+owner|your\s+owner|he|she|they)\s+live|what\s+(?:city|country|town|state)\b|\byour\s+(?:home\s+)?(?:address|location|ip|api\s+keys?|keys?|password|token|secrets?|owner|creator)\b|who\s+(?:owns|runs|made|built)\s+(?:you|this)|\bother\s+orgs?\b|\bmail\s*hub\b|what\s+(?:os|computer|machine|pc)\b",
    )
}

pub fn flags_for(text: &str, ctx: &FlagContext<'_>) -> Vec<Flag> {
    let mut f = Vec::new();
    if link_re().is_match(text) {
        f.push(Flag::Link);
    }
    if injection_re().is_match(text) {
        f.push(Flag::InjectionSuspect);
    }
    if pii_re().is_match(text) {
        f.push(Flag::PiiProbe);
    }
    let lower = text.to_lowercase();
    if ctx.recent.iter().any(|r| strsim::normalized_levenshtein(r, &lower) >= REPEAT_SIMILARITY) {
        f.push(Flag::Repeat);
    }
    let letters: Vec<char> = text.chars().filter(|c| c.is_alphabetic()).collect();
    if letters.len() >= 12 {
        let upper = letters.iter().filter(|c| c.is_uppercase()).count();
        if upper as f64 / letters.len() as f64 >= 0.7 {
            f.push(Flag::CapsFlood);
        }
    }
    if ctx.now - ctx.first_seen < NEW_ACCOUNT_S {
        f.push(Flag::NewAccount);
    }
    f
}
