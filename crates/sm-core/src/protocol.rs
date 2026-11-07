//! §4.3: org → bridge commands, sent with `orgtree_watchdog action:"send"`.
//! Parsing validates every argument; a bad command has no side effects.

use regex::Regex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;

pub const MAX_CHAT_SAY_CHARS: usize = 400;
pub const MAX_REASON_CHARS: usize = 500;
pub const MAX_TIMEOUT_S: u32 = 1_209_600;
pub const MAX_HOLD_MIN: u32 = 240;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    Twitch,
    Youtube,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateValueArg {
    MinInterval,
    MinDonation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    StreamEnd { reason: String, platforms: Option<Vec<Platform>> },
    StreamAfk { on: bool, reason: String },
    FxTrigger { effect: String, params: Option<Value> },
    ChatSay { text: String, platforms: Option<Vec<Platform>>, reply_to: Option<String> },
    ModTimeout { viewer: String, duration_s: u32, reason: String },
    ModBan { viewer: String, reason: String },
    ModUnban { viewer: String, reason: Option<String> },
    ModDelete { msg_id: String },
    ModSlowMode { seconds: u32 },
    GateTighten { value: GateValueArg, reason: String, hold_min: Option<u32> },
    GateLoosen { value: GateValueArg, reason: String },
    GateSet { min_interval_s: Option<f64>, min_donation_usd: Option<f64>, reason: String, hold_min: Option<u32> },
    LimitsHold { viewer: String, minutes: u32 },
    LimitsRelease { viewer: String },
    /// The org's turn state from the auto-sleep relay (plan rev 2).
    OrgTurns { state: crate::sleep::TurnsReport },
    Ping,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CmdError {
    pub code: &'static str,
    pub detail: String,
    /// Reported whenever it could be read, so the result can still be correlated.
    pub cmd_id: Option<String>,
}

fn err(code: &'static str, detail: impl Into<String>, cmd_id: &Option<String>) -> CmdError {
    CmdError { code, detail: detail.into(), cmd_id: cmd_id.clone() }
}

fn de<T: DeserializeOwned>(args: Value, id: &Option<String>) -> Result<T, CmdError> {
    serde_json::from_value(args).map_err(|e| err("bad_args", e.to_string(), id))
}

fn viewer_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^(?:twitch|youtube):[A-Za-z0-9_-]{1,64}$").expect("static regex"))
}

fn effect_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^[a-z0-9_-]{1,64}$").expect("static regex"))
}

fn reason(r: String, id: &Option<String>) -> Result<String, CmdError> {
    let t = r.trim();
    if t.is_empty() || t.chars().count() > MAX_REASON_CHARS {
        return Err(err("bad_args", format!("reason must be 1-{MAX_REASON_CHARS} characters"), id));
    }
    Ok(t.to_string())
}

fn viewer(v: String, id: &Option<String>) -> Result<String, CmdError> {
    if viewer_re().is_match(&v) {
        Ok(v)
    } else {
        Err(err("bad_args", "viewer must look like twitch:<id> or youtube:<id>", id))
    }
}

fn hold(h: Option<u32>, id: &Option<String>) -> Result<Option<u32>, CmdError> {
    match h {
        Some(m) if !(1..=MAX_HOLD_MIN).contains(&m) => Err(err("bad_args", format!("hold_min must be 1-{MAX_HOLD_MIN}"), id)),
        other => Ok(other),
    }
}

fn gate_number(x: Option<f64>, name: &str, id: &Option<String>) -> Result<Option<f64>, CmdError> {
    match x {
        Some(v) if !(v.is_finite() && v >= 0.0) => Err(err("bad_args", format!("{name} must be a number of at least 0"), id)),
        other => Ok(other),
    }
}

/// Parses one command message. Returns its `cmd_id` and the validated command.
pub fn parse_command(raw: &str) -> Result<(String, Command), CmdError> {
    let none = None;
    let v: Value = serde_json::from_str(raw).map_err(|e| err("bad_json", e.to_string(), &none))?;
    let obj = v.as_object().ok_or_else(|| err("bad_json", "a command is a JSON object", &none))?;
    let id: Option<String> = obj.get("cmd_id").and_then(Value::as_str).map(str::to_string);
    if obj.get("v").and_then(Value::as_u64) != Some(1) {
        return Err(err("unsupported_version", "v must be 1", &id));
    }
    if obj.get("type").and_then(Value::as_str) != Some("cmd") {
        return Err(err("bad_args", "type must be \"cmd\"", &id));
    }
    let cmd_id = match &id {
        Some(s) if !s.is_empty() && s.chars().count() <= 100 => s.clone(),
        _ => return Err(err("bad_args", "cmd_id must be 1-100 characters", &id)),
    };
    let cmd = obj.get("cmd").and_then(Value::as_str).ok_or_else(|| err("bad_args", "cmd is required", &id))?;
    let args = obj.get("args").cloned().unwrap_or_else(|| Value::Object(Default::default()));

    let command = match cmd {
        "ping" => Command::Ping,
        "org.turns" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                state: crate::sleep::TurnsReport,
            }
            let a: A = de(args, &id)?;
            Command::OrgTurns { state: a.state }
        }
        "stream.end" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                confirm: String,
                reason: String,
                platforms: Option<Vec<Platform>>,
            }
            let a: A = de(args, &id)?;
            if a.confirm != "END" {
                return Err(err("bad_args", "confirm must be \"END\"", &id));
            }
            Command::StreamEnd { reason: reason(a.reason, &id)?, platforms: a.platforms }
        }
        "stream.afk" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                on: bool,
                reason: String,
            }
            let a: A = de(args, &id)?;
            Command::StreamAfk { on: a.on, reason: reason(a.reason, &id)? }
        }
        "fx.trigger" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                effect: String,
                params: Option<Value>,
            }
            let a: A = de(args, &id)?;
            if !effect_re().is_match(&a.effect) {
                return Err(err("bad_args", "effect must be a catalogue id", &id));
            }
            Command::FxTrigger { effect: a.effect, params: a.params }
        }
        "chat.say" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                text: String,
                platforms: Option<Vec<Platform>>,
                reply_to: Option<String>,
            }
            let a: A = de(args, &id)?;
            let n = a.text.trim().chars().count();
            if n == 0 || a.text.chars().count() > MAX_CHAT_SAY_CHARS {
                return Err(err("bad_args", format!("text must be 1-{MAX_CHAT_SAY_CHARS} characters"), &id));
            }
            Command::ChatSay { text: a.text, platforms: a.platforms, reply_to: a.reply_to }
        }
        "mod.timeout" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                viewer: String,
                duration_s: u32,
                reason: String,
            }
            let a: A = de(args, &id)?;
            if !(1..=MAX_TIMEOUT_S).contains(&a.duration_s) {
                return Err(err("bad_args", format!("duration_s must be 1-{MAX_TIMEOUT_S}"), &id));
            }
            Command::ModTimeout { viewer: viewer(a.viewer, &id)?, duration_s: a.duration_s, reason: reason(a.reason, &id)? }
        }
        "mod.ban" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                viewer: String,
                reason: String,
            }
            let a: A = de(args, &id)?;
            Command::ModBan { viewer: viewer(a.viewer, &id)?, reason: reason(a.reason, &id)? }
        }
        "mod.unban" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                viewer: String,
                reason: Option<String>,
            }
            let a: A = de(args, &id)?;
            let r = match a.reason {
                Some(r) => Some(reason(r, &id)?),
                None => None,
            };
            Command::ModUnban { viewer: viewer(a.viewer, &id)?, reason: r }
        }
        "mod.delete" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                msg_id: String,
            }
            let a: A = de(args, &id)?;
            let n = a.msg_id.chars().count();
            if n == 0 || n > 200 {
                return Err(err("bad_args", "msg_id must be 1-200 characters", &id));
            }
            Command::ModDelete { msg_id: a.msg_id }
        }
        "mod.slow_mode" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                seconds: u32,
            }
            let a: A = de(args, &id)?;
            if a.seconds != 0 && !(3..=120).contains(&a.seconds) {
                return Err(err("bad_args", "seconds must be 0 (off) or 3-120", &id));
            }
            Command::ModSlowMode { seconds: a.seconds }
        }
        "gate.tighten" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                value: GateValueArg,
                reason: String,
                hold_min: Option<u32>,
            }
            let a: A = de(args, &id)?;
            Command::GateTighten { value: a.value, reason: reason(a.reason, &id)?, hold_min: hold(a.hold_min, &id)? }
        }
        "gate.loosen" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                value: GateValueArg,
                reason: String,
            }
            let a: A = de(args, &id)?;
            Command::GateLoosen { value: a.value, reason: reason(a.reason, &id)? }
        }
        "gate.set" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                min_interval_s: Option<f64>,
                min_donation_usd: Option<f64>,
                reason: String,
                hold_min: Option<u32>,
            }
            let a: A = de(args, &id)?;
            if a.min_interval_s.is_none() && a.min_donation_usd.is_none() {
                return Err(err("bad_args", "give min_interval_s, min_donation_usd or both", &id));
            }
            Command::GateSet {
                min_interval_s: gate_number(a.min_interval_s, "min_interval_s", &id)?,
                min_donation_usd: gate_number(a.min_donation_usd, "min_donation_usd", &id)?,
                reason: reason(a.reason, &id)?,
                hold_min: hold(a.hold_min, &id)?,
            }
        }
        "limits.hold" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                viewer: String,
                minutes: u32,
            }
            let a: A = de(args, &id)?;
            if !(1..=1440).contains(&a.minutes) {
                return Err(err("bad_args", "minutes must be 1-1440", &id));
            }
            Command::LimitsHold { viewer: viewer(a.viewer, &id)?, minutes: a.minutes }
        }
        "limits.release" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct A {
                viewer: String,
            }
            let a: A = de(args, &id)?;
            Command::LimitsRelease { viewer: viewer(a.viewer, &id)? }
        }
        other => return Err(err("unknown_cmd", format!("unknown command {other:?}"), &id)),
    };
    Ok((cmd_id, command))
}

/// Idempotency by `cmd_id` (§4.3): a resent command gets its first result back.
pub struct CmdCache {
    cap: usize,
    map: HashMap<String, Value>,
    order: VecDeque<String>,
}

impl CmdCache {
    pub fn new(cap: usize) -> Self {
        CmdCache { cap: cap.max(1), map: HashMap::new(), order: VecDeque::new() }
    }

    pub fn get(&self, cmd_id: &str) -> Option<&Value> {
        self.map.get(cmd_id)
    }

    /// Stores the first result for `cmd_id`; later puts for the same id are ignored.
    pub fn put(&mut self, cmd_id: String, result: Value) {
        if self.map.contains_key(&cmd_id) {
            return;
        }
        self.order.push_back(cmd_id.clone());
        self.map.insert(cmd_id, result);
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
    }
}
