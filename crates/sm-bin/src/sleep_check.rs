//! Auto-sleep's read-only OBS startup check (plan rev 2): the sleep scene must exist; a missing
//! per-scene transition override, Studio Mode or a fade longer than `min_sleep_s` only warn.

use serde_json::{json, Value};
use sm_connectors::obs::ObsClient;

#[derive(Debug, Clone, PartialEq)]
pub struct ObsSleepFacts {
    pub scenes: Vec<String>,
    pub override_name: Option<String>,
    pub override_ms: Option<u64>,
    pub studio_mode: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SleepCheck {
    pub usable: bool,
    /// (warning code, detail), in a fixed order.
    pub warnings: Vec<(&'static str, String)>,
}

pub fn sleep_check(scene: &str, min_sleep_s: f64, f: &ObsSleepFacts) -> SleepCheck {
    if !f.scenes.iter().any(|s| s == scene) {
        return SleepCheck {
            usable: false,
            warnings: vec![("sleep_scene_missing", format!("OBS has no scene named {scene:?}; auto-sleep stays off"))],
        };
    }
    let mut w = Vec::new();
    match (&f.override_name, f.override_ms) {
        (Some(_), Some(ms)) => {
            if ms as f64 > min_sleep_s * 1000.0 {
                w.push(("sleep_fade_longer_than_min_sleep", format!("the sleep scene's fade is {ms} ms, longer than sleep.min_sleep_s")));
            }
        }
        _ => w.push(("sleep_no_override", "the sleep scene has no transition override; it switches with the normal transition".to_string())),
    }
    if f.studio_mode {
        w.push(("sleep_studio_mode", "OBS Studio Mode is on; scene switches may skip the sleep scene's transition override".to_string()));
    }
    SleepCheck { usable: true, warnings: w }
}

/// Reads the facts with read-only requests on its own connection. Changes nothing in OBS.
pub async fn read_obs_sleep_facts(url: &str, password: Option<&str>, scene: &str) -> Result<ObsSleepFacts, String> {
    let c = ObsClient::connect(url, password).await.map_err(|e| e.to_string())?;
    let list = c.request("GetSceneList", json!({})).await.map_err(|e| e.to_string())?;
    let scenes = list["scenes"].as_array().map(|a| a.iter().filter_map(|s| s["sceneName"].as_str().map(str::to_string)).collect()).unwrap_or_default();
    let ov = c.request("GetSceneSceneTransitionOverride", json!({"sceneName": scene})).await.unwrap_or(Value::Null);
    let studio = c.request("GetStudioModeEnabled", json!({})).await.map_err(|e| e.to_string())?;
    Ok(ObsSleepFacts {
        scenes,
        override_name: ov["transitionName"].as_str().map(str::to_string),
        override_ms: ov["transitionDuration"].as_u64(),
        studio_mode: studio["studioModeEnabled"].as_bool().unwrap_or(false),
    })
}
