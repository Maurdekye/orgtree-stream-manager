//! `stream-manager run [--config PATH] [--secrets PATH]`
//! `stream-manager check-config [--config PATH] [--secrets PATH]`
//! `stream-manager gen-token`                       a 256-bit bridge token
//! `stream-manager setup twitch-bot|twitch-broadcaster|youtube [--config PATH] [--secrets PATH]`
//! `stream-manager consent [--config PATH]`         record the channel owner's consent
//! `stream-manager health`                          for the container health check
//!
//! The run loop (plan §1): connectors → bridge → org events (WebSocket hub) and platform
//! actions (executors); a 1 s tick for AFK/gate timers and stats, stats.json every 5 s,
//! chat retention hourly. Setup (OAuth sign-ins, token generation) is a separate step.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;
use sm_bin::config::{load_config, Config};
use sm_bin::exec::{Executor, HelixSubscriber, ObsCtx, OverlayCtx, TwitchCtx, YtCtx, YtHttpImpl};
use sm_bin::overlay::OverlayHub;
use sm_bin::setup::{self as smsetup, DeviceFlow};
use sm_bin::handler::BridgeHandler;
use sm_bin::http::HttpClient;
use sm_bin::secrets::{load_secrets, Secrets};
use sm_bin::tokens::{TokenConfig, TokenSource};
use sm_bridge::{Action, Bridge};
use sm_connectors::model::InEvent;
use sm_connectors::streamlabs::{run_streamlabs, StreamlabsConfig};
use sm_connectors::twitch::eventsub::{default_specs, run_eventsub, EventSubConfig};
use sm_connectors::youtube::run_youtube_with;
use sm_connectors::ytstate::{YtBans, YtQuota};
use sm_server::{Clock, Hub, ServerConfig};
use sm_store::Store;
use tokio::sync::mpsc;

fn arg(args: &[String], flag: &str, default: &str) -> String {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone()).unwrap_or_else(|| default.to_string())
}

fn load(args: &[String]) -> Result<(Config, Secrets), String> {
    let cp = arg(args, "--config", "/config/config.toml");
    let sp = arg(args, "--secrets", "/run/secrets/sm/secrets.toml");
    let ct = std::fs::read_to_string(&cp).map_err(|e| format!("reading {cp}: {e}"))?;
    let st = std::fs::read_to_string(&sp).map_err(|e| format!("reading the secrets file: {e}"))?;
    let c = load_config(&ct).map_err(|e| format!("{cp}: {e}"))?;
    let s = load_secrets(&st)?;
    if c.twitch.enabled && s.twitch.is_none() {
        return Err("twitch is enabled but secrets.toml has no [twitch]".into());
    }
    if c.youtube.enabled && s.youtube.is_none() {
        return Err("youtube is enabled but secrets.toml has no [youtube]".into());
    }
    if c.streamlabs.enabled && s.streamlabs.is_none() {
        return Err("streamlabs is enabled but secrets.toml has no [streamlabs]".into());
    }
    if s.bridge_tokens.is_empty() {
        return Err("secrets.toml has no [[bridge_token]]: no watchdog could connect".into());
    }
    Ok((c, s))
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let r = match args.get(1).map(String::as_str) {
        Some("run") => run(&args).await,
        Some("check-config") => load(&args).map(|(c, s)| println!("config ok: {c:?}\nsecrets: {s:?}")),
        Some("gen-token") => {
            println!("{}", smsetup::gen_bridge_token());
            Ok(())
        }
        Some("setup") => setup(&args).await,
        Some("consent") => consent(&args),
        Some("health") => health(&args).await,
        _ => Err("usage: stream-manager run|check-config|gen-token|setup|consent|health [--config PATH] [--secrets PATH]".into()),
    };
    if let Err(e) = r {
        eprintln!("stream-manager: {e}");
        std::process::exit(1);
    }
}

fn wall_clock() -> Clock {
    Arc::new(|| SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0))
}

async fn run(args: &[String]) -> Result<(), String> {
    let (cfg, secrets) = load(args)?;
    let consent_path = Path::new(&cfg.server.data_dir).join("consent.json");
    if !consent_path.exists() {
        return Err("no consent record: the channel owner runs `stream-manager consent` first (plan §2)".into());
    }
    let token_dir = Path::new(&cfg.server.data_dir).join("tokens");
    std::fs::create_dir_all(&token_dir).map_err(|e| format!("token dir: {e}"))?;
    let clock = wall_clock();
    let now = clock();
    std::fs::create_dir_all(&cfg.server.data_dir).map_err(|e| format!("data dir: {e}"))?;
    let store = Store::open(&Path::new(&cfg.server.data_dir).join("state.db")).map_err(|e| e.to_string())?;
    let http = Arc::new(HttpClient::new());

    let bridge = Arc::new(Mutex::new(Bridge::new(cfg.bridge_config(secrets.all_values()), now)));
    // The chat disclosure goes out once per live session, across restarts (USER ma80a334356d2).
    let disclosure_path = Path::new(&cfg.server.data_dir).join("disclosure.json");
    if let Some(v) = std::fs::read(&disclosure_path).ok().and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok()) {
        bridge.lock().unwrap_or_else(|e| e.into_inner()).restore_disclosure(&v);
    }
    // The gate is kept across restarts within a live session (liaison me6d4d70ab422).
    let gate_path = Path::new(&cfg.server.data_dir).join("gate.json");
    if let Some(v) = std::fs::read(&gate_path).ok().and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok()) {
        bridge.lock().unwrap_or_else(|e| e.into_inner()).restore_gate(&v, now);
    }
    // YouTube's quota counter and ban ids (youtube-spec.md sections 5, 6): shared by the bridge, the
    // connector and the executor, kept across restarts through the store (a second connection).
    let yt_quota = Arc::new(YtQuota::with_costs(cfg.youtube.units_per_day, cfg.youtube.costs.clone()));
    let yt_bans = Arc::new(YtBans::default());
    let yt_store = Arc::new(Mutex::new(Store::open(&Path::new(&cfg.server.data_dir).join("state.db")).map_err(|e| e.to_string())?));
    if cfg.youtube.enabled {
        {
            let st = yt_store.lock().unwrap_or_else(|e| e.into_inner());
            if let Ok(Some((day, used))) = st.yt_quota_load() {
                yt_quota.restore(&day, used);
            }
            if let Ok(rows) = st.yt_bans_load() {
                yt_bans.import(rows);
            }
        }
        bridge.lock().unwrap_or_else(|e| e.into_inner()).set_youtube_state(yt_quota.clone(), yt_bans.clone());
    }
    // Whether Twitch is live: a rising edge starts the fast YouTube broadcast lookup window (section 4).
    let (tw_live_tx, tw_live_rx) = tokio::sync::watch::channel(false);
    let (act_tx, mut act_rx) = mpsc::unbounded_channel::<Action>();
    let handler = Arc::new(BridgeHandler::new(bridge.clone(), clock.clone(), act_tx.clone()));
    let hub = Hub::new(
        store,
        ServerConfig {
            tokens: secrets.token_table(),
            replay_max: cfg.server.replay_max,
            replay_window_s: cfg.server.replay_window_s,
            hello_timeout_s: 10.0,
        },
        handler.clone(),
        clock.clone(),
    );
    handler.set_publisher(hub.clone());

    // Every new token value joins the outbound filter (review round 1, finding 12).
    let sink: sm_bin::tokens::SecretSink = {
        let bridge = bridge.clone();
        Arc::new(move |v: &str| bridge.lock().unwrap_or_else(|e| e.into_inner()).add_secret(v))
    };
    // One token source per account, shared by everything that uses it (finding 8).
    let twitch_tokens = |refresh: String, file: &str| {
        let s = secrets.twitch.as_ref().expect("checked by load");
        Arc::new(
            TokenSource::new(
                TokenConfig {
                    token_url: cfg.twitch.token_url.clone(),
                    client_id: cfg.twitch.client_id.clone(),
                    client_secret: s.client_secret.clone(),
                    refresh_token: refresh,
                },
                http.clone(),
            )
            .with_store(token_dir.join(file))
            .with_secret_sink(sink.clone()),
        )
    };
    let (bot_tokens, bc_tokens) = match (&secrets.twitch, cfg.twitch.enabled) {
        (Some(s), true) => {
            let bot = twitch_tokens(s.refresh_token.clone(), "twitch-bot.json");
            let bc = s.broadcaster_refresh_token.clone().map(|r| twitch_tokens(r, "twitch-broadcaster.json"));
            (Some(bot), bc)
        }
        _ => (None, None),
    };
    // `broadcaster: true` uses the broadcaster's tokens (falling back to the bot's).
    let twitch_ctx = |_http: &Arc<HttpClient>, broadcaster: bool| -> Option<TwitchCtx> {
        let bot = bot_tokens.clone()?;
        let tokens = if broadcaster { bc_tokens.clone().unwrap_or(bot) } else { bot };
        Some(TwitchCtx {
            api_base: cfg.twitch.api_base.clone(),
            client_id: cfg.twitch.client_id.clone(),
            broadcaster_id: cfg.twitch.broadcaster_id.clone(),
            bot_user_id: cfg.twitch.bot_user_id.clone(),
            tokens,
        })
    };
    let yt_chat = Arc::new(Mutex::new(None::<(String, String)>));
    let yt_tokens = match (&secrets.youtube, cfg.youtube.enabled) {
        (Some(g), true) => Some(Arc::new(TokenSource::new(
            TokenConfig {
                token_url: cfg.youtube.token_url.clone(),
                client_id: g.client_id.clone(),
                client_secret: g.client_secret.clone(),
                refresh_token: g.refresh_token.clone(),
            },
            http.clone(),
        )
        .with_store(token_dir.join("youtube.json"))
        .with_park_on_invalid_grant()
        .with_secret_sink(sink.clone()))),
        _ => None,
    };
    let yt_ctx = || {
        yt_tokens.as_ref().map(|t| YtCtx {
            api_base: cfg.youtube.api_base.clone(),
            tokens: t.clone(),
            chat: yt_chat.clone(),
            bans: yt_bans.clone(),
            quota: Some(yt_quota.clone()),
        })
    };

    // Connectors.
    let (ev_tx, mut ev_rx) = mpsc::channel::<InEvent>(1024);
    if let Some(bot) = twitch_ctx(&http, false) {
        let all = default_specs(&cfg.twitch.broadcaster_id, &cfg.twitch.bot_user_id);
        let two_users = secrets.twitch.as_ref().is_some_and(|t| t.broadcaster_refresh_token.is_some());
        let session = |ctx: TwitchCtx, specs| {
            run_eventsub(
                EventSubConfig { url: cfg.twitch.eventsub_url.clone(), subscriptions: specs, keepalive_grace_s: 5.0 },
                Arc::new(HelixSubscriber { http: http.clone(), ctx }),
                ev_tx.clone(),
            )
        };
        if two_users {
            // Chat as the bot; bits, subs and stream state as the broadcaster (2 of 3 sessions).
            let (chat, rest): (Vec<_>, Vec<_>) = all.into_iter().partition(|s| s.sub_type == "channel.chat.message");
            tokio::spawn(session(bot, chat));
            if let Some(bc) = twitch_ctx(&http, true) {
                tokio::spawn(session(bc, rest));
            }
        } else {
            tokio::spawn(session(bot, all));
        }
    }
    if let Some(ctx) = yt_ctx() {
        let ycfg = cfg.youtube.to_yt_config();
        tokio::spawn(run_youtube_with(ycfg, Arc::new(YtHttpImpl { http: http.clone(), ctx }), ev_tx.clone(), Some(tw_live_rx)));
    }
    if let (true, Some(sl)) = (cfg.streamlabs.enabled, &secrets.streamlabs) {
        let url = format!("{}&token={}", cfg.streamlabs.socket_url, sm_bin_form(&sl.socket_token));
        tokio::spawn(run_streamlabs(
            StreamlabsConfig { url, native_twitch: cfg.twitch.enabled, native_youtube: cfg.youtube.enabled },
            ev_tx.clone(),
        ));
    }
    if cfg.orgtree_feed.enabled {
        let f = &cfg.orgtree_feed;
        tokio::spawn(sm_connectors::orgtree_feed::run_orgtree_feed(
            sm_connectors::orgtree_feed::OrgtreeFeedConfig { url: f.url.clone(), host: f.host.clone(), silence_s: 75.0, max_backoff_s: 30.0 },
            ev_tx.clone(),
        ));
    }
    // Startup liveness: a bridge started mid-stream learns that Twitch is already live
    // (review round 1, finding 3; YouTube is live once the poller finds the broadcast).
    if let Some(ctx) = twitch_ctx(&http, false) {
        let http = http.clone();
        let tx = ev_tx.clone();
        tokio::spawn(async move {
            let Ok(req) = sm_connectors::twitch::helix::get_stream(&ctx.broadcaster_id) else { return };
            if let Ok((status, body)) = sm_bin::exec::helix_send(&http, &ctx, &req).await {
                if let Ok(true) = sm_connectors::twitch::helix::parse_stream_live(status, &body) {
                    let _ = tx.send(InEvent::StreamState { platform: sm_core::protocol::Platform::Twitch, live: true }).await;
                }
            }
        });
    }
    drop(ev_tx);

    // Overlay page (OBS browser source) and /healthz.
    let overlay = OverlayHub::new();
    {
        let l = tokio::net::TcpListener::bind(&cfg.server.overlay_addr)
            .await
            .map_err(|e| format!("binding {}: {e}", cfg.server.overlay_addr))?;
        let app = overlay.router();
        tokio::spawn(async move {
            let _ = axum::serve(l, app).await;
        });
    }

    // Executor.
    let exec = Arc::new(Executor {
        http: http.clone(),
        twitch: twitch_ctx(&http, false),
        youtube: yt_ctx(),
        obs: cfg.obs.enabled.then(|| ObsCtx::new(cfg.obs.url.clone(), secrets.obs.as_ref().map(|o| o.password.clone()), cfg.effect_media_inputs())),
        overlay: Some(OverlayCtx { hub: overlay.clone(), animations: cfg.effect_overlays() }),
    });
    // Auto-sleep's read-only OBS startup check (plan rev 2): no usable sleep scene, no sleep.
    if cfg.sleep.enabled {
        let hub = hub.clone();
        let bridge = bridge.clone();
        let (url, pw, scene, min_sleep_s) = (cfg.obs.url.clone(), secrets.obs.as_ref().map(|o| o.password.clone()), cfg.sleep.scene.clone(), cfg.sleep.min_sleep_s);
        let obs_on = cfg.obs.enabled;
        tokio::spawn(async move {
            let facts = if obs_on { sm_bin::sleep_check::read_obs_sleep_facts(&url, pw.as_deref(), &scene).await } else { Err("OBS is not enabled".into()) };
            match facts {
                Err(e) => bridge.lock().unwrap_or_else(|e| e.into_inner()).disable_sleep("sleep_obs_unreadable", &format!("auto-sleep stays off: {e}"), hub.as_ref()),
                Ok(f) => {
                    let r = sm_bin::sleep_check::sleep_check(&scene, min_sleep_s, &f);
                    for (i, (code, detail)) in r.warnings.iter().enumerate() {
                        if i == 0 && !r.usable {
                            bridge.lock().unwrap_or_else(|e| e.into_inner()).disable_sleep(code, detail, hub.as_ref());
                        } else {
                            hub.publish("system.warning", json!({"code": code, "detail": detail}));
                        }
                    }
                }
            }
        });
    }
    {
        let hub = hub.clone();
        tokio::spawn(async move {
            while let Some(a) = act_rx.recv().await {
                if let Err(e) = exec.run(&a).await {
                    hub.publish("system.health", json!({"component": "executor", "status": "error", "action": action_name(&a), "detail": e}));
                }
            }
        });
    }

    // Events and ticks.
    {
        let hub = hub.clone();
        let bridge = bridge.clone();
        let clock = clock.clone();
        let stats_path = PathBuf::from(&cfg.server.stats_path);
        let disclosure_path = disclosure_path.clone();
        let mut disclosure_saved = bridge.lock().unwrap_or_else(|e| e.into_inner()).disclosure_record();
        let gate_path = gate_path.clone();
        let mut gate_saved = bridge.lock().unwrap_or_else(|e| e.into_inner()).gate_record();
        let retention_s = cfg.server.chat_retention_days * 86_400.0;
        let yt_chat = yt_chat.clone();
        let yt_enabled = cfg.youtube.enabled;
        let (yt_bans, yt_quota, yt_store) = (yt_bans.clone(), yt_quota.clone(), yt_store.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            let mut n: u64 = 0;
            loop {
                tokio::select! {
                    ev = ev_rx.recv() => {
                        let Some(ev) = ev else { break };
                        match &ev {
                            InEvent::BroadcastFound { broadcast_id, live_chat_id, .. } => {
                                *yt_chat.lock().unwrap_or_else(|e| e.into_inner()) = Some((broadcast_id.clone(), live_chat_id.clone()));
                            }
                            InEvent::StreamState { platform: sm_core::protocol::Platform::Youtube, live: false } => {
                                *yt_chat.lock().unwrap_or_else(|e| e.into_inner()) = None;
                            }
                            InEvent::StreamState { platform: sm_core::protocol::Platform::Twitch, live } => {
                                let _ = tw_live_tx.send(*live);
                            }
                            _ => {}
                        }
                        let acts = bridge.lock().unwrap_or_else(|e| e.into_inner()).on_event(clock(), ev, hub.as_ref());
                        for a in acts { let _ = act_tx.send(a); }
                    }
                    _ = tick.tick() => {
                        n += 1;
                        let now = clock();
                        let (snapshot, disclosure) = {
                            let mut b = bridge.lock().unwrap_or_else(|e| e.into_inner());
                            for a in b.tick(now, hub.as_ref()) { let _ = act_tx.send(a); }
                            ((n % 5 == 0).then(|| b.stats().snapshot_bytes(now)), b.disclosure_record())
                        };
                        let gate_now = bridge.lock().unwrap_or_else(|e| e.into_inner()).gate_record();
                        if gate_now != gate_saved && n % 5 == 0 {
                            let path = gate_path.clone();
                            let bytes = gate_now.to_string().into_bytes();
                            let r = tokio::task::spawn_blocking(move || sm_server::stats::write_bytes_atomic(&path, &bytes))
                                .await
                                .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())));
                            match r {
                                Ok(_) => gate_saved = gate_now,
                                Err(e) => { hub.publish("system.health", json!({"component": "gate_store", "status": "error", "detail": e.to_string()})); }
                            }
                        }
                        if disclosure != disclosure_saved && n % 5 == 0 {
                            let path = disclosure_path.clone();
                            let bytes = disclosure.to_string().into_bytes();
                            let r = tokio::task::spawn_blocking(move || sm_server::stats::write_bytes_atomic(&path, &bytes))
                                .await
                                .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())));
                            match r {
                                Ok(_) => disclosure_saved = disclosure,
                                Err(e) => { hub.publish("system.health", json!({"component": "disclosure", "status": "error", "detail": e.to_string()})); }
                            }
                        }
                        // Written outside the bridge lock, on a blocking thread (finding 13e).
                        if let Some(bytes) = snapshot {
                            let path = stats_path.clone();
                            let r = match bytes {
                                Ok(bytes) => tokio::task::spawn_blocking(move || sm_server::stats::write_bytes_atomic(&path, &bytes))
                                    .await
                                    .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string()))),
                                Err(e) => Err(e),
                            };
                            if let Err(e) = r {
                                hub.publish("system.health", json!({"component": "stats", "status": "error", "detail": e.to_string()}));
                            }
                        }
                        // YouTube's quota day and ban ids, kept across restarts (sections 5, 6).
                        if yt_enabled && n % 30 == 0 {
                            yt_bans.prune(now);
                            let rows = yt_bans.export();
                            let (day, used) = yt_quota.snapshot(now);
                            let store = yt_store.clone();
                            let r = tokio::task::spawn_blocking(move || {
                                let st = store.lock().unwrap_or_else(|e| e.into_inner());
                                st.yt_bans_replace(&rows).map_err(|e| e.to_string())?;
                                if !day.is_empty() {
                                    st.yt_quota_save(&day, used).map_err(|e| e.to_string())?;
                                }
                                Ok::<(), String>(())
                            })
                            .await
                            .unwrap_or_else(|e| Err(e.to_string()));
                            if let Err(e) = r {
                                hub.publish("system.health", json!({"component": "youtube_store", "status": "error", "detail": e}));
                            }
                        }
                        if n % 3600 == 0 {
                            hub.prune(retention_s);
                        }
                    }
                }
            }
        });
    }

    let listener = tokio::net::TcpListener::bind(&cfg.server.events_addr).await.map_err(|e| format!("binding {}: {e}", cfg.server.events_addr))?;
    eprintln!("stream-manager: listening on {}", cfg.server.events_addr);
    axum::serve(listener, hub.router())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| e.to_string())
}

/// Percent-encodes a query value (the Streamlabs token).
fn sm_bin_form(s: &str) -> String {
    s.bytes()
        .map(|b| if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') { (b as char).to_string() } else { format!("%{b:02X}") })
        .collect()
}

/// A short action name for health events (no chat text, no reasons).
fn action_name(a: &Action) -> &'static str {
    match a {
        Action::Chat { .. } => "chat",
        Action::Ban { .. } => "ban",
        Action::Unban { .. } => "unban",
        Action::Delete { .. } => "delete",
        Action::SlowMode { .. } => "slow_mode",
        Action::ObsTransition { .. } => "obs_transition",
        Action::ObsScene { .. } => "obs_scene",
        Action::ObsStop => "obs_stop",
        Action::YoutubeEnd { .. } => "youtube_end",
        Action::PlayEffect { .. } => "effect",
    }
}

/// `setup <which>`: a device-code sign-in; prints the secrets.toml line to add.
async fn setup(args: &[String]) -> Result<(), String> {
    let which = args.get(2).map(String::as_str).unwrap_or("");
    let cp = arg(args, "--config", "/config/config.toml");
    let cfg = load_config(&std::fs::read_to_string(&cp).map_err(|e| format!("reading {cp}: {e}"))?)?;
    let secrets = std::fs::read_to_string(arg(args, "--secrets", "/run/secrets/sm/secrets.toml"))
        .ok()
        .and_then(|t| load_secrets(&t).ok())
        .unwrap_or_default();
    let (flow, key, section) = match which {
        "twitch-bot" | "twitch-broadcaster" => {
            if cfg.twitch.client_id.is_empty() {
                return Err("set twitch.client_id in config.toml first".into());
            }
            let scopes = if which == "twitch-bot" { smsetup::TWITCH_BOT_SCOPES } else { smsetup::TWITCH_BROADCASTER_SCOPES };
            (
                DeviceFlow {
                    device_url: smsetup::TWITCH_DEVICE_URL.into(),
                    token_url: cfg.twitch.token_url.clone(),
                    client_id: cfg.twitch.client_id.clone(),
                    client_secret: secrets.twitch.as_ref().map(|t| t.client_secret.clone()),
                    scopes: scopes.iter().map(|s| s.to_string()).collect(),
                },
                if which == "twitch-bot" { "refresh_token" } else { "broadcaster_refresh_token" },
                "[twitch]",
            )
        }
        "youtube" => {
            let Some(g) = secrets.youtube.as_ref() else {
                return Err("put client_id and client_secret under [youtube] in secrets.toml first (refresh_token can be blank)".into());
            };
            (
                DeviceFlow {
                    device_url: smsetup::GOOGLE_DEVICE_URL.into(),
                    token_url: cfg.youtube.token_url.clone(),
                    client_id: g.client_id.clone(),
                    client_secret: Some(g.client_secret.clone()),
                    scopes: smsetup::YOUTUBE_SCOPES.iter().map(|s| s.to_string()).collect(),
                },
                "refresh_token",
                "[youtube]",
            )
        }
        _ => return Err("usage: stream-manager setup twitch-bot|twitch-broadcaster|youtube".into()),
    };
    let http = HttpClient::new();
    let start = flow.start(&http).await?;
    eprintln!("Open {} and enter the code {} (sign in as the {which} account).", start.verification_uri, start.user_code);
    let tokens = flow.poll(&http, &start).await?;
    eprintln!("Signed in. Add this line under {section} in secrets.toml (it is a secret):");
    println!("{key} = {:?}", tokens.refresh_token);
    Ok(())
}

/// `consent`: the channel owner types I AGREE to record consent to automated actions.
fn consent(args: &[String]) -> Result<(), String> {
    let cp = arg(args, "--config", "/config/config.toml");
    let cfg = load_config(&std::fs::read_to_string(&cp).map_err(|e| format!("reading {cp}: {e}"))?)?;
    eprintln!("As the owner of these channels, you allow stream-manager to do these without asking each time:");
    for item in smsetup::CONSENT_ITEMS {
        eprintln!("  - {item}");
    }
    eprintln!("Type I AGREE to record this:");
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|e| e.to_string())?;
    if line.trim() != "I AGREE" {
        return Err("consent not recorded".into());
    }
    let yt = if cfg.youtube.enabled { "enabled" } else { "disabled" };
    let channels = format!("twitch:{} youtube:{yt}", cfg.twitch.broadcaster_id);
    std::fs::create_dir_all(&cfg.server.data_dir).map_err(|e| e.to_string())?;
    smsetup::write_consent(&Path::new(&cfg.server.data_dir).join("consent.json"), &channels, wall_clock()()).map_err(|e| e.to_string())?;
    eprintln!("Consent recorded.");
    Ok(())
}

/// `health`: exit 0 when the configured overlay address answers /healthz (finding 13c).
async fn health(args: &[String]) -> Result<(), String> {
    let cp = arg(args, "--config", "/config/config.toml");
    let addr = std::fs::read_to_string(&cp)
        .ok()
        .and_then(|t| load_config(&t).ok())
        .map(|c| c.server.overlay_addr)
        .unwrap_or_else(|| "127.0.0.1:8788".into());
    let port = addr.rsplit(':').next().unwrap_or("8788");
    let host = match addr.rsplit_once(':').map(|x| x.0) {
        Some("0.0.0.0") | Some("") | None => "127.0.0.1",
        Some(h) => h,
    };
    let http = HttpClient::new();
    let (status, body) = http
        .send(sm_bin::http::HttpReq { method: "GET", url: format!("http://{host}:{port}/healthz"), headers: vec![], body: None })
        .await?;
    if status == 200 && body == "ok" {
        Ok(())
    } else {
        Err(format!("unhealthy: HTTP {status}"))
    }
}
