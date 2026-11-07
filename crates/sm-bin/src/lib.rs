//! The stream-manager program's pieces (plan §2, §17): configuration, secrets, the HTTPS
//! client, OAuth token refresh, platform executors and the command-handler adapter.
//! `main.rs` wires them into the run loop.

pub mod config;
pub mod exec;
pub mod handler;
pub mod http;
pub mod overlay;
pub mod secrets;
pub mod setup;
pub mod sleep_check;
pub mod tokens;
