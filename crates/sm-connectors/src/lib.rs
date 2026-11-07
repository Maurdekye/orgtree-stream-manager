//! Stream manager connectors (plan §3). Each speaks one outside protocol and turns it
//! into [`model::InEvent`]s, or turns commands into requests. Phase 2: Twitch EventSub
//! and Helix request shapes, and obs-websocket v5.
//!
//! These are small clients of our own, not the `twitch_api`/`obws` crates of plan §17:
//! those weren't in the offline cargo cache, and fetching them would be outside contact.

pub mod model;
pub mod obs;
pub mod orgtree_feed;
pub mod streamlabs;
pub mod twitch;
pub mod youtube;
pub mod ytstate;
