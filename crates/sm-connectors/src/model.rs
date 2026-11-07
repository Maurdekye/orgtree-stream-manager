//! What connectors report. Every string here came from outside and is untrusted.

use sm_core::donation::Source;
use sm_core::protocol::Platform;

#[derive(Debug, Clone, PartialEq)]
pub enum InEvent {
    Chat {
        platform: Platform,
        /// `twitch:<user id>` / `youtube:<channel id>`.
        viewer_id: String,
        login: String,
        display: String,
        text: String,
        /// Platform-prefixed message id (`tw:…`), used by `mod.delete`.
        msg_id: String,
        roles: Vec<String>,
        /// A paid subscriber or channel member (credit floor F, §6.1).
        paid_sub: bool,
    },
    Donation {
        /// None for a donation service's own donations (Streamlabs), which belong to
        /// no platform.
        platform: Option<Platform>,
        /// None for anonymous gifts or unlinked donors.
        viewer_id: Option<String>,
        display: Option<String>,
        source: Source,
        text: Option<String>,
        /// Unique per delivery, for de-duplication in the log.
        external_id: String,
    },
    StreamState {
        platform: Platform,
        live: bool,
    },
    /// A live broadcast and its chat were found (YouTube); actions need these ids.
    BroadcastFound {
        platform: Platform,
        broadcast_id: String,
        live_chat_id: String,
        /// The channel's own id: its messages posted through the API come back as chat
        /// and must be ignored (review round 1, finding 4).
        channel_id: Option<String>,
    },
    Revoked {
        sub_type: String,
        status: String,
    },
    /// The Orgtree live feed's turn state (`orgtree_feed`).
    OrgTurns(sm_core::sleep::TurnsReport),
    Health {
        component: &'static str,
        up: bool,
        detail: String,
    },
}
