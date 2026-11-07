//! The server's command handler: runs agent commands through the bridge and queues the
//! resulting platform actions for the executor. Lock order everywhere: bridge, then the
//! hub's store (the hub never takes the bridge lock).

use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;
use sm_bridge::{Action, Bridge, Publisher};
use sm_core::protocol::Command;
use sm_server::{Clock, CommandHandler};
use tokio::sync::mpsc::UnboundedSender;

pub type SharedPublisher = Arc<dyn Publisher + Send + Sync>;

pub struct BridgeHandler {
    bridge: Arc<Mutex<Bridge>>,
    clock: Clock,
    actions: UnboundedSender<Action>,
    publisher: OnceLock<SharedPublisher>,
}

impl BridgeHandler {
    pub fn new(bridge: Arc<Mutex<Bridge>>, clock: Clock, actions: UnboundedSender<Action>) -> Self {
        BridgeHandler { bridge, clock, actions, publisher: OnceLock::new() }
    }

    /// The hub is built with this handler, so the publisher is set right after.
    pub fn set_publisher(&self, p: SharedPublisher) {
        let _ = self.publisher.set(p);
    }
}

impl CommandHandler for BridgeHandler {
    fn handle(&self, actor: &str, cmd: &Command) -> Result<Value, (String, String)> {
        let Some(p) = self.publisher.get() else {
            return Err(("not_ready".into(), "the stream manager is still starting".into()));
        };
        let now = (self.clock)();
        let (r, acts) = self.bridge.lock().unwrap_or_else(|e| e.into_inner()).on_command(now, actor, cmd, p.as_ref());
        for a in acts {
            let _ = self.actions.send(a);
        }
        r
    }
}
