use parking_lot::RwLock;
use reth_eth_wire::{ClayerConsensusEvent, ClayerConsensusMessageAgentTrait};
use reth_network_peers::PeerId;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc::{Receiver, Sender};

use lazy_static::lazy_static;
lazy_static! {
    pub static ref ClayerConsensusMessageAgentRef: ClayerConsensusMessageAgent =
        ClayerConsensusMessageAgent::default();
}

/// Consensus layer message agent
pub struct ClayerConsensusMessageAgentInner {
    /// Consensus layer message outgoing sender
    pub outgoing_sender: Option<Sender<(Vec<PeerId>, reth_primitives::Bytes)>>,
    /// Consensus layer message incoming sender
    pub incoming_sender: Option<Sender<ClayerConsensusEvent>>,
    /// Connect peers
    active_peers: HashSet<PeerId>,
}

impl Default for ClayerConsensusMessageAgentInner {
    fn default() -> Self {
        Self { outgoing_sender: None, incoming_sender: None, active_peers: HashSet::new() }
    }
}
/// Consensus layer message agent
#[derive(Clone)]
pub struct ClayerConsensusMessageAgent {
    /// Consensus layer message agent inner
    pub inner: std::sync::Arc<RwLock<ClayerConsensusMessageAgentInner>>,
}

impl std::fmt::Debug for ClayerConsensusMessageAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClayerConsensusMessageAgent").finish()
    }
}

impl Default for ClayerConsensusMessageAgent {
    fn default() -> Self {
        Self { inner: Arc::new(Default::default()) }
    }
}

impl ClayerConsensusMessageAgentTrait for ClayerConsensusMessageAgent {
    /// Returns outgoing consensus listener
    fn outgoing_consensus_channel(&self) -> Receiver<(Vec<PeerId>, reth_primitives::Bytes)> {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        self.inner.write().outgoing_sender = Some(tx);
        rx
    }
    /// Returns incoming consensus listener
    fn incoming_consensus_channel(&self) -> Receiver<ClayerConsensusEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        self.inner.write().incoming_sender = Some(tx);
        rx
    }
    /// push data received from network into agent
    fn push_incoming_msg(&self, peer_id: PeerId, data: reth_primitives::Bytes) {
        if let Some(sender) = self.inner.read().incoming_sender.as_ref() {
            match sender.try_send(ClayerConsensusEvent::PeerMessage(peer_id, data)) {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("agent push incoming message error:{}", e);
                }
            }
        }
    }
    /// push network event
    fn push_network_event(&self, peer_id: PeerId, connected: bool) {
        if let Some(sender) = self.inner.read().incoming_sender.as_ref() {
            match sender.try_send(ClayerConsensusEvent::PeerNetWork(peer_id, connected)) {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("agent push network event error:{}", e);
                }
            }
        }
        if connected {
            self.inner.write().active_peers.insert(peer_id);
        } else {
            self.inner.write().active_peers.remove(&peer_id);
        }
    }

    /// broadcast consensus
    fn broadcast_consensus(&self, peers: Vec<PeerId>, data: reth_primitives::Bytes) {
        if let Some(sender) = &self.inner.read().outgoing_sender {
            match sender.try_send((peers, data)) {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("agent broadcast consensus message error:{}", e);
                }
            }
        }
    }

    /// push block event
    fn push_block_event(&self, event: ClayerConsensusEvent) {
        if let Some(sender) = self.inner.read().incoming_sender.as_ref() {
            match sender.try_send(event) {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("agent push block commit error:{}", e);
                }
            }
        }
    }

    /// get all peers
    fn peers(&self) -> Vec<PeerId> {
        self.inner.read().active_peers.iter().copied().collect()
    }
}
