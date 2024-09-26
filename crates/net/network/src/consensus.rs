//! Consensus management for the p2p network.

use crate::{
    budget::{
        DEFAULT_BUDGET_TRY_DRAIN_NETWORK_CONSENSUS_EVENTS,
        DEFAULT_BUDGET_TRY_DRAIN_PENDING_CONSENSUS_IMPORTS, DEFAULT_BUDGET_TRY_DRAIN_STREAM,
    },
    metered_poll_nested_stream_with_budget, NetworkHandle,
};
use futures::{Future, StreamExt};
use reth_eth_wire::{ClayerConsensusMessageAgentTrait, ClayerConsensusMsg, EthVersion};
use reth_network_api::{NetworkEvent, NetworkEventListenerProvider, PeerRequestSender};
use reth_network_peers::PeerId;
use reth_tokio_util::EventStream;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

/// Manages consensus on top of the p2p network.
#[derive(Debug)]
pub struct NetworkClayerManager<ConsensusAgent> {
    /// Consensus layer.
    clayer: ConsensusAgent,
    /// Network access.
    network: NetworkHandle,
    /// From which we get all new incoming transaction related messages.
    network_events: EventStream<NetworkEvent>,
    /// All the connected peers.
    peers: HashMap<PeerId, ConsensusPeer>,
    /// Incoming events from the [`NetworkManager`](crate::NetworkManager).
    consensus_events: UnboundedReceiverStream<NetworkConsensusEvent>,
    /// Incoming commands from [`ConsensussHandle`].
    pending_consensuses: ReceiverStream<(Vec<PeerId>, reth_primitives::Bytes)>,
}

impl<ConsensusAgent: ClayerConsensusMessageAgentTrait> NetworkClayerManager<ConsensusAgent> {
    /// Sets up a new instance.
    ///
    /// Note: This expects an existing [`NetworkManager`](crate::NetworkManager) instance.
    pub fn new(
        network: NetworkHandle,
        clayer: ConsensusAgent,
        from_network: mpsc::UnboundedReceiver<NetworkConsensusEvent>,
    ) -> Self {
        let network_events = network.event_listener();

        // install a listener for new pending consensus that are allowed to be propagated over
        // the network
        let pending = clayer.outgoing_consensus_channel();

        Self {
            clayer,
            network,
            network_events,
            peers: Default::default(),
            consensus_events: UnboundedReceiverStream::new(from_network),
            pending_consensuses: ReceiverStream::new(pending),
        }
    }
}

impl<ConsensusAgent> NetworkClayerManager<ConsensusAgent>
where
    ConsensusAgent: ClayerConsensusMessageAgentTrait + 'static,
{
    fn on_network_event(&mut self, event: NetworkEvent) {
        match event {
            NetworkEvent::SessionClosed { peer_id, .. } => {
                // remove the peer
                self.peers.remove(&peer_id);
                self.clayer.push_network_event(peer_id, false);
            }
            NetworkEvent::SessionEstablished {
                peer_id, client_version, messages, version, ..
            } => {
                // insert a new peer into the peerset
                self.peers.insert(
                    peer_id,
                    ConsensusPeer { request_tx: messages, version, client_version },
                );
                self.clayer.push_network_event(peer_id, true);
            }
            _ => {}
        }
    }

    fn on_network_consensus_event(&mut self, event: NetworkConsensusEvent) {
        match event {
            NetworkConsensusEvent::IncomingConsensus { peer_id, msg } => {
                self.clayer.push_incoming_msg(peer_id, msg.0.clone());
            }
        }
    }

    fn propagate_consensus(&mut self, peers: Vec<PeerId>, data: reth_primitives::Bytes) {
        for (_, (peer_id, _peer)) in self.peers.iter_mut().enumerate() {
            if peers.is_empty() {
                self.network.send_consensus(*peer_id, data.clone());
            } else {
                if peers.contains(peer_id) {
                    self.network.send_consensus(*peer_id, data.clone());
                }
            }
        }
    }
}

/// An endless future.
///
/// This should be spawned or used as part of `tokio::select!`.
impl<ConsensusAgent> Future for NetworkClayerManager<ConsensusAgent>
where
    ConsensusAgent: ClayerConsensusMessageAgentTrait + Unpin + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut consensus_durations = ConsensusManagerPollDurations::default();

        let maybe_more_network_events = metered_poll_nested_stream_with_budget!(
            consensus_durations.acc_network_events,
            "net::consensus",
            "Network events stream",
            DEFAULT_BUDGET_TRY_DRAIN_STREAM,
            this.network_events.poll_next_unpin(cx),
            |event| this.on_network_event(event)
        );

        let maybe_more_consensus_events = metered_poll_nested_stream_with_budget!(
            consensus_durations.acc_consensus_events,
            "net::consensus",
            "Network transaction events stream",
            DEFAULT_BUDGET_TRY_DRAIN_NETWORK_CONSENSUS_EVENTS,
            this.consensus_events.poll_next_unpin(cx),
            |event| this.on_network_consensus_event(event),
        );

        let maybe_more_pending_consensuses = metered_poll_nested_stream_with_budget!(
            consensus_durations.acc_pending_imports,
            "net::consensus",
            "Pending consensuses stream",
            DEFAULT_BUDGET_TRY_DRAIN_PENDING_CONSENSUS_IMPORTS,
            this.pending_consensuses.poll_next_unpin(cx),
            |(peers, data)| this.propagate_consensus(peers, data)
        );

        if maybe_more_network_events
            || maybe_more_consensus_events
            || maybe_more_pending_consensuses
        {
            // make sure we're woken up again
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        Poll::Pending
    }
}

/// All events related to cconsensus emitted by the network.
#[derive(Debug)]
#[allow(missing_docs)]
pub enum NetworkConsensusEvent {
    /// Received list of cconsensus from the given peer.
    ///
    /// This represents cconsensus that were broadcasted to use from the peer.
    IncomingConsensus { peer_id: PeerId, msg: ClayerConsensusMsg },
}

/// Tracks a single peer
#[derive(Debug)]
struct ConsensusPeer {
    /// A communication channel directly to the peer's session task.
    #[allow(unused)]
    request_tx: PeerRequestSender,
    /// negotiated version of the session.
    #[allow(unused)]
    version: EthVersion,
    /// The peer's client version.
    #[allow(unused)]
    client_version: Arc<str>,
}

#[derive(Debug, Default)]
struct ConsensusManagerPollDurations {
    acc_network_events: Duration,
    acc_pending_imports: Duration,
    acc_consensus_events: Duration,
}
