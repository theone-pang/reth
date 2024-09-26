//! Implementation of consensus layer messages

use alloy_rlp::{RlpDecodableWrapper, RlpEncodableWrapper};
use reth_codecs_derive::add_arbitrary_tests;
use reth_network_peers::PeerId;
use reth_primitives::{Bytes, B256};
use std::fmt::Debug;
use tokio::sync::mpsc::Receiver;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Consensus layer message

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct ClayerConsensusMsg(pub Bytes);

/// Consensus layer event
#[derive(Debug)]
pub enum ClayerConsensusEvent {
    /// Peer connected or disconnected
    PeerNetWork(PeerId, bool),
    /// Consensus message
    PeerMessage(PeerId, reth_primitives::Bytes),
    /// Consensus OnBlockValid
    BlockValid(B256),
    /// Consensus OnBlockInvalid
    BlockInvalid(B256),
    /// Consensus OnBlockCommit
    BlockCommit((B256, bool)),
}

/// Consensus layer message agent interface
#[async_trait::async_trait]
// #[auto_impl::auto_impl(Arc)]
pub trait ClayerConsensusMessageAgentTrait: Send + Sync + Clone {
    /// Returns outgoing consensus listener
    fn outgoing_consensus_channel(&self) -> Receiver<(Vec<PeerId>, reth_primitives::Bytes)>;
    /// Returns incoming consensus listener
    fn incoming_consensus_channel(&self) -> Receiver<ClayerConsensusEvent>;

    /// push data received from network into agent
    fn push_incoming_msg(&self, peer_id: PeerId, data: reth_primitives::Bytes);
    /// push network event
    fn push_network_event(&self, peer_id: PeerId, connected: bool);

    /// broadcast consensus
    fn broadcast_consensus(&self, peers: Vec<PeerId>, data: reth_primitives::Bytes);
    /// push block event
    fn push_block_event(&self, event: ClayerConsensusEvent);
    /// get all peers
    fn peers(&self) -> Vec<PeerId>;
}
