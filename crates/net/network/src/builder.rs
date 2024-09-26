//! Builder support for configuring the entire setup.

use reth_eth_wire::ClayerConsensusMessageAgentTrait;
use reth_network_api::test_utils::PeersHandleProvider;
use reth_transaction_pool::TransactionPool;
use tokio::sync::mpsc;

use crate::{
    consensus::NetworkClayerManager,
    eth_requests::EthRequestHandler,
    transactions::{TransactionsManager, TransactionsManagerConfig},
    NetworkHandle, NetworkManager,
};

/// We set the max channel capacity of the `EthRequestHandler` to 256
/// 256 requests with malicious 10MB body requests is 2.6GB which can be absorbed by the node.
pub(crate) const ETH_REQUEST_CHANNEL_CAPACITY: usize = 256;

/// A builder that can configure all components of the network.
#[allow(missing_debug_implementations)]
pub struct NetworkBuilder<Tx, Eth, Cm> {
    pub(crate) network: NetworkManager,
    pub(crate) transactions: Tx,
    pub(crate) request_handler: Eth,
    pub(crate) consensus_manager: Cm,
}

// === impl NetworkBuilder ===

impl<Tx, Eth, Cm> NetworkBuilder<Tx, Eth, Cm> {
    /// Consumes the type and returns all fields.
    pub fn split(self) -> (NetworkManager, Tx, Eth, Cm) {
        let Self { network, transactions, request_handler, consensus_manager } = self;
        (network, transactions, request_handler, consensus_manager)
    }

    /// Returns the network manager.
    pub const fn network(&self) -> &NetworkManager {
        &self.network
    }

    /// Returns the mutable network manager.
    pub fn network_mut(&mut self) -> &mut NetworkManager {
        &mut self.network
    }

    /// Returns the handle to the network.
    pub fn handle(&self) -> NetworkHandle {
        self.network.handle().clone()
    }

    /// Consumes the type and returns all fields and also return a [`NetworkHandle`].
    pub fn split_with_handle(self) -> (NetworkHandle, NetworkManager, Tx, Eth, Cm) {
        let Self { network, transactions, request_handler, consensus_manager } = self;
        let handle = network.handle().clone();
        (handle, network, transactions, request_handler, consensus_manager)
    }

    /// Creates a new [`TransactionsManager`] and wires it to the network.
    pub fn transactions<Pool: TransactionPool>(
        self,
        pool: Pool,
        transactions_manager_config: TransactionsManagerConfig,
    ) -> NetworkBuilder<TransactionsManager<Pool>, Eth, Cm> {
        let Self { mut network, request_handler, consensus_manager, .. } = self;
        let (tx, rx) = mpsc::unbounded_channel();
        network.set_transactions(tx);
        let handle = network.handle().clone();
        let transactions = TransactionsManager::new(handle, pool, rx, transactions_manager_config);
        NetworkBuilder { network, request_handler, transactions, consensus_manager }
    }

    /// Creates a new [`EthRequestHandler`] and wires it to the network.
    pub fn request_handler<Client>(
        self,
        client: Client,
    ) -> NetworkBuilder<Tx, EthRequestHandler<Client>, Cm> {
        let Self { mut network, transactions, consensus_manager, .. } = self;
        let (tx, rx) = mpsc::channel(ETH_REQUEST_CHANNEL_CAPACITY);
        network.set_eth_request_handler(tx);
        let peers = network.handle().peers_handle().clone();
        let request_handler = EthRequestHandler::new(client, peers, rx);
        NetworkBuilder { network, request_handler, transactions, consensus_manager }
    }

    /// Creates a new [`NetworkClayerManager`] and wires it to the network.
    pub fn consensus<ConsensusAgent: ClayerConsensusMessageAgentTrait>(
        self,
        consensus_agent: ConsensusAgent,
    ) -> NetworkBuilder<Tx, Eth, NetworkClayerManager<ConsensusAgent>> {
        let NetworkBuilder { mut network, request_handler, transactions, .. } = self;
        let (tx, rx) = mpsc::unbounded_channel();
        network.set_consensus(tx);
        let handle = network.handle().clone();
        let consensus_manager = NetworkClayerManager::new(handle, consensus_agent, rx);
        NetworkBuilder { network, request_handler, transactions, consensus_manager }
    }
}
