use super::ProviderNodeTypes;
use crate::{ConsensusNumberReader, ConsensusNumberWriter, ProviderFactory};
use reth_db::models::consensus::ConsensusBytes;
use reth_node_types::NodeTypesWithDB;
use reth_primitives::{BlockNumber, B256};
use reth_storage_errors::provider::ProviderResult;
/// The main type for interacting with the blockchain.
///
/// This type serves as the main entry point for interacting with the blockchain and provides data
/// from database storage and from the blockchain tree (pending state etc.) It is a simple wrapper
/// type that holds an instance of the database and the blockchain tree.
#[derive(Clone, Debug)]
pub struct ConsensusProvider<DB: NodeTypesWithDB> {
    /// Provider type used to access the database.
    database: ProviderFactory<DB>,
}

impl<DB: ProviderNodeTypes> ConsensusProvider<DB> {
    /// Create a new provider using only the database and the tree, fetching the latest header from
    /// the database to initialize the provider.
    pub fn new(database: ProviderFactory<DB>) -> ProviderResult<Self> {
        Ok(Self { database })
    }
}

impl<DB: ProviderNodeTypes> ConsensusNumberReader for ConsensusProvider<DB> {
    /// Returns the best block number in the chain.
    fn last_consensus_number(&self) -> ProviderResult<BlockNumber> {
        self.database.provider()?.last_consensus_number()
    }

    /// Gets the `BlockNumber` for the given hash. Returns `None` if no block with this hash exists.
    fn consensus_number(&self, hash: B256) -> ProviderResult<Option<BlockNumber>> {
        self.database.provider()?.consensus_number(hash)
    }

    /// Gets the `BlockNumber` for the given hash. Returns `None` if no block with this hash exists.
    fn consensus_content(&self, hash: B256) -> ProviderResult<Option<ConsensusBytes>> {
        self.database.provider()?.consensus_content(hash)
    }
}

impl<DB: ProviderNodeTypes> ConsensusNumberWriter for ConsensusProvider<DB> {
    fn save_consensus_number(&self, hash: B256, num: BlockNumber) -> ProviderResult<bool> {
        let provider = self.database.provider_rw()?;
        provider.save_consensus_number(hash, num)?;
        provider.commit()
    }

    fn save_consensus_content(&self, hash: B256, ct: ConsensusBytes) -> ProviderResult<bool> {
        let provider = self.database.provider_rw()?;
        provider.save_consensus_content(hash, ct)?;
        provider.commit()
    }
}
