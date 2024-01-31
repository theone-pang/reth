//! Main node command
//!
//! Starts the client
use crate::{
    args::{
        get_secret_key,
        utils::{chain_help, genesis_value_parser, parse_socket_address, SUPPORTED_CHAINS},
        ClayerArgs, DatabaseArgs, DebugArgs, DevArgs, NetworkArgs, PayloadBuilderArgs, PruningArgs,
        RpcServerArgs, TxPoolArgs,
    },
    cli::{
        components::RethNodeComponentsImpl,
        config::{RethRpcConfig, RethTransactionPoolConfig},
        ext::{RethCliExt, RethNodeCommandConfig},
    },
    dirs::{ChainPath, DataDirPath, MaybePlatformPath},
    init::init_genesis,
    node::cl_events::ConsensusLayerHealthEvents,
    prometheus_exporter,
    runner::CliContext,
    utils::get_single_header,
    version::SHORT_VERSION,
};
use clap::{value_parser, Parser};
use eyre::Context;
use futures::{future::Either, pin_mut, stream, stream_select, StreamExt};
use metrics_exporter_prometheus::PrometheusHandle;
use reth_auto_seal_consensus::{AutoSealBuilder, AutoSealConsensus, MiningMode};
use reth_beacon_consensus::{
    hooks::{EngineHooks, PruneHook},
    BeaconConsensus, BeaconConsensusEngine, MIN_BLOCKS_FOR_PIPELINE_RUN,
};
use reth_blockchain_tree::{
    config::BlockchainTreeConfig, externals::TreeExternals, BlockchainTree, ShareableBlockchainTree,
};
use reth_clayer::{ClayerConsensusMessagingAgent, ConsensusBuilder};
use reth_config::{
    config::{PruneConfig, StageConfig},
    Config,
};
use reth_db::{database::Database, database_metrics::DatabaseMetrics, init_db};
use reth_downloaders::{
    bodies::bodies::BodiesDownloaderBuilder,
    headers::reverse_headers::ReverseHeadersDownloaderBuilder,
};
use reth_interfaces::{
    clayer::ClayerConsensusMessageAgentTrait,
    consensus::Consensus,
    p2p::{
        bodies::{client::BodiesClient, downloader::BodyDownloader},
        either::EitherDownloader,
        headers::{client::HeadersClient, downloader::HeaderDownloader},
    },
    RethResult,
};
use reth_network::{NetworkBuilder, NetworkConfig, NetworkEvents, NetworkHandle, NetworkManager};
use reth_network_api::{NetworkInfo, PeersInfo};
use reth_primitives::{
    constants::eip4844::{LoadKzgSettingsError, MAINNET_KZG_TRUSTED_SETUP},
    fs,
    hex::encode,
    kzg::KzgSettings,
    stage::StageId,
    BlockHashOrNumber, BlockNumber, ChainSpec, DisplayHardforks, Head, SealedHeader, B256,
};
use reth_provider::{
    providers::BlockchainProvider, providers::ConsensusProvider, BlockHashReader, BlockReader,
    CanonStateSubscriptions, HeaderProvider, HeaderSyncMode, ProviderFactory,
    StageCheckpointReader,
};
use reth_prune::PrunerBuilder;
use reth_revm::EvmProcessorFactory;
use reth_revm_inspectors::stack::Hook;
use reth_rpc_engine_api::EngineApi;
use reth_stages::{
    prelude::*,
    stages::{
        AccountHashingStage, ExecutionStage, ExecutionStageThresholds, IndexAccountHistoryStage,
        IndexStorageHistoryStage, MerkleStage, SenderRecoveryStage, StorageHashingStage,
        TotalDifficultyStage, TransactionLookupStage,
    },
};
use reth_tasks::TaskExecutor;
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, TransactionPool, TransactionValidationTaskExecutor,
};
use secp256k1::SecretKey;
use std::{
    net::{SocketAddr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{mpsc::unbounded_channel, oneshot, watch};
use tracing::*;

pub mod cl_events;
pub mod events;

/// Start the node
#[derive(Debug, Parser)]
pub struct NodeCommand<Ext: RethCliExt = ()> {
    /// The path to the data dir for all reth files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/reth/` or `$HOME/.local/share/reth/`
    /// - Windows: `{FOLDERID_RoamingAppData}/reth/`
    /// - macOS: `$HOME/Library/Application Support/reth/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// The path to the configuration file to use.
    #[arg(long, value_name = "FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        long_help = chain_help(),
        default_value = SUPPORTED_CHAINS[0],
        default_value_if("dev", "true", "dev"),
        value_parser = genesis_value_parser,
        required = false,
    )]
    pub chain: Arc<ChainSpec>,

    /// Enable Prometheus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    #[arg(long, value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Metrics")]
    pub metrics: Option<SocketAddr>,

    /// Add a new instance of a node.
    ///
    /// Configures the ports of the node to avoid conflicts with the defaults.
    /// This is useful for running multiple nodes on the same machine.
    ///
    /// Max number of instances is 200. It is chosen in a way so that it's not possible to have
    /// port numbers that conflict with each other.
    ///
    /// Changes to the following port numbers:
    /// - DISCOVERY_PORT: default + `instance` - 1
    /// - AUTH_PORT: default + `instance` * 100 - 100
    /// - HTTP_RPC_PORT: default - `instance` + 1
    /// - WS_RPC_PORT: default + `instance` * 2 - 2
    #[arg(long, value_name = "INSTANCE", global = true, default_value_t = 1, value_parser = value_parser!(u16).range(..=200))]
    pub instance: u16,

    /// Overrides the KZG trusted setup by reading from the supplied file.
    #[arg(long, value_name = "PATH")]
    pub trusted_setup_file: Option<PathBuf>,

    /// All networking related arguments
    #[clap(flatten)]
    pub network: NetworkArgs,

    /// All rpc related arguments
    #[clap(flatten)]
    pub rpc: RpcServerArgs,

    /// All txpool related arguments with --txpool prefix
    #[clap(flatten)]
    pub txpool: TxPoolArgs,

    /// All payload builder related arguments
    #[clap(flatten)]
    pub builder: PayloadBuilderArgs,

    /// All debug related arguments with --debug prefix
    #[clap(flatten)]
    pub debug: DebugArgs,

    /// All database related arguments
    #[clap(flatten)]
    pub db: DatabaseArgs,

    /// All dev related arguments with --dev prefix
    #[clap(flatten)]
    pub dev: DevArgs,

    /// All pruning related arguments
    #[clap(flatten)]
    pub pruning: PruningArgs,

    /// All clayer related arguments
    #[clap(flatten)]
    pub clayer: ClayerArgs,

    /// Rollup related arguments
    #[cfg(feature = "optimism")]
    #[clap(flatten)]
    pub rollup: crate::args::RollupArgs,

    /// Additional cli arguments
    #[clap(flatten)]
    #[clap(next_help_heading = "Extension")]
    pub ext: Ext::Node,
}

impl<Ext: RethCliExt> NodeCommand<Ext> {
    /// Replaces the extension of the node command
    pub fn with_ext<E: RethCliExt>(self, ext: E::Node) -> NodeCommand<E> {
        let Self {
            datadir,
            config,
            chain,
            metrics,
            trusted_setup_file,
            instance,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            clayer,
            #[cfg(feature = "optimism")]
            rollup,
            ..
        } = self;
        NodeCommand {
            datadir,
            config,
            chain,
            metrics,
            instance,
            trusted_setup_file,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            clayer,
            #[cfg(feature = "optimism")]
            rollup,
            ext,
        }
    }

    /// Execute `node` command
    pub async fn execute(mut self, ctx: CliContext) -> eyre::Result<()> {
        info!(target: "reth::cli", "reth {} starting", SHORT_VERSION);

        // Raise the fd limit of the process.
        // Does not do anything on windows.
        let _ = fdlimit::raise_fd_limit();

        // get config
        let config = self.load_config()?;

        let prometheus_handle = self.install_prometheus_recorder()?;

        let data_dir = self.data_dir();
        let db_path = data_dir.db_path();

        info!(target: "reth::cli", path = ?db_path, "Opening database");
        let db = Arc::new(init_db(&db_path, self.db.log_level)?.with_metrics());
        info!(target: "reth::cli", "Database opened");

        let mut provider_factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&self.chain));

        // configure snapshotter
        let snapshotter = reth_snapshot::Snapshotter::new(
            provider_factory.clone(),
            data_dir.snapshots_path(),
            self.chain.snapshot_block_interval,
        )?;

        provider_factory = provider_factory
            .with_snapshots(data_dir.snapshots_path(), snapshotter.highest_snapshot_receiver())?;

        self.start_metrics_endpoint(prometheus_handle, Arc::clone(&db)).await?;

        debug!(target: "reth::cli", chain=%self.chain.chain, genesis=?self.chain.genesis_hash(), "Initializing genesis");

        let genesis_hash = init_genesis(Arc::clone(&db), self.chain.clone())?;

        info!(target: "reth::cli", "{}", DisplayHardforks::new(self.chain.hardforks()));

        let consensus = self.consensus();

        debug!(target: "reth::cli", "Spawning stages metrics listener task");
        let (sync_metrics_tx, sync_metrics_rx) = unbounded_channel();
        let sync_metrics_listener = reth_stages::MetricsListener::new(sync_metrics_rx);
        ctx.task_executor.spawn_critical("stages metrics listener task", sync_metrics_listener);

        let prune_config =
            self.pruning.prune_config(Arc::clone(&self.chain))?.or(config.prune.clone());

        // configure blockchain tree
        let tree_externals = TreeExternals::new(
            provider_factory.clone(),
            Arc::clone(&consensus),
            EvmProcessorFactory::new(self.chain.clone()),
        );
        let tree_config = BlockchainTreeConfig::default();
        let tree = BlockchainTree::new(
            tree_externals,
            tree_config,
            prune_config.clone().map(|config| config.segments),
        )?
        .with_sync_metrics_tx(sync_metrics_tx.clone());
        let canon_state_notification_sender = tree.canon_state_notification_sender();
        let blockchain_tree = ShareableBlockchainTree::new(tree);
        debug!(target: "reth::cli", "configured blockchain tree");

        // fetch the head block from the database
        let head =
            self.lookup_head(provider_factory.clone()).wrap_err("the head block is missing")?;

        // setup the blockchain provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())?;
        let blob_store = InMemoryBlobStore::default();
        let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&self.chain))
            .with_head_timestamp(head.timestamp)
            .kzg_settings(self.kzg_settings()?)
            .with_additional_tasks(1)
            .build_with_tasks(blockchain_db.clone(), ctx.task_executor.clone(), blob_store.clone());

        let transaction_pool =
            reth_transaction_pool::Pool::eth_pool(validator, blob_store, self.txpool.pool_config());
        info!(target: "reth::cli", "Transaction pool initialized");

        // spawn txpool maintenance task
        {
            let pool = transaction_pool.clone();
            let chain_events = blockchain_db.canonical_state_stream();
            let client = blockchain_db.clone();
            ctx.task_executor.spawn_critical(
                "txpool maintenance task",
                reth_transaction_pool::maintain::maintain_transaction_pool_future(
                    client,
                    pool,
                    chain_events,
                    ctx.task_executor.clone(),
                    Default::default(),
                ),
            );
            debug!(target: "reth::cli", "Spawned txpool maintenance task");
        }

        info!(target: "reth::cli", "Connecting to P2P network");
        let network_secret_path =
            self.network.p2p_secret_key.clone().unwrap_or_else(|| data_dir.p2p_secret_path());
        let secret_key = get_secret_key(&network_secret_path)?;
        info!(target: "reth::cli", network_secret_path = ?network_secret_path, secret_key = %reth_primitives::hex::encode(secret_key.as_ref()), "P2P network");
        let default_peers_path = data_dir.known_peers_path();
        let network_config = self.load_network_config(
            &config,
            provider_factory.clone(),
            ctx.task_executor.clone(),
            head,
            secret_key,
            default_peers_path.clone(),
        );

        let network_client = network_config.client.clone();
        let mut network_builder = NetworkManager::builder(network_config).await?;

        let components = RethNodeComponentsImpl {
            provider: blockchain_db.clone(),
            pool: transaction_pool.clone(),
            network: network_builder.handle(),
            task_executor: ctx.task_executor.clone(),
            events: blockchain_db.clone(),
        };

        // allow network modifications
        self.ext.configure_network(network_builder.network_mut(), &components)?;

        // launch network
        let clayer_consensus_messaging_agent = ClayerConsensusMessagingAgent::new();
        let network = self.start_network(
            network_builder,
            &ctx.task_executor,
            transaction_pool.clone(),
            network_client,
            default_peers_path,
            clayer_consensus_messaging_agent.clone(),
        );

        info!(target: "reth::cli", peer_id = %network.peer_id(), local_addr = %network.local_addr(), enode = %network.local_node_record(), "Connected to P2P network");
        let network_client = network.fetch_client().await?;

        self.ext.on_components_initialized(&components)?;

        debug!(target: "reth::cli", "Spawning payload builder service");
        let payload_builder = self.ext.spawn_payload_builder_service(&self.builder, &components)?;

        let (consensus_engine_tx, consensus_engine_rx) = unbounded_channel();
        let max_block = if let Some(block) = self.debug.max_block {
            Some(block)
        } else if let Some(tip) = self.debug.tip {
            Some(self.lookup_or_fetch_tip(provider_factory.clone(), &network_client, tip).await?)
        } else {
            None
        };

        // Configure the pipeline
        let (mut pipeline, client) = if self.dev.dev {
            info!(target: "reth::cli", "Starting Reth in dev mode");

            let mining_mode = if let Some(interval) = self.dev.block_time {
                MiningMode::interval(interval)
            } else if let Some(max_transactions) = self.dev.block_max_transactions {
                MiningMode::instant(
                    max_transactions,
                    transaction_pool.pending_transactions_listener(),
                )
            } else {
                info!(target: "reth::cli", "No mining mode specified, defaulting to ReadyTransaction");
                MiningMode::instant(1, transaction_pool.pending_transactions_listener())
            };

            let (_, client, mut task) = AutoSealBuilder::new(
                Arc::clone(&self.chain),
                blockchain_db.clone(),
                transaction_pool.clone(),
                consensus_engine_tx.clone(),
                canon_state_notification_sender,
                mining_mode,
            )
            .build();

            let mut pipeline = self
                .build_networked_pipeline(
                    &config.stages,
                    client.clone(),
                    Arc::clone(&consensus),
                    provider_factory.clone(),
                    &ctx.task_executor,
                    sync_metrics_tx,
                    prune_config.clone(),
                    max_block,
                )
                .await?;

            let pipeline_events = pipeline.events();
            task.set_pipeline_events(pipeline_events);
            debug!(target: "reth::cli", "Spawning auto mine task");
            ctx.task_executor.spawn(Box::pin(task));

            (pipeline, EitherDownloader::Left(client))
        } else {
            let mut pipeline = self
                .build_networked_pipeline(
                    &config.stages,
                    network_client.clone(),
                    Arc::clone(&consensus),
                    provider_factory.clone(),
                    &ctx.task_executor,
                    sync_metrics_tx,
                    prune_config.clone(),
                    max_block,
                )
                .await?;

            // ===============================================================================
            // extract the jwt secret from the args if possible
            let consensus_db = ConsensusProvider::new(provider_factory.clone())?;

            let default_jwt_path = data_dir.jwt_path();
            let jwt_secret = self.rpc.auth_jwt_secret(default_jwt_path)?;
            let mut task = ConsensusBuilder::new(
                jwt_secret.as_bytes(),
                secret_key,
                Arc::clone(&self.chain),
                blockchain_db.clone(),
                transaction_pool.clone(),
                network.clone(),
                clayer_consensus_messaging_agent,
                consensus_db,
                self.rpc.auth_port,
            )
            .build();
            let pipeline_events = pipeline.events();
            task.set_pipeline_events(pipeline_events);
            //ctx.task_executor.spawn(Box::pin(task));

            std::thread::spawn(move || {
                println!("a thread for consensus!");
                task.start()
            });
            // ===============================================================================

            (pipeline, EitherDownloader::Right(network_client))
        };

        let pipeline_events = pipeline.events();

        let initial_target = if let Some(tip) = self.debug.tip {
            // Set the provided tip as the initial pipeline target.
            debug!(target: "reth::cli", %tip, "Tip manually set");
            Some(tip)
        } else if self.debug.continuous {
            // Set genesis as the initial pipeline target.
            // This will allow the downloader to start
            debug!(target: "reth::cli", "Continuous sync mode enabled");
            Some(genesis_hash)
        } else {
            None
        };

        let mut hooks = EngineHooks::new();

        let pruner_events = if let Some(prune_config) = prune_config {
            let mut pruner = PrunerBuilder::new(prune_config.clone())
                .max_reorg_depth(tree_config.max_reorg_depth() as usize)
                .prune_delete_limit(self.chain.prune_delete_limit)
                .build(provider_factory, snapshotter.highest_snapshot_receiver());

            let events = pruner.events();
            hooks.add(PruneHook::new(pruner, Box::new(ctx.task_executor.clone())));

            info!(target: "reth::cli", ?prune_config, "Pruner initialized");
            Either::Left(events)
        } else {
            Either::Right(stream::empty())
        };

        // Configure the consensus engine
        let (beacon_consensus_engine, beacon_engine_handle) = BeaconConsensusEngine::with_channel(
            client,
            pipeline,
            blockchain_db.clone(),
            Box::new(ctx.task_executor.clone()),
            Box::new(network.clone()),
            max_block,
            self.debug.continuous,
            payload_builder.clone(),
            initial_target,
            MIN_BLOCKS_FOR_PIPELINE_RUN,
            consensus_engine_tx,
            consensus_engine_rx,
            hooks,
        )?;
        info!(target: "reth::cli", "Consensus engine initialized");

        let events = stream_select!(
            network.event_listener().map(Into::into),
            beacon_engine_handle.event_listener().map(Into::into),
            pipeline_events.map(Into::into),
            if self.debug.tip.is_none() {
                Either::Left(
                    ConsensusLayerHealthEvents::new(Box::new(blockchain_db.clone()))
                        .map(Into::into),
                )
            } else {
                Either::Right(stream::empty())
            },
            pruner_events.map(Into::into)
        );
        ctx.task_executor.spawn_critical(
            "events task",
            events::handle_events(Some(network.clone()), Some(head.number), events, db.clone()),
        );

        let engine_api = EngineApi::new(
            blockchain_db.clone(),
            self.chain.clone(),
            beacon_engine_handle,
            payload_builder.into(),
            Box::new(ctx.task_executor.clone()),
        );
        info!(target: "reth::cli", "Engine API handler initialized");

        // extract the jwt secret from the args if possible
        let default_jwt_path = data_dir.jwt_path();
        let jwt_secret = self.rpc.auth_jwt_secret(default_jwt_path)?;

        // adjust rpc port numbers based on instance number
        self.adjust_instance_ports();

        // Start RPC servers
        let _rpc_server_handles =
            self.rpc.start_servers(&components, engine_api, jwt_secret, &mut self.ext).await?;

        // Run consensus engine to completion
        let (tx, rx) = oneshot::channel();
        info!(target: "reth::cli", "Starting consensus engine");
        ctx.task_executor.spawn_critical_blocking("consensus engine", async move {
            let res = beacon_consensus_engine.await;
            let _ = tx.send(res);
        });

        self.ext.on_node_started(&components)?;

        // If `enable_genesis_walkback` is set to true, the rollup client will need to
        // perform the derivation pipeline from genesis, validating the data dir.
        // When set to false, set the finalized, safe, and unsafe head block hashes
        // on the rollup client using a fork choice update. This prevents the rollup
        // client from performing the derivation pipeline from genesis, and instead
        // starts syncing from the current tip in the DB.
        #[cfg(feature = "optimism")]
        if self.chain.is_optimism() && !self.rollup.enable_genesis_walkback {
            let client = _rpc_server_handles.auth.http_client();
            reth_rpc_api::EngineApiClient::fork_choice_updated_v2(
                &client,
                reth_rpc_types::engine::ForkchoiceState {
                    head_block_hash: head.hash,
                    safe_block_hash: head.hash,
                    finalized_block_hash: head.hash,
                },
                None,
            )
            .await?;
        }

        rx.await??;

        info!(target: "reth::cli", "Consensus engine has exited.");

        if self.debug.terminate {
            Ok(())
        } else {
            // The pipeline has finished downloading blocks up to `--debug.tip` or
            // `--debug.max-block`. Keep other node components alive for further usage.
            futures::future::pending().await
        }
    }

    /// Returns the [Consensus] instance to use.
    ///
    /// By default this will be a [BeaconConsensus] instance, but if the `--dev` flag is set, it
    /// will be an [AutoSealConsensus] instance.
    pub fn consensus(&self) -> Arc<dyn Consensus> {
        if self.dev.dev {
            Arc::new(AutoSealConsensus::new(Arc::clone(&self.chain)))
        } else {
            Arc::new(BeaconConsensus::new(Arc::clone(&self.chain)))
        }
    }

    /// Constructs a [Pipeline] that's wired to the network
    #[allow(clippy::too_many_arguments)]
    async fn build_networked_pipeline<DB, Client>(
        &self,
        config: &StageConfig,
        client: Client,
        consensus: Arc<dyn Consensus>,
        provider_factory: ProviderFactory<DB>,
        task_executor: &TaskExecutor,
        metrics_tx: reth_stages::MetricEventsSender,
        prune_config: Option<PruneConfig>,
        max_block: Option<BlockNumber>,
    ) -> eyre::Result<Pipeline<DB>>
    where
        DB: Database + Unpin + Clone + 'static,
        Client: HeadersClient + BodiesClient + Clone + 'static,
    {
        // building network downloaders using the fetch client
        let header_downloader = ReverseHeadersDownloaderBuilder::new(config.headers)
            .build(client.clone(), Arc::clone(&consensus))
            .into_task_with(task_executor);

        let body_downloader = BodiesDownloaderBuilder::new(config.bodies)
            .build(client, Arc::clone(&consensus), provider_factory.clone())
            .into_task_with(task_executor);

        let pipeline = self
            .build_pipeline(
                provider_factory,
                config,
                header_downloader,
                body_downloader,
                consensus,
                max_block,
                self.debug.continuous,
                metrics_tx,
                prune_config,
            )
            .await?;

        Ok(pipeline)
    }

    /// Returns the chain specific path to the data dir.
    fn data_dir(&self) -> ChainPath<DataDirPath> {
        self.datadir.unwrap_or_chain_default(self.chain.chain)
    }

    /// Returns the path to the config file.
    fn config_path(&self) -> PathBuf {
        self.config.clone().unwrap_or_else(|| self.data_dir().config_path())
    }

    /// Loads the reth config with the given datadir root
    fn load_config(&self) -> eyre::Result<Config> {
        let config_path = self.config_path();
        let mut config = confy::load_path::<Config>(&config_path)
            .wrap_err_with(|| format!("Could not load config file {:?}", config_path))?;

        info!(target: "reth::cli", path = ?config_path, "Configuration loaded");

        // Update the config with the command line arguments
        config.peers.connect_trusted_nodes_only = self.network.trusted_only;

        if !self.network.trusted_peers.is_empty() {
            info!(target: "reth::cli", "Adding trusted nodes");
            self.network.trusted_peers.iter().for_each(|peer| {
                config.peers.trusted_nodes.insert(*peer);
            });
        }

        Ok(config)
    }

    /// Loads the trusted setup params from a given file path or falls back to
    /// `MAINNET_KZG_TRUSTED_SETUP`.
    fn kzg_settings(&self) -> eyre::Result<Arc<KzgSettings>> {
        if let Some(ref trusted_setup_file) = self.trusted_setup_file {
            let trusted_setup = KzgSettings::load_trusted_setup_file(trusted_setup_file)
                .map_err(LoadKzgSettingsError::KzgError)?;
            Ok(Arc::new(trusted_setup))
        } else {
            Ok(Arc::clone(&MAINNET_KZG_TRUSTED_SETUP))
        }
    }

    fn install_prometheus_recorder(&self) -> eyre::Result<PrometheusHandle> {
        prometheus_exporter::install_recorder()
    }

    async fn start_metrics_endpoint<Metrics>(
        &self,
        prometheus_handle: PrometheusHandle,
        db: Metrics,
    ) -> eyre::Result<()>
    where
        Metrics: DatabaseMetrics + 'static + Send + Sync,
    {
        if let Some(listen_addr) = self.metrics {
            info!(target: "reth::cli", addr = %listen_addr, "Starting metrics endpoint");
            prometheus_exporter::serve(
                listen_addr,
                prometheus_handle,
                db,
                metrics_process::Collector::default(),
            )
            .await?;
        }

        Ok(())
    }

    /// Spawns the configured network and associated tasks and returns the [NetworkHandle] connected
    /// to that network.
    fn start_network<C, Pool, Consensus>(
        &self,
        builder: NetworkBuilder<C, (), (), ()>,
        task_executor: &TaskExecutor,
        pool: Pool,
        client: C,
        default_peers_path: PathBuf,
        consensus: Consensus,
    ) -> NetworkHandle
    where
        C: BlockReader + HeaderProvider + Clone + Unpin + 'static,
        Pool: TransactionPool + Unpin + 'static,
        Consensus: ClayerConsensusMessageAgentTrait + Unpin + 'static,
    {
        let (handle, network, txpool, eth, consensus_manager) = builder
            .transactions(pool)
            .request_handler(client)
            .consensus(consensus)
            .split_with_handle();

        task_executor.spawn_critical("p2p txpool", txpool);
        task_executor.spawn_critical("p2p eth request handler", eth);
        task_executor.spawn_critical("p2p consensus", consensus_manager);

        let known_peers_file = self.network.persistent_peers_file(default_peers_path);
        task_executor
            .spawn_critical_with_graceful_shutdown_signal("p2p network task", |shutdown| {
                run_network_until_shutdown(shutdown, network, known_peers_file)
            });

        handle
    }

    /// Fetches the head block from the database.
    ///
    /// If the database is empty, returns the genesis block.
    fn lookup_head<DB: Database>(&self, factory: ProviderFactory<DB>) -> RethResult<Head> {
        let provider = factory.provider()?;

        let head = provider.get_stage_checkpoint(StageId::Finish)?.unwrap_or_default().block_number;

        let header = provider
            .header_by_number(head)?
            .expect("the header for the latest block is missing, database is corrupt");

        let total_difficulty = provider
            .header_td_by_number(head)?
            .expect("the total difficulty for the latest block is missing, database is corrupt");

        let hash = provider
            .block_hash(head)?
            .expect("the hash for the latest block is missing, database is corrupt");

        Ok(Head {
            number: head,
            hash,
            difficulty: header.difficulty,
            total_difficulty,
            timestamp: header.timestamp,
        })
    }

    /// Attempt to look up the block number for the tip hash in the database.
    /// If it doesn't exist, download the header and return the block number.
    ///
    /// NOTE: The download is attempted with infinite retries.
    async fn lookup_or_fetch_tip<DB, Client>(
        &self,
        provider_factory: ProviderFactory<DB>,
        client: Client,
        tip: B256,
    ) -> RethResult<u64>
    where
        DB: Database,
        Client: HeadersClient,
    {
        Ok(self.fetch_tip(provider_factory, client, BlockHashOrNumber::Hash(tip)).await?.number)
    }

    /// Attempt to look up the block with the given number and return the header.
    ///
    /// NOTE: The download is attempted with infinite retries.
    async fn fetch_tip<DB, Client>(
        &self,
        factory: ProviderFactory<DB>,
        client: Client,
        tip: BlockHashOrNumber,
    ) -> RethResult<SealedHeader>
    where
        DB: Database,
        Client: HeadersClient,
    {
        let provider = factory.provider()?;

        let header = provider.header_by_hash_or_number(tip)?;

        // try to look up the header in the database
        if let Some(header) = header {
            info!(target: "reth::cli", ?tip, "Successfully looked up tip block in the database");
            return Ok(header.seal_slow());
        }

        info!(target: "reth::cli", ?tip, "Fetching tip block from the network.");
        loop {
            match get_single_header(&client, tip).await {
                Ok(tip_header) => {
                    info!(target: "reth::cli", ?tip, "Successfully fetched tip");
                    return Ok(tip_header);
                }
                Err(error) => {
                    error!(target: "reth::cli", %error, "Failed to fetch the tip. Retrying...");
                }
            }
        }
    }

    fn load_network_config<DB: Database>(
        &self,
        config: &Config,
        provider_factory: ProviderFactory<DB>,
        executor: TaskExecutor,
        head: Head,
        secret_key: SecretKey,
        default_peers_path: PathBuf,
    ) -> NetworkConfig<ProviderFactory<DB>> {
        let cfg_builder = self
            .network
            .network_config(config, self.chain.clone(), secret_key, default_peers_path)
            .with_task_executor(Box::new(executor))
            .set_head(head)
            .listener_addr(SocketAddr::V4(SocketAddrV4::new(
                self.network.addr,
                // set discovery port based on instance number
                self.network.port + self.instance - 1,
            )))
            .discovery_addr(SocketAddr::V4(SocketAddrV4::new(
                self.network.addr,
                // set discovery port based on instance number
                self.network.port + self.instance - 1,
            )));

        // When `sequencer_endpoint` is configured, the node will forward all transactions to a
        // Sequencer node for execution and inclusion on L1, and disable its own txpool
        // gossip to prevent other parties in the network from learning about them.
        #[cfg(feature = "optimism")]
        let cfg_builder = cfg_builder
            .sequencer_endpoint(self.rollup.sequencer_http.clone())
            .disable_tx_gossip(self.rollup.disable_txpool_gossip);

        cfg_builder.build(provider_factory)
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_pipeline<DB, H, B>(
        &self,
        provider_factory: ProviderFactory<DB>,
        config: &StageConfig,
        header_downloader: H,
        body_downloader: B,
        consensus: Arc<dyn Consensus>,
        max_block: Option<u64>,
        continuous: bool,
        metrics_tx: reth_stages::MetricEventsSender,
        prune_config: Option<PruneConfig>,
    ) -> eyre::Result<Pipeline<DB>>
    where
        DB: Database + Clone + 'static,
        H: HeaderDownloader + 'static,
        B: BodyDownloader + 'static,
    {
        let mut builder = Pipeline::builder();

        if let Some(max_block) = max_block {
            debug!(target: "reth::cli", max_block, "Configuring builder to use max block");
            builder = builder.with_max_block(max_block)
        }

        let (tip_tx, tip_rx) = watch::channel(B256::ZERO);
        use reth_revm_inspectors::stack::InspectorStackConfig;
        let factory = reth_revm::EvmProcessorFactory::new(self.chain.clone());

        let stack_config = InspectorStackConfig {
            use_printer_tracer: self.debug.print_inspector,
            hook: if let Some(hook_block) = self.debug.hook_block {
                Hook::Block(hook_block)
            } else if let Some(tx) = self.debug.hook_transaction {
                Hook::Transaction(tx)
            } else if self.debug.hook_all {
                Hook::All
            } else {
                Hook::None
            },
        };

        let factory = factory.with_stack_config(stack_config);

        let prune_modes = prune_config.map(|prune| prune.segments).unwrap_or_default();

        let header_mode =
            if continuous { HeaderSyncMode::Continuous } else { HeaderSyncMode::Tip(tip_rx) };
        let pipeline = builder
            .with_tip_sender(tip_tx)
            .with_metrics_tx(metrics_tx.clone())
            .add_stages(
                DefaultStages::new(
                    provider_factory.clone(),
                    header_mode,
                    Arc::clone(&consensus),
                    header_downloader,
                    body_downloader,
                    factory.clone(),
                )
                .set(
                    TotalDifficultyStage::new(consensus)
                        .with_commit_threshold(config.total_difficulty.commit_threshold),
                )
                .set(SenderRecoveryStage {
                    commit_threshold: config.sender_recovery.commit_threshold,
                })
                .set(
                    ExecutionStage::new(
                        factory,
                        ExecutionStageThresholds {
                            max_blocks: config.execution.max_blocks,
                            max_changes: config.execution.max_changes,
                            max_cumulative_gas: config.execution.max_cumulative_gas,
                        },
                        config
                            .merkle
                            .clean_threshold
                            .max(config.account_hashing.clean_threshold)
                            .max(config.storage_hashing.clean_threshold),
                        prune_modes.clone(),
                    )
                    .with_metrics_tx(metrics_tx),
                )
                .set(AccountHashingStage::new(
                    config.account_hashing.clean_threshold,
                    config.account_hashing.commit_threshold,
                ))
                .set(StorageHashingStage::new(
                    config.storage_hashing.clean_threshold,
                    config.storage_hashing.commit_threshold,
                ))
                .set(MerkleStage::new_execution(config.merkle.clean_threshold))
                .set(TransactionLookupStage::new(
                    config.transaction_lookup.commit_threshold,
                    prune_modes.transaction_lookup,
                ))
                .set(IndexAccountHistoryStage::new(
                    config.index_account_history.commit_threshold,
                    prune_modes.account_history,
                ))
                .set(IndexStorageHistoryStage::new(
                    config.index_storage_history.commit_threshold,
                    prune_modes.storage_history,
                )),
            )
            .build(provider_factory);

        Ok(pipeline)
    }

    /// Change rpc port numbers based on the instance number.
    fn adjust_instance_ports(&mut self) {
        // auth port is scaled by a factor of instance * 100
        self.rpc.auth_port += self.instance * 100 - 100;
        // http port is scaled by a factor of -instance
        self.rpc.http_port -= self.instance - 1;
        // ws port is scaled by a factor of instance * 2
        self.rpc.ws_port += self.instance * 2 - 2;
    }
}

/// Drives the [NetworkManager] future until a [Shutdown](reth_tasks::shutdown::Shutdown) signal is
/// received. If configured, this writes known peers to `persistent_peers_file` afterwards.
async fn run_network_until_shutdown<C>(
    shutdown: reth_tasks::shutdown::GracefulShutdown,
    network: NetworkManager<C>,
    persistent_peers_file: Option<PathBuf>,
) where
    C: BlockReader + HeaderProvider + Clone + Unpin + 'static,
{
    pin_mut!(network, shutdown);

    let mut graceful_guard = None;
    tokio::select! {
        _ = &mut network => {},
        guard = shutdown => {
            graceful_guard = Some(guard);
        },
    }

    if let Some(file_path) = persistent_peers_file {
        let known_peers = network.all_peers().collect::<Vec<_>>();
        if let Ok(known_peers) = serde_json::to_string_pretty(&known_peers) {
            trace!(target: "reth::cli", peers_file =?file_path, num_peers=%known_peers.len(), "Saving current peers");
            let parent_dir = file_path.parent().map(fs::create_dir_all).transpose();
            match parent_dir.and_then(|_| fs::write(&file_path, known_peers)) {
                Ok(_) => {
                    info!(target: "reth::cli", peers_file=?file_path, "Wrote network peers to file");
                }
                Err(err) => {
                    warn!(target: "reth::cli", ?err, peers_file=?file_path, "Failed to write network peers to file");
                }
            }
        }
    }

    drop(graceful_guard)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::utils::SUPPORTED_CHAINS;
    use reth_discv4::DEFAULT_DISCOVERY_PORT;
    use std::{
        net::{IpAddr, Ipv4Addr},
        path::Path,
    };

    #[test]
    fn parse_help_node_command() {
        let err = NodeCommand::<()>::try_parse_from(["reth", "--help"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
    }

    #[test]
    fn parse_common_node_command_chain_args() {
        for chain in SUPPORTED_CHAINS {
            let args: NodeCommand = NodeCommand::<()>::parse_from(["reth", "--chain", chain]);
            assert_eq!(args.chain.chain, chain.parse().unwrap());
        }
    }

    #[test]
    fn parse_discovery_addr() {
        let cmd =
            NodeCommand::<()>::try_parse_from(["reth", "--discovery.addr", "127.0.0.1"]).unwrap();
        assert_eq!(cmd.network.discovery.addr, Ipv4Addr::LOCALHOST);
    }

    #[test]
    fn parse_addr() {
        let cmd = NodeCommand::<()>::try_parse_from([
            "reth",
            "--discovery.addr",
            "127.0.0.1",
            "--addr",
            "127.0.0.1",
        ])
        .unwrap();
        assert_eq!(cmd.network.discovery.addr, Ipv4Addr::LOCALHOST);
        assert_eq!(cmd.network.addr, Ipv4Addr::LOCALHOST);
    }

    #[test]
    fn parse_discovery_port() {
        let cmd = NodeCommand::<()>::try_parse_from(["reth", "--discovery.port", "300"]).unwrap();
        assert_eq!(cmd.network.discovery.port, 300);
    }

    #[test]
    fn parse_port() {
        let cmd =
            NodeCommand::<()>::try_parse_from(["reth", "--discovery.port", "300", "--port", "99"])
                .unwrap();
        assert_eq!(cmd.network.discovery.port, 300);
        assert_eq!(cmd.network.port, 99);
    }

    #[test]
    fn parse_metrics_port() {
        let cmd = NodeCommand::<()>::try_parse_from(["reth", "--metrics", "9001"]).unwrap();
        assert_eq!(cmd.metrics, Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001)));

        let cmd = NodeCommand::<()>::try_parse_from(["reth", "--metrics", ":9001"]).unwrap();
        assert_eq!(cmd.metrics, Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001)));

        let cmd =
            NodeCommand::<()>::try_parse_from(["reth", "--metrics", "localhost:9001"]).unwrap();
        assert_eq!(cmd.metrics, Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001)));
    }

    #[test]
    fn parse_config_path() {
        let cmd = NodeCommand::<()>::try_parse_from(["reth", "--config", "my/path/to/reth.toml"])
            .unwrap();
        // always store reth.toml in the data dir, not the chain specific data dir
        let data_dir = cmd.datadir.unwrap_or_chain_default(cmd.chain.chain);
        let config_path = cmd.config.unwrap_or(data_dir.config_path());
        assert_eq!(config_path, Path::new("my/path/to/reth.toml"));

        let cmd = NodeCommand::<()>::try_parse_from(["reth"]).unwrap();

        // always store reth.toml in the data dir, not the chain specific data dir
        let data_dir = cmd.datadir.unwrap_or_chain_default(cmd.chain.chain);
        let config_path = cmd.config.clone().unwrap_or(data_dir.config_path());
        let end = format!("reth/{}/reth.toml", SUPPORTED_CHAINS[0]);
        assert!(config_path.ends_with(end), "{:?}", cmd.config);
    }

    #[test]
    fn parse_db_path() {
        let cmd = NodeCommand::<()>::try_parse_from(["reth"]).unwrap();
        let data_dir = cmd.datadir.unwrap_or_chain_default(cmd.chain.chain);
        let db_path = data_dir.db_path();
        let end = format!("reth/{}/db", SUPPORTED_CHAINS[0]);
        assert!(db_path.ends_with(end), "{:?}", cmd.config);

        let cmd =
            NodeCommand::<()>::try_parse_from(["reth", "--datadir", "my/custom/path"]).unwrap();
        let data_dir = cmd.datadir.unwrap_or_chain_default(cmd.chain.chain);
        let db_path = data_dir.db_path();
        assert_eq!(db_path, Path::new("my/custom/path/db"));
    }

    #[test]
    #[cfg(not(feature = "optimism"))] // dev mode not yet supported in op-reth
    fn parse_dev() {
        let cmd = NodeCommand::<()>::parse_from(["reth", "--dev"]);
        let chain = reth_primitives::DEV.clone();
        assert_eq!(cmd.chain.chain, chain.chain);
        assert_eq!(cmd.chain.genesis_hash, chain.genesis_hash);
        assert_eq!(
            cmd.chain.paris_block_and_final_difficulty,
            chain.paris_block_and_final_difficulty
        );
        assert_eq!(cmd.chain.hardforks, chain.hardforks);

        assert!(cmd.rpc.http);
        assert!(cmd.network.discovery.disable_discovery);

        assert!(cmd.dev.dev);
    }

    #[test]
    fn parse_instance() {
        let mut cmd = NodeCommand::<()>::parse_from(["reth"]);
        cmd.adjust_instance_ports();
        cmd.network.port = DEFAULT_DISCOVERY_PORT + cmd.instance - 1;
        // check rpc port numbers
        assert_eq!(cmd.rpc.auth_port, 8551);
        assert_eq!(cmd.rpc.http_port, 8545);
        assert_eq!(cmd.rpc.ws_port, 8546);
        // check network listening port number
        assert_eq!(cmd.network.port, 30303);

        let mut cmd = NodeCommand::<()>::parse_from(["reth", "--instance", "2"]);
        cmd.adjust_instance_ports();
        cmd.network.port = DEFAULT_DISCOVERY_PORT + cmd.instance - 1;
        // check rpc port numbers
        assert_eq!(cmd.rpc.auth_port, 8651);
        assert_eq!(cmd.rpc.http_port, 8544);
        assert_eq!(cmd.rpc.ws_port, 8548);
        // check network listening port number
        assert_eq!(cmd.network.port, 30304);

        let mut cmd = NodeCommand::<()>::parse_from(["reth", "--instance", "3"]);
        cmd.adjust_instance_ports();
        cmd.network.port = DEFAULT_DISCOVERY_PORT + cmd.instance - 1;
        // check rpc port numbers
        assert_eq!(cmd.rpc.auth_port, 8751);
        assert_eq!(cmd.rpc.http_port, 8543);
        assert_eq!(cmd.rpc.ws_port, 8550);
        // check network listening port number
        assert_eq!(cmd.network.port, 30305);
    }
}
