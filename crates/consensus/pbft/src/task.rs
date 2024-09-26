use crate::consensus::{
    assemble_peer_id, clayer_block_from_header, clayer_block_from_seal, ParsedMessage, PbftConfig,
    PbftError, PbftMode, PbftState,
};
use crate::engine_api::ApiService;
use crate::engine_pbft::{handle_consensus_event, parse_consensus_message, ConsensusEvent};
use crate::incoming_msg_queue::IncomingMsgQueue;
use crate::{
    consensus::{ClayerConsensusEngine, ELECT_VOTING_ADDRESS},
    timing,
};
use crate::{create_sync_api, AuthHttpConfig, ClayerConsensusMessageAgent};
use futures_util::{future::BoxFuture, FutureExt};
use parking_lot::RwLock;
use reth_chainspec::ChainSpec;
use reth_eth_wire::{ClayerConsensusEvent, ClayerConsensusMessageAgentTrait};
use reth_primitives::SealedHeader;
use reth_provider::{
    BlockReaderIdExt, CanonChainTracker, ConsensusNumberReader, ConsensusNumberWriter,
    StageCheckpointReader, StateProviderFactory,
};
use reth_stages::PipelineEvent;
use reth_tokio_util::EventStream;
use secp256k1::SecretKey;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tracing::*;

pub struct ClTask<Client, CDB> {
    /// The configured chain spec
    // #[allow(unused)]
    chain_spec: Arc<ChainSpec>,
    /// The client used to interact with the state
    client: Client,
    /// Single active future that inserts a new block into `storage`
    insert_task: Option<BoxFuture<'static, Option<EventStream<PipelineEvent>>>>,
    /// backlog of sets of transactions ready to be mined
    // queued: VecDeque<Vec<Arc<ValidPoolTransaction<<Pool as TransactionPool>::Transaction>>>>,
    queued: VecDeque<u64>,
    /// The pipeline events to listen on
    pipe_line_events: Option<EventStream<PipelineEvent>>,
    ///
    block_publishing_ticker: timing::AsyncTicker,
    /// consensus agent receive consensus event and push consensus message
    consensus_agent: ClayerConsensusMessageAgent,
    /// storage
    storages: Arc<CDB>,
    /// consensus loop running state
    // #[allow(unused)]
    pbft_running_state: Arc<AtomicBool>,
    /// The latest sealed header
    startup_latest_header: SealedHeader,
    /// the handle of the consensus thread
    consensus_engine_task_handle: Option<std::thread::JoinHandle<()>>,
    /// the http auth config
    auth_config: AuthHttpConfig,
    /// the node secret
    secret: SecretKey,
    /// message queue for receive consensus event
    pub(crate) consensus_event_queue: IncomingMsgQueue,
    /// the consensus receiver
    pub(crate) consensus_event_receiver:
        Arc<RwLock<tokio::sync::mpsc::Receiver<ClayerConsensusEvent>>>,
}

impl<Client, CDB> ClTask<Client, CDB>
where
    CDB: ConsensusNumberReader + ConsensusNumberWriter + 'static,
    Client: BlockReaderIdExt + StageCheckpointReader + Clone + 'static,
{
    /// Creates a new instance of the task
    pub(crate) fn new(
        secret: SecretKey,
        chain_spec: Arc<ChainSpec>,
        client: Client,
        auth_config: AuthHttpConfig,
        consensus_agent: ClayerConsensusMessageAgent,
        storages: CDB,
        startup_latest_header: SealedHeader,
    ) -> Self {
        let consensus_event_receiver =
            Arc::new(RwLock::new(consensus_agent.incoming_consensus_channel()));
        Self {
            secret,
            chain_spec,
            client,
            insert_task: None,
            queued: Default::default(),
            pipe_line_events: None,
            auth_config,
            block_publishing_ticker: timing::AsyncTicker::new(Duration::from_secs(30)),
            consensus_agent,
            storages: Arc::new(storages),
            pbft_running_state: Arc::new(AtomicBool::new(false)),
            startup_latest_header,
            consensus_engine_task_handle: None,
            consensus_event_queue: IncomingMsgQueue::new(),
            consensus_event_receiver,
        }
    }

    /// Sets the pipeline events to listen on.
    pub fn set_pipeline_events(&mut self, events: EventStream<PipelineEvent>) {
        self.pipe_line_events = Some(events);
    }

    pub fn start_clayer_consensus_engine(&mut self) {
        let consensus_agent = self.consensus_agent.clone();
        let consensus_event_queue = self.consensus_event_queue.clone();
        let auth_config = self.auth_config.clone();

        let cdb = self.storages.clone();
        let client = self.client.clone();
        let secret = self.secret.clone();

        let startup_latest_header = self.startup_latest_header.clone();
        let thread_join_handle = std::thread::spawn(move || {
            let api = create_sync_api(&auth_config);
            // let execution_block =
            //     api.get_block_by_number("latest".to_string()).expect("get latest block error");
            let execution_block = api.get_block_by_number("latest".to_string());
            info!(target: "consensus::cl","latest block: {:?}", execution_block);
            let validator_datas = api
                .query_validators(ELECT_VOTING_ADDRESS.to_string(), startup_latest_header.number)
                .expect("query validators failed");
            let peers = assemble_peer_id(validator_datas).expect("parse peer id failed");

            let mut pbft_config = PbftConfig::default();
            pbft_config.members.clone_from(&peers);
            let mut pbft_state = PbftState::new(secret, startup_latest_header.number, &pbft_config);
            let state = &mut pbft_state;
            let mut consensus_engine = ClayerConsensusEngine::new(
                consensus_agent.clone(),
                ApiService::new(Arc::new(api)),
                cdb,
                client,
                consensus_event_queue.clone(),
            );

            // let receiver = consensus_agent.receiver();
            let mut block_publishing_ticker =
                timing::SyncTicker::new(pbft_config.block_publishing_delay);

            let seal = match consensus_engine.load_seal(startup_latest_header.hash) {
                Ok(seal) => seal,
                Err(e) => {
                    log_any_error(Err(e));
                    panic!("Failed to load seal");
                }
            };

            let block = if startup_latest_header.number == 0 {
                // genesis block
                clayer_block_from_header(&startup_latest_header)
            } else {
                match seal {
                    Some(seal) => clayer_block_from_seal(&startup_latest_header, seal),
                    None => {
                        if state.is_validator() {
                            //no seal,so need to sync seal
                            state.becoming_validator = true;
                        }
                        clayer_block_from_header(&startup_latest_header)
                    }
                }
            };
            consensus_engine.initialize(block, &pbft_config, state);
            consensus_engine.start_idle_timeout(state);

            loop {
                if let Some(event) = consensus_event_queue.pop_msg() {
                    let incoming_event = match event {
                        ClayerConsensusEvent::PeerNetWork(peer_id, connect) => {
                            let e = if connect {
                                Some(ConsensusEvent::PeerConnected(peer_id))
                            } else {
                                Some(ConsensusEvent::PeerDisconnected(peer_id))
                            };
                            e
                        }
                        ClayerConsensusEvent::PeerMessage(peer_id, bytes) => {
                            let e = match parse_consensus_message(&bytes) {
                                Ok(msg) => Some(ConsensusEvent::PeerMessage(peer_id, msg)),
                                Err(e) => {
                                    log_any_error(Err(e));
                                    None
                                }
                            };
                            e
                        }
                        ClayerConsensusEvent::BlockValid(block_id) => {
                            Some(ConsensusEvent::BlockValid(block_id))
                        }
                        ClayerConsensusEvent::BlockInvalid(block_id) => {
                            Some(ConsensusEvent::BlockInvalid(block_id))
                        }
                        ClayerConsensusEvent::BlockCommit((block_id, committing)) => {
                            Some(ConsensusEvent::BlockCommit((block_id, committing)))
                        }
                    };
                    if let Some(incoming_event) = incoming_event {
                        match handle_consensus_event(&mut consensus_engine, incoming_event, state) {
                            Ok(again) => {
                                if !again {
                                    break;
                                }
                            }
                            Err(err) => log_any_error(Err(err)),
                        }
                    }
                } else {
                    log_any_error(consensus_engine.sync_seal(state));
                    sleep(pbft_config.update_recv_timeout);
                }

                if state.is_validator() {
                    // If the block publishing delay has passed, attempt to publish a block
                    block_publishing_ticker
                        .tick(|| log_any_error(consensus_engine.try_publish(state)));

                    // If the idle timeout has expired, initiate a view change
                    if consensus_engine.check_idle_timeout_expired(state) {
                        warn!(target:"consensus::cl", "Idle timeout expired; proposing view change");
                        log_any_error(consensus_engine.start_view_change(state, state.view + 1));
                    }

                    // If the commit timeout has expired, initiate a view change
                    if consensus_engine.check_commit_timeout_expired(state) {
                        warn!(target:"consensus::cl", "Commit timeout expired; proposing view change");
                        log_any_error(consensus_engine.start_view_change(state, state.view + 1));
                    }

                    // Check the view change timeout if the node is view changing so we can start a new
                    // view change if we don't get a NewView in time
                    if let PbftMode::ViewChanging(v) = state.mode {
                        if consensus_engine.check_view_change_timeout_expired(state) {
                            warn!(target:"consensus::cl",
                                "View change timeout expired; proposing view change for view {}",
                                v + 1
                            );
                            log_any_error(consensus_engine.start_view_change(state, v + 1));
                        }
                    }
                }
            }
        });
        self.consensus_engine_task_handle = Some(thread_join_handle);
    }
}

impl<Client, CDB> Future for ClTask<Client, CDB>
where
    Client: StateProviderFactory
        + CanonChainTracker
        + BlockReaderIdExt
        + StageCheckpointReader
        + Clone
        + Unpin
        + 'static,
    CDB: ConsensusNumberReader + ConsensusNumberWriter + Unpin + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            if let Poll::Ready(x) = this.block_publishing_ticker.poll(cx) {
                this.queued.push_back(x);
                // info!(target: "consensus::cl", "execute insert task: publishing_ticker Ready {}",this.queued.len());
                if this.queued.len() > 1 {
                    // info!(target: "consensus::cl", "pbft_running_state {}",this.pbft_running_state.load(Ordering::Relaxed));
                    if !this.pbft_running_state.load(Ordering::Relaxed) {
                        info!(target: "consensus::cl", "start pbft engine");
                        this.pbft_running_state.store(true, Ordering::Relaxed);
                        this.start_clayer_consensus_engine();
                    }
                }
            }

            if this.insert_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }
                if this.queued.len() > 1 {
                    this.queued.pop_front().expect("not empty");
                }

                let chain_spec = Arc::clone(&this.chain_spec);
                //let client = this.client.clone();
                let events = this.pipe_line_events.take();
                // let agent = this.consensus_agent.clone();
                let consensus_event_receiver = this.consensus_event_receiver.clone();
                let consensus_event_queue = this.consensus_event_queue.clone();

                // define task
                this.insert_task = Some(Box::pin(async move {
                    // let data = reth_primitives::Bytes::from("hello");
                    // agent.broadcast_consensus(Vec::new(), data);

                    let my_duration = tokio::time::Duration::from_millis(500);
                    while let Ok(value) =
                        tokio::time::timeout(my_duration, consensus_event_receiver.write().recv())
                            .await
                    {
                        if let Some(event) = value {
                            consensus_event_queue.push_msg(event);

                            // match event {
                            //     ClayerConsensusEvent::PeerMessage(peer_id, message) => {
                            //         match std::str::from_utf8(&message) {
                            //             Ok(v) => {
                            //                 info!(target: "consensus::cl", "execute insert task: receive consensus event ({}):({})",peer_id,v);
                            //             }
                            //             Err(e) => {
                            //                 error!(target: "consensus::cl", "parse receive message error:{}",e)
                            //             }
                            //         }
                            //     }
                            //     _ => {}
                            // }
                        } else {
                            info!(target: "consensus::cl", "execute insert task: receive consensus event 0");
                        }
                    }

                    trace!(target: "consensus::cl", "execute insert task: skip id:{}",chain_spec.chain().id());

                    events
                }));
            }

            if let Some(mut fut) = this.insert_task.take() {
                match fut.poll_unpin(cx) {
                    Poll::Ready(events) => {
                        this.pipe_line_events = events;
                    }
                    Poll::Pending => {
                        this.insert_task = Some(fut);
                        break;
                    }
                }
            }
        }
        Poll::Pending
    }
}

fn log_any_error(res: Result<(), PbftError>) {
    if let Err(e) = res {
        // Treat errors that result from other nodes' messages as warnings
        match e {
            PbftError::SigningError(_)
            | PbftError::FaultyPrimary(_)
            | PbftError::InvalidMessage(_) => warn!("{}", e),
            _ => error!(target:"consensus::cl","{}", e),
        }
    }
}
