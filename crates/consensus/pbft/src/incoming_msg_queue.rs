use std::{collections::VecDeque, sync::Arc};

use reth_eth_wire::ClayerConsensusEvent;

#[derive(Clone)]
pub struct IncomingMsgQueue {
    pub inner: Arc<parking_lot::RwLock<IncomingMsgQueueinner>>,
}

pub struct IncomingMsgQueueinner {
    pub queue: VecDeque<ClayerConsensusEvent>,
}

impl IncomingMsgQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(parking_lot::RwLock::new(IncomingMsgQueueinner {
                queue: VecDeque::new(),
            })),
        }
    }

    pub fn push_msg(&self, msg: ClayerConsensusEvent) {
        self.inner.write().queue.push_back(msg);
    }

    pub fn push_front_msg(&self, msg: ClayerConsensusEvent) {
        self.inner.write().queue.push_front(msg);
    }

    pub fn pop_msg(&self) -> Option<ClayerConsensusEvent> {
        self.inner.write().queue.pop_front()
    }
}
