pub mod actor;
pub mod election;
pub mod log;
pub mod replication;
pub mod state;
use crate::raft::log::Command;
use crate::storage::KvStore;
use state::*;
use std::collections::HashSet;
use tokio::sync::watch;

pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub role: NodeRole,
    pub persistent: PersistentState,
    pub volatile: VolatileState,
    pub votes_received: HashSet<NodeId>,
    pub store: KvStore,
    pub commit_tx: watch::Sender<u64>,
    pub commit_rx: watch::Receiver<u64>,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        let (commit_tx, commit_rx) = watch::channel(0u64);
        Self {
            id,
            peers,
            role: NodeRole::Follower,
            persistent: PersistentState::new(),
            volatile: VolatileState::new(),
            votes_received: HashSet::new(),
            store: KvStore::default(),
            commit_tx,
            commit_rx,
        }
    }
    pub fn persist(&self) {
        let path = format!("raft-state-node-{}.json", self.id);
        self.persistent.save(&path).expect("persist failed — fatal");
    }
}
use tokio::sync::oneshot;

pub enum RaftCommand {
    Propose {
        command: Command,
        reply: oneshot::Sender<u64>,
    },

    HandleAppendRequest {
        args: AppendRequest,
        reply: oneshot::Sender<AppendResponse>,
    },

    GetRole {
        reply: oneshot::Sender<NodeRole>,
    },

    GetValue {
        key: String,
        reply: oneshot::Sender<Option<String>>,
    },
    HandleVoteRequest {
        args: VoteRequest,
        reply: oneshot::Sender<VoteResponse>,
    },
}
