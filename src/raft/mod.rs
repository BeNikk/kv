pub mod election;
pub mod log;
pub mod replication;
pub mod state;
use state::*;
use std::collections::HashSet;

pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub role: NodeRole,
    pub persistent: PersistentState,
    pub volatile: VolatileState,
    pub votes_received: HashSet<NodeId>,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            peers,
            role: NodeRole::Follower,
            persistent: PersistentState::new(),
            volatile: VolatileState::new(),
            votes_received: HashSet::new(),
        }
    }
}
