use crate::raft::{RaftNode, state::*};

impl RaftNode {
    pub fn become_follower(&mut self, term: u64) {
        self.persistent.current_term = term;
        self.persistent.voted_for = None;
        self.role = NodeRole::Follower;
        self.votes_received.clear();
    }
}
