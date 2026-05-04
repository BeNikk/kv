use crate::raft::{RaftNode, state::*};

impl RaftNode {
    // this function is called when there is a new term/leader, then our present node becomes a follower.
    pub fn become_follower(&mut self, term: u64) {
        self.persistent.current_term = term;
        self.persistent.voted_for = None;
        self.role = NodeRole::Follower;
        self.votes_received.clear();
    }

    // this function is called when timeout occurs, it will increment the current term and become a candidate.
    pub fn on_election_timeout(&mut self) -> Vec<Message> {
        self.role = NodeRole::Candidate;
        self.persistent.current_term += 1;
        self.persistent.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);

        self.peers
            .iter()
            .map(|&peer| Message::RequestVote {
                to: peer,
                args: VoteRequest {
                    term: self.persistent.current_term,
                    candidate_id: self.id,
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                },
            })
            .collect()
    }
}
