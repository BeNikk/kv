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
    /// Handles an incoming RequestVote RPC (receiver side).
    ///
    /// This function decides whether this node should grant its vote
    /// to a candidate during an election.
    ///
    /// A vote is granted ONLY if:
    /// 1. The candidate's term is at least as up-to-date as ours
    /// 2. We have not already voted for someone else in this term
    /// 3. The candidate's log is at least as up-to-date as ours
    ///
    /// This ensures:
    /// - Only one leader per term
    /// - Leaders always have the most up-to-date log
    pub fn handle_request_vote(&mut self, args: VoteRequest) -> VoteResponse {
        // Reject if candidate is from an older term
        if args.term < self.persistent.current_term {
            return VoteResponse {
                term: self.persistent.current_term,
                vote_granted: false,
            };
        }

        // If candidate has higher term, step down
        if args.term > self.persistent.current_term {
            self.become_follower(args.term);
        }

        // Check if we can vote (haven't voted yet or voted for same candidate)
        let can_vote = self.persistent.voted_for.is_none()
            || self.persistent.voted_for == Some(args.candidate_id);

        // Check if candidate's log is at least as up-to-date
        let log_ok = args.last_log_term > self.last_log_term()
            || (args.last_log_term == self.last_log_term()
                && args.last_log_index >= self.last_log_index());

        // Grant vote only if both conditions are satisfied
        if can_vote && log_ok {
            self.persistent.voted_for = Some(args.candidate_id);

            VoteResponse {
                term: self.persistent.current_term,
                vote_granted: true,
            }
        } else {
            VoteResponse {
                term: self.persistent.current_term,
                vote_granted: false,
            }
        }
    }
}
