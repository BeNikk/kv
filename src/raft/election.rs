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
            self.persist();
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
            self.persist();

            VoteResponse {
                term: self.persistent.current_term,
                vote_granted: true,
            }
        } else {
            self.persist();
            VoteResponse {
                term: self.persistent.current_term,
                vote_granted: false,
            }
        }
    }

    /// Handles a vote response from a peer during an election.
    ///
    /// This function is called after we (as a candidate) send RequestVote RPCs
    /// and start receiving replies.
    ///
    /// It:
    /// 1. Ignores responses if we are no longer a candidate
    /// 2. Steps down if it sees a higher term (safety rule)
    /// 3. Tracks votes that were granted
    /// 4. Checks if we have a majority
    /// 5. If majority is reached → becomes leader
    pub fn handle_vote_response(&mut self, from: NodeId, resp: VoteResponse) -> Vec<Message> {
        // If we are not a candidate anymore, ignore this response
        // (we may already be follower or leader)
        if self.role != NodeRole::Candidate {
            return vec![];
        }

        // If response has higher term, we are outdated → step down
        // This is a safety rule in Raft
        if resp.term > self.persistent.current_term {
            self.become_follower(resp.term);
            return vec![];
        }

        // If vote was granted, record who voted for us
        if resp.vote_granted {
            self.votes_received.insert(from);
        }

        // Majority = more than half of all nodes (including self)
        let majority = (self.peers.len() + 1) / 2 + 1;

        // If we reached majority, we win the election
        if self.votes_received.len() >= majority {
            return self.become_leader();
        }

        // Otherwise, still waiting for more votes
        vec![]
    }

    /// Called when this node becomes the leader.
    ///
    /// Responsibilities:
    /// 1. Switch role to Leader
    /// 2. Initialize replication state for all peers
    /// 3. Send initial heartbeats to assert leadership
    fn become_leader(&mut self) -> Vec<Message> {
        // Mark self as leader
        self.role = NodeRole::Leader;
        println!(
            "Node {} became Leader for term {}",
            self.id, self.persistent.current_term
        );

        let last_index = self.last_log_index();

        // Initialize replication tracking for each follower
        for &peer in &self.peers {
            // Next log entry to send to each peer
            self.volatile.next_index.insert(peer, last_index + 1);

            // Highest known replicated log index for each peer
            self.volatile.match_index.insert(peer, 0);
        }

        // Send initial heartbeats to all peers
        self.send_heartbeats()
    }

    /// Sends empty AppendEntries messages (heartbeats)
    ///
    /// Heartbeats are used to:
    /// - Maintain leadership
    /// - Prevent new elections
    /// - Tell followers "I am still alive"
    pub fn send_heartbeats(&self) -> Vec<Message> {
        self.peers
            .iter()
            .map(|&peer| {
                let next = self.volatile.next_index.get(&peer).copied().unwrap_or(1);
                let prev_idx = next.saturating_sub(1);
                let prev_term = self.get_entry(prev_idx).map(|e| e.term).unwrap_or(0);

                Message::AppendEntries {
                    to: peer,
                    args: AppendRequest {
                        term: self.persistent.current_term,
                        leader_id: self.id,

                        // Empty entries = heartbeat (no log replication payload)
                        prev_log_index: prev_idx,
                        prev_log_term: prev_term,
                        entries: vec![],

                        // What the leader has committed so far
                        leader_commit: self.volatile.commit_index,
                    },
                }
            })
            .collect()
    }
    pub fn start_election(&mut self) -> Vec<Message> {
        // become candidate
        self.role = NodeRole::Candidate;

        // increment term
        self.persistent.current_term += 1;

        // vote for self
        self.persistent.voted_for = Some(self.id);

        // reset votes
        self.votes_received.clear();
        self.votes_received.insert(self.id);

        println!(
            "Node {} became Candidate for term {}",
            self.id, self.persistent.current_term
        );

        // send RequestVote to all peers
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftNode;

    fn three_node_cluster() -> (RaftNode, RaftNode, RaftNode) {
        (
            RaftNode::new(1, vec![2, 3]),
            RaftNode::new(2, vec![1, 3]),
            RaftNode::new(3, vec![1, 2]),
        )
    }

    #[test]
    fn election_timeout_makes_candidate() {
        let (mut n1, _, _) = three_node_cluster();
        n1.on_election_timeout();
        assert_eq!(n1.role, NodeRole::Candidate);
        assert_eq!(n1.persistent.current_term, 1);
    }

    #[test]
    fn majority_vote_makes_leader() {
        let (mut n1, mut n2, _) = three_node_cluster();
        let reqs = n1.on_election_timeout();
        let req = match &reqs[0] {
            Message::RequestVote { args, .. } => args.clone(),
            _ => panic!(),
        };
        let resp = n2.handle_request_vote(req);
        let msgs = n1.handle_vote_response(2, resp);
        assert_eq!(n1.role, NodeRole::Leader);
        assert!(!msgs.is_empty());
    }
}
