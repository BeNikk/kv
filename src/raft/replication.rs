use crate::raft::log::Command;
use crate::raft::log::LogEntry;
use crate::raft::{RaftNode, state::*};

impl RaftNode {
    pub fn propose(&mut self, command: Command) -> Vec<Message> {
        assert_eq!(self.role, NodeRole::Leader);
        let entry = LogEntry {
            term: self.persistent.current_term,
            index: self.last_log_index() + 1,
            command,
        };
        self.persistent.log.push(entry);
        self.replicate_to_peers()
    }

    fn replicate_to_peers(&self) -> Vec<Message> {
        self.peers
            .iter()
            .map(|&peer| {
                let next = self.volatile.next_index.get(&peer).copied().unwrap_or(1);
                let prev_idx = next.saturating_sub(1);
                let prev_term = self.get_entry(prev_idx).map(|e| e.term).unwrap_or(0);
                let entries = self
                    .persistent
                    .log
                    .iter()
                    .filter(|e| e.index >= next)
                    .cloned()
                    .collect();
                Message::AppendEntries {
                    to: peer,
                    args: AppendRequest {
                        term: self.persistent.current_term,
                        leader_id: self.id,
                        prev_log_index: prev_idx,
                        prev_log_term: prev_term,
                        entries,
                        leader_commit: self.volatile.commit_index,
                    },
                }
            })
            .collect()
    }
    pub fn handle_append_entries(&mut self, args: AppendRequest) -> AppendResponse {
        // Rule 1: stale leader
        if args.term < self.persistent.current_term {
            return AppendResponse {
                term: self.persistent.current_term,
                success: false,
            };
        }
        self.become_follower(args.term);

        // Rule 2: consistency check
        if args.prev_log_index > 0 {
            match self.get_entry(args.prev_log_index) {
                None => {
                    return AppendResponse {
                        term: self.persistent.current_term,
                        success: false,
                    };
                }
                Some(e) if e.term != args.prev_log_term => {
                    self.persistent
                        .log
                        .truncate((args.prev_log_index - 1) as usize);
                    return AppendResponse {
                        term: self.persistent.current_term,
                        success: false,
                    };
                }
                _ => {}
            }
        }
        // Rule 3: append entries
        for entry in args.entries {
            let idx = entry.index as usize;
            if idx <= self.persistent.log.len() {
                self.persistent.log[idx - 1] = entry;
                self.persistent.log.truncate(idx);
            } else {
                self.persistent.log.push(entry);
            }
        }
        // Rule 4: advance commit index
        if args.leader_commit > self.volatile.commit_index {
            self.volatile.commit_index = args.leader_commit.min(self.last_log_index());
        }
        self.apply_committed_entries();

        AppendResponse {
            term: self.persistent.current_term,
            success: true,
        }
    }
    pub fn handle_append_response(
        &mut self,
        from: NodeId,
        success: bool,
        match_idx: u64,
    ) -> Vec<Message> {
        if self.role != NodeRole::Leader {
            return vec![];
        }
        if success {
            self.volatile.match_index.insert(from, match_idx);
            self.volatile.next_index.insert(from, match_idx + 1);
            self.try_advance_commit_index();
            self.apply_committed_entries();
            vec![]
        } else {
            let next = self.volatile.next_index.get(&from).copied().unwrap_or(1);
            self.volatile
                .next_index
                .insert(from, next.saturating_sub(1).max(1));
            self.replicate_to_peers()
        }
    }

    fn try_advance_commit_index(&mut self) {
        let mut indices: Vec<u64> = self.volatile.match_index.values().copied().collect();
        indices.push(self.last_log_index());
        indices.sort_unstable();
        let majority = (self.peers.len() + 1) / 2;
        let n = indices[indices.len() - 1 - majority];
        if n > self.volatile.commit_index {
            if let Some(e) = self.get_entry(n) {
                if e.term == self.persistent.current_term {
                    self.volatile.commit_index = n;
                }
            }
        }
    }
    /// Drain all committed-but-not-yet-applied log entries
    /// into the KV state machine. Call after any commit_index change.
    pub fn apply_committed_entries(&mut self) {
        while self.volatile.last_applied < self.volatile.commit_index {
            let next = self.volatile.last_applied + 1;
            if let Some(entry) = self.get_entry(next) {
                let cmd = entry.command.clone();
                self.store.apply(next, &cmd);
                self.volatile.last_applied = next;
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{RaftNode, state::*};

    fn make_leader(id: NodeId, peers: Vec<NodeId>) -> RaftNode {
        let mut n = RaftNode::new(id, peers);
        n.persistent.current_term = 1;
        n.role = NodeRole::Leader;
        for &p in &n.peers.clone() {
            n.volatile.next_index.insert(p, 1);
            n.volatile.match_index.insert(p, 0);
        }
        n
    }

    #[test]
    fn propose_appends_to_log() {
        let mut leader = make_leader(1, vec![2, 3]);
        leader.propose(Command::Set {
            key: "x".into(),
            value: "42".into(),
        });
        assert_eq!(leader.last_log_index(), 1);
    }

    #[test]
    fn commit_advances_after_majority_ack() {
        let mut leader = make_leader(1, vec![2, 3]);
        leader.propose(Command::Set {
            key: "k".into(),
            value: "v".into(),
        });
        leader.handle_append_response(2, true, 1);
        assert_eq!(leader.volatile.commit_index, 1);
    }
}
