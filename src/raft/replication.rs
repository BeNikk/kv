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
}
