use crate::raft::RaftNode;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
    NoOp,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub term: u64,        // era of leadership
    pub index: u64,       // position of the command
    pub command: Command, // command which will run
}

impl RaftNode {
    pub fn last_log_index(&self) -> u64 {
        self.persistent.log.len() as u64
    }
    pub fn last_log_term(&self) -> u64 {
        self.persistent.log.last().map(|e| e.term).unwrap_or(0)
    }
    pub fn get_entry(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.persistent.log.get((index - 1) as usize)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftNode;

    #[test]
    fn empty_log_returns_zero() {
        let node = RaftNode::new(1, vec![2, 3]);
        assert_eq!(node.last_log_index(), 0);
        assert_eq!(node.last_log_term(), 0);
    }
}
