pub type NodeId = u64;
use crate::raft::log::LogEntry;
#[derive(Debug, Clone, PartialEq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
} // A Node can be either a Leader, or a Candidate to be a leader, or a Follower. 

// this is for persisting state of a node, in case of a failure, we can restart from here.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
}

impl PersistentState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![],
        }
    }
    pub fn save(&self, path: &str) -> std::io::Result<()> {
        let json = serde_json::to_string(self)?;
        let tmp = format!("{}.tmp", path);
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn load(path: &str) -> std::io::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(json) => Ok(serde_json::from_str(&json)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new()),
            Err(e) => Err(e),
        }
    }
}
// this is for the volatile state of a node, it is not persisted, it is reset when the node starts.
#[derive(Debug)]
pub struct VolatileState {
    pub commit_index: u64,
    pub last_applied: u64,
    pub next_index: std::collections::HashMap<NodeId, u64>,
    pub match_index: std::collections::HashMap<NodeId, u64>,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
            next_index: std::collections::HashMap::new(),
            match_index: std::collections::HashMap::new(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone)]
pub struct AppendResponse {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub enum Message {
    RequestVote { to: NodeId, args: VoteRequest },
    AppendEntries { to: NodeId, args: AppendRequest },
}
