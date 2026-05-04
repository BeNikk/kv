pub type NodeId = u64;

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
    pub voted_for:    Option<NodeId>,
    pub log:          Vec<LogEntry>,
}

impl PersistentState {
    pub fn new() -> Self {
        Self { current_term: 0, voted_for: None, log: vec![] }
    }
}
// this is for the volatile state of a node, it is not persisted, it is reset when the node starts. 
#[derive(Debug)]
pub struct VolatileState {
    pub commit_index: u64,
    pub last_applied: u64,
    pub next_index:   std::collections::HashMap<NodeId, u64>,
    pub match_index:  std::collections::HashMap<NodeId, u64>,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: 0, last_applied: 0,
            next_index: std::collections::HashMap::new(),
            match_index: std::collections::HashMap::new(),
        }
    }
}