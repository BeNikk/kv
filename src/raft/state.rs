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