use crate::raft::RaftNode;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub raft: Arc<RwLock<RaftNode>>,
}
