pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}