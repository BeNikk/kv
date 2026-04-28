pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
} // A Node can be either a Leader, or a Candidate to be a leader, or a Follower. 

