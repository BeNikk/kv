pub mod proto {
    tonic::include_proto!("raft");
}
pub mod server;
pub use proto::raft_rpc_client::RaftRpcClient;
pub use proto::raft_rpc_server::{RaftRpc, RaftRpcServer};
pub use proto::*;
