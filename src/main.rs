mod api;
mod raft;
mod rpc;
mod sim;
mod storage;
use raft::start_election;

#[tokio::main]
async fn main() {
    println!("raft-kv node starting...");
    start_election();
}
