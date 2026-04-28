mod raft;
mod storage;
mod rpc;
mod api;
mod sim;

#[tokio::main]
async fn main() {
    println!("raft-kv node starting...");
}