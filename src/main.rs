mod api;
mod raft;
mod rpc;
mod sim;
mod storage;

#[tokio::main]
async fn main() {
    println!("raft-kv node starting...");
}
