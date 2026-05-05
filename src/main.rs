mod api;
mod raft;
mod rpc;
mod sim;
mod storage;

use crate::rpc::proto::raft_rpc_server::RaftRpcServer;
use crate::rpc::server::RaftService;
use raft::RaftNode;
use std::collections::HashMap;

#[tokio::main]

async fn main() {
    println!("raft-kv node starting...");

    let node = RaftNode::new(1, vec![2, 3, 4, 5]);

    let commit_rx = node.commit_rx.clone();

    let peer_addrs = HashMap::from([
        (2u64, "http://localhost:4002".to_string()),
        (3u64, "http://localhost:4003".to_string()),
        (4u64, "http://localhost:4004".to_string()),
        (5u64, "http://localhost:4005".to_string()),
    ]);

    // 4. Create channel to talk to Raft actor
    let (tx, rx) = tokio::sync::mpsc::channel(64);

    // 5. Start Raft actor (brain)
    tokio::spawn(raft::actor::run_raft_actor(node, rx, peer_addrs));

    // 6. Start HTTP API
    let state = api::AppState {
        raft_tx: tx,
        commit_rx,
    };

    let app = api::router(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    println!("HTTP server running on port 3000");

    axum::serve(listener, app).await.unwrap();
}
