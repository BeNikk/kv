mod api;
mod raft;
mod rpc;
mod sim;
mod storage;

use crate::rpc::proto::raft_rpc_server::RaftRpcServer;
use crate::rpc::server::RaftService;
use raft::RaftNode;

use std::collections::HashMap;
use tokio::sync::mpsc;

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

    let (tx, rx) = mpsc::channel(64);

    tokio::spawn(raft::actor::run_raft_actor(node, rx, peer_addrs));

    let raft_service = RaftService {
        raft_tx: tx.clone(),
    };

    tokio::spawn(async move {
        println!("gRPC server running on port 4001");

        tonic::transport::Server::builder()
            .add_service(RaftRpcServer::new(raft_service))
            .serve("0.0.0.0:4001".parse().unwrap())
            .await
            .unwrap();
    });

    let state = api::AppState {
        raft_tx: tx,
        commit_rx,
    };

    let app = api::router(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    println!("HTTP server running on port 3000");

    axum::serve(listener, app).await.unwrap();
}
