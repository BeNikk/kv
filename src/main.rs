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
    // Read node ID from CLI
    let args: Vec<String> = std::env::args().collect();
    let id: u64 = args
        .get(1)
        .expect("provide node id")
        .parse()
        .expect("invalid node id");

    println!("starting node {}", id);

    // Ports per node
    let http_port = 3000 + id;
    let grpc_port = 4000 + id;

    // Define cluster (3 nodes for now, testing)
    let all_nodes = vec![1, 2, 3];

    let peers: Vec<u64> = all_nodes.iter().copied().filter(|&p| p != id).collect();

    let node = RaftNode::new(id, peers);

    let commit_rx = node.commit_rx.clone();

    // Peer gRPC addresses
    let mut peer_addrs = HashMap::new();
    for peer_id in all_nodes {
        if peer_id != id {
            peer_addrs.insert(peer_id, format!("http://localhost:{}", 4000 + peer_id));
            println!("Node {} peers: {:?}", id, peer_addrs);
        }
    }

    // Channel for actor
    let (tx, rx) = mpsc::channel(64);
    let actor_tx = tx.clone();

    // Start Raft actor
    tokio::spawn(async move {
        raft::actor::run_raft_actor(node, rx, peer_addrs, actor_tx).await;
    });

    // Start gRPC server
    let raft_service = RaftService {
        raft_tx: tx.clone(),
    };

    tokio::spawn(async move {
        println!("node {} gRPC on {}", id, grpc_port);

        tonic::transport::Server::builder()
            .add_service(RaftRpcServer::new(raft_service))
            .serve(format!("0.0.0.0:{}", grpc_port).parse().unwrap())
            .await
            .unwrap();
    });

    // Start HTTP server
    let state = api::AppState {
        raft_tx: tx,
        commit_rx,
    };

    let app = api::router(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", http_port))
        .await
        .unwrap();

    println!("node {} HTTP on {}", id, http_port);

    axum::serve(listener, app).await.unwrap();
}
