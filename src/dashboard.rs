use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeStatus {
    pub id: u64,
    pub role: String,
    pub term: u64,
    pub log_length: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub alive: bool,
}

pub async fn broadcast_status(status: NodeStatus, dashboard_port: u16) {
    use tokio::net::UdpSocket;

    if let Ok(sock) = UdpSocket::bind("0.0.0.0:0").await {
        let addr = format!("127.0.0.1:{}", dashboard_port);
        if let Ok(json) = serde_json::to_vec(&status) {
            let _ = sock.send_to(&json, addr).await;
        }
    }
}
