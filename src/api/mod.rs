use crate::raft::RaftNode;
use crate::raft::log::Command;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub raft: Arc<RwLock<RaftNode>>,
}

use crate::raft::state::NodeRole;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, put},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct PutBody {
    pub value: String,
}

#[derive(Serialize)]
pub struct GetResponse {
    pub key: String,
    pub value: String,
}

async fn handle_put(
    Path(key): Path<String>,
    State(state): State<AppState>,
    Json(body): Json<PutBody>,
) -> StatusCode {
    // Step 1: propose the command, get back the target log index
    let (target_index, mut commit_rx) = {
        let mut node = state.raft.write().await;

        if node.role != NodeRole::Leader {
            // Not the leader — client should retry on another node
            return StatusCode::TEMPORARY_REDIRECT;
        }

        let cmd = Command::Set {
            key,
            value: body.value,
        };
        node.propose(cmd); // appends to log, sends AppendEntries

        let target = node.volatile.last_applied + 1; // our entry's index
        let rx = node.commit_rx.clone();
        (target, rx)
    }; // ← release write lock here so Raft can make progress

    // Step 2: wait until commit_index reaches our entry's index
    loop {
        if *commit_rx.borrow() >= target_index {
            return StatusCode::OK; // committed by majority
        }
        if commit_rx.changed().await.is_err() {
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }
}
async fn handle_get(
    Path(key): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<GetResponse>, StatusCode> {
    let node = state.raft.read().await;
    node.store.get(&key)
        .map(|v| Json(GetResponse {
            key: key.clone(),
            value: v.clone()
        }))
        .ok_or(StatusCode::NOT_FOUND)
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/keys/:key", get(handle_get))
        .route("/v1/keys/:key", put(handle_put))
        .with_state(state)
}