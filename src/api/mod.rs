use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, put},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};

use crate::raft::RaftCommand;
use crate::raft::log::Command;
use crate::raft::state::NodeRole;

#[derive(Clone)]
pub struct AppState {
    pub raft_tx: mpsc::Sender<RaftCommand>,
    pub commit_rx: watch::Receiver<u64>,
}

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
    // Step 1: check if this node is leader
    let (role_tx, role_rx) = oneshot::channel();
    if state
        .raft_tx
        .send(RaftCommand::GetRole { reply: role_tx })
        .await
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    let role = match role_rx.await {
        Ok(r) => r,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    if role != NodeRole::Leader {
        return StatusCode::TEMPORARY_REDIRECT;
    }

    // Step 2: propose command
    let (reply_tx, reply_rx) = oneshot::channel();
    if state
        .raft_tx
        .send(RaftCommand::Propose {
            command: Command::Set {
                key,
                value: body.value,
            },
            reply: reply_tx,
        })
        .await
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    // Step 3: get log index
    let target_index = match reply_rx.await {
        Ok(idx) => idx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Step 4: wait for commit
    let mut commit_rx = state.commit_rx.clone();

    loop {
        if *commit_rx.borrow() >= target_index {
            return StatusCode::OK;
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
    let (reply_tx, reply_rx) = oneshot::channel();

    if state
        .raft_tx
        .send(RaftCommand::GetValue {
            key: key.clone(),
            reply: reply_tx,
        })
        .await
        .is_err()
    {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    match reply_rx.await {
        Ok(Some(value)) => Ok(Json(GetResponse { key, value })),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/keys/:key", get(handle_get))
        .route("/v1/keys/:key", put(handle_put))
        .with_state(state)
}
