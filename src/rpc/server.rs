use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use tonic::{Request, Response, Status};

use crate::raft::RaftCommand;
use crate::raft::state::{AppendRequest, AppendResponse, VoteRequest, VoteResponse};

use crate::rpc::proto::raft_rpc_server::{RaftRpc, RaftRpcServer};
use crate::rpc::proto::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::{RequestVoteArgs, RequestVoteReply};

#[derive(Clone)]
pub struct RaftService {
    pub raft_tx: mpsc::Sender<RaftCommand>,
}

#[tonic::async_trait]
impl RaftRpc for RaftService {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let req = request.into_inner();

        // convert proto → internal
        let args = AppendRequest {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: req
                .entries
                .into_iter()
                .map(|e| crate::raft::log::LogEntry {
                    term: e.term,
                    index: e.index,
                    command: serde_json::from_slice(&e.command).unwrap(),
                })
                .collect(),
            leader_commit: req.leader_commit,
        };

        // send to actor
        let (tx, rx) = oneshot::channel();

        self.raft_tx
            .send(RaftCommand::HandleAppendRequest { args, reply: tx })
            .await
            .map_err(|_| Status::internal("actor down"))?;

        // wait for response
        let resp: AppendResponse = rx.await.map_err(|_| Status::internal("actor dropped"))?;

        // convert internal → proto
        let reply = AppendEntriesReply {
            term: resp.term,
            success: resp.success,
        };

        Ok(Response::new(reply))
    }

    async fn request_vote(
        &self,
        request: tonic::Request<RequestVoteArgs>,
    ) -> Result<tonic::Response<RequestVoteReply>, tonic::Status> {
        let req = request.into_inner();

        // convert proto → internal
        let args = VoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        };

        // send to actor
        let (tx, rx) = oneshot::channel();

        self.raft_tx
            .send(RaftCommand::HandleVoteRequest { args, reply: tx })
            .await
            .map_err(|_| tonic::Status::internal("actor down"))?;

        // wait for response
        let resp: VoteResponse = rx
            .await
            .map_err(|_| tonic::Status::internal("actor dropped"))?;

        // convert internal → proto
        let reply = RequestVoteReply {
            term: resp.term,
            vote_granted: resp.vote_granted,
        };

        Ok(tonic::Response::new(reply))
    }
}
