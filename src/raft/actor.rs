use crate::raft::state::{Message, NodeId};
use crate::raft::{RaftCommand, RaftNode};

use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::rpc::proto::raft_rpc_client::RaftRpcClient;
use crate::rpc::proto::{AppendEntriesArgs, RequestVoteArgs};
use tonic::Request;

pub async fn run_raft_actor(
    mut node: RaftNode,
    mut rx: mpsc::Receiver<RaftCommand>,
    peer_addrs: HashMap<NodeId, String>,
    raft_tx: mpsc::Sender<RaftCommand>,
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            RaftCommand::Propose { command, reply } => {
                let msgs = node.propose(command);
                let index = node.last_log_index();

                let _ = reply.send(index);

                send_messages(msgs, &peer_addrs, raft_tx.clone()).await;
            }

            RaftCommand::HandleAppendRequest { args, reply } => {
                let resp = node.handle_append_entries(args);
                let _ = reply.send(resp);
            }

            RaftCommand::HandleVoteRequest { args, reply } => {
                let resp = node.handle_request_vote(args);
                let _ = reply.send(resp);
            }

            RaftCommand::HandleAppendResponse {
                from,
                success,
                match_index,
            } => {
                let msgs = node.handle_append_response(from, success, match_index);
                send_messages(msgs, &peer_addrs, raft_tx.clone()).await;
            }

            RaftCommand::HandleVoteResponse { from, resp } => {
                let msgs = node.handle_vote_response(from, resp);
                send_messages(msgs, &peer_addrs, raft_tx.clone()).await;
            }

            RaftCommand::GetRole { reply } => {
                let _ = reply.send(node.role.clone());
            }

            RaftCommand::GetValue { key, reply } => {
                let _ = reply.send(node.store.get(&key).cloned());
            }
        }
    }
}

async fn send_messages(
    msgs: Vec<Message>,
    peer_addrs: &HashMap<NodeId, String>,
    raft_tx: mpsc::Sender<RaftCommand>,
) {
    for msg in msgs {
        match msg {
            Message::AppendEntries { to, args } => {
                let addr = match peer_addrs.get(&to) {
                    Some(a) => a.clone(),
                    None => continue,
                };

                let raft_tx = raft_tx.clone();

                tokio::spawn(async move {
                    if let Ok(mut client) = RaftRpcClient::connect(addr).await {
                        let req = AppendEntriesArgs {
                            term: args.term,
                            leader_id: args.leader_id,
                            prev_log_index: args.prev_log_index,
                            prev_log_term: args.prev_log_term,
                            entries: args
                                .entries
                                .into_iter()
                                .map(|e| crate::rpc::proto::LogEntryProto {
                                    term: e.term,
                                    index: e.index,
                                    command: serde_json::to_vec(&e.command).unwrap(),
                                })
                                .collect(),
                            leader_commit: args.leader_commit,
                        };

                        if let Ok(resp) = client.append_entries(Request::new(req)).await {
                            let r = resp.into_inner();

                            let _ = raft_tx
                                .send(RaftCommand::HandleAppendResponse {
                                    from: args.leader_id, // simplification
                                    success: r.success,
                                    match_index: args.prev_log_index + 1, // simplification
                                })
                                .await;
                        }
                    }
                });
            }

            Message::RequestVote { to, args } => {
                let addr = match peer_addrs.get(&to) {
                    Some(a) => a.clone(),
                    None => continue,
                };

                let raft_tx = raft_tx.clone();

                tokio::spawn(async move {
                    if let Ok(mut client) = RaftRpcClient::connect(addr).await {
                        let req = RequestVoteArgs {
                            term: args.term,
                            candidate_id: args.candidate_id,
                            last_log_index: args.last_log_index,
                            last_log_term: args.last_log_term,
                        };

                        if let Ok(resp) = client.request_vote(Request::new(req)).await {
                            let r = resp.into_inner();

                            let _ = raft_tx
                                .send(RaftCommand::HandleVoteResponse {
                                    from: to,
                                    resp: crate::raft::state::VoteResponse {
                                        term: r.term,
                                        vote_granted: r.vote_granted,
                                    },
                                })
                                .await;
                        }
                    }
                });
            }
        }
    }
}
