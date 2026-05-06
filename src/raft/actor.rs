use crate::raft::state::{Message, NodeId};
use crate::raft::{RaftCommand, RaftNode};

use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::rpc::proto::raft_rpc_client::RaftRpcClient;
use crate::rpc::proto::{AppendEntriesArgs, RequestVoteArgs};
use rand::Rng;
use tokio::time::{Duration, Instant, sleep_until};
use tonic::Request;

pub async fn run_raft_actor(
    mut node: RaftNode,
    mut rx: mpsc::Receiver<RaftCommand>,
    peer_addrs: HashMap<NodeId, String>,
    raft_tx: mpsc::Sender<RaftCommand>,
) {
    let mut election_deadline = new_election_deadline();
    let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(100));
    tokio::time::sleep(Duration::from_millis(1000)).await;

    loop {
        tokio::select! {

            // incoming messages
            Some(cmd) = rx.recv() => {
                println!("Node {} role: {:?}", node.id, node.role);

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

                        // reset timer when leader talks
                        election_deadline = new_election_deadline();
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

            // election timeout
            _ = sleep_until(election_deadline) => {
                if node.role != crate::raft::state::NodeRole::Leader {
                    println!("Node {} starting election", node.id);

                    let msgs = node.start_election();

                    election_deadline = new_election_deadline();

                    send_messages(msgs, &peer_addrs, raft_tx.clone()).await;
                }
            }
            _ = heartbeat_interval.tick() => {
                if node.role == crate::raft::state::NodeRole::Leader {
                    let msgs = node.send_heartbeats();
                    send_messages(msgs, &peer_addrs, raft_tx.clone()).await;
                }
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
            // AppendEntries
            Message::AppendEntries { to, args } => {
                let addr = match peer_addrs.get(&to) {
                    Some(a) => a.clone(),
                    None => continue,
                };

                let raft_tx = raft_tx.clone();

                tokio::spawn(async move {
                    match RaftRpcClient::connect(addr.clone()).await {
                        Ok(mut client) => {
                            let sent_count = args.entries.len() as u64;
                            let replicated_match_index = args.prev_log_index + sent_count;
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

                            match client.append_entries(Request::new(req)).await {
                                Ok(resp) => {
                                    let r = resp.into_inner();

                                    let _ = raft_tx
                                        .send(RaftCommand::HandleAppendResponse {
                                            from: to,
                                            success: r.success,
                                            match_index: replicated_match_index,
                                        })
                                        .await;
                                }
                                Err(e) => {
                                    println!("AppendEntries RPC failed to {}: {:?}", addr, e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("Failed to connect to {}: {:?}", addr, e);
                        }
                    }
                });
            }

            //  RequestVote
            Message::RequestVote { to, args } => {
                let addr = match peer_addrs.get(&to) {
                    Some(a) => a.clone(),
                    None => continue,
                };

                let raft_tx = raft_tx.clone();

                tokio::spawn(async move {
                    match RaftRpcClient::connect(addr.clone()).await {
                        Ok(mut client) => {
                            let req = RequestVoteArgs {
                                term: args.term,
                                candidate_id: args.candidate_id,
                                last_log_index: args.last_log_index,
                                last_log_term: args.last_log_term,
                            };

                            match client.request_vote(Request::new(req)).await {
                                Ok(resp) => {
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
                                Err(e) => {
                                    println!("RequestVote RPC failed to {}: {:?}", addr, e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("Failed to connect to {}: {:?}", addr, e);
                        }
                    }
                });
            }
        }
    }
}
fn new_election_deadline() -> Instant {
    let ms = rand::thread_rng().gen_range(300..600);
    Instant::now() + Duration::from_millis(ms)
}
