use crate::raft::state::{Message, NodeId};
use crate::raft::{RaftCommand, RaftNode};
use std::collections::HashMap;
use tokio::sync::mpsc;

pub async fn run_raft_actor(
    mut node: RaftNode,
    mut rx: mpsc::Receiver<RaftCommand>,
    peer_addrs: HashMap<NodeId, String>, // ← NEW
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            RaftCommand::Propose { command, reply } => {
                let msgs = node.propose(command);
                let index = node.last_log_index();

                let _ = reply.send(index);

                send_messages(&msgs, &peer_addrs).await;
            }
            RaftCommand::HandleAppendRequest { args, reply } => {
                let resp = node.handle_append_entries(args);
                let _ = reply.send(resp);
            }
            RaftCommand::GetRole { reply } => {
                let _ = reply.send(node.role.clone());
            }

            RaftCommand::GetValue { key, reply } => {
                let _ = reply.send(node.store.get(&key).cloned());
            }
            RaftCommand::HandleVoteRequest { args, reply } => {
                let resp = node.handle_request_vote(args);
                let _ = reply.send(resp);
            }
        }
    }
}
async fn send_messages(msgs: &[Message], peer_addrs: &HashMap<NodeId, String>) {
    for msg in msgs {
        match msg {
            Message::AppendEntries { to, args } => {
                println!(
                    "Sending AppendEntries → node {} at {:?} | entries: {}",
                    to,
                    peer_addrs.get(to),
                    args.entries.len()
                );
            }

            Message::RequestVote { to, args } => {
                println!(
                    "Sending RequestVote → node {} at {:?}",
                    to,
                    peer_addrs.get(to)
                );
            }
        }
    }
}
