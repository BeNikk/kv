use crate::raft::log::Command;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct KvStore {
    data: HashMap<String, String>,
    pub last_applied: u64,
}

impl KvStore {
    pub fn apply(&mut self, index: u64, command: &Command) {
        match command {
            Command::Set { key, value } => {
                self.data.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                self.data.remove(key);
            }
            Command::NoOp => {}
        }
        self.last_applied = index;
    }
    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }
}
