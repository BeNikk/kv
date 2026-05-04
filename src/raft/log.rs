#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
    NoOp,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub term: u64,        // era of leadership
    pub index: u64,       // position of the command
    pub command: Command, // command which will run
}
