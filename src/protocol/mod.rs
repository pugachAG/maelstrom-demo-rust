use serde::{Serialize, Deserialize};

pub mod echo;

pub type MessageId = i32;

#[derive(Debug, Serialize, Deserialize)]
pub struct InitData {
    pub msg_id: MessageId,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitOkData {
    pub msg_id: MessageId,
    pub in_reply_to: MessageId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<T> {
    pub src: String,
    pub dest: String,
    pub body: T,
}