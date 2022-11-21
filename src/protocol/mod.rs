use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

pub mod broadcast;
pub mod crdts;
pub mod txn_list_append;
pub mod echo;
pub mod link_kv;

pub type MessageId = u64;
pub type NodeId = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct Body<T> {
    pub msg_id: Option<MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<MessageId>,
    #[serde(flatten)]
    pub data: T,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitData {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
}

#[derive(Debug, PartialEq, Eq, serde_repr::Serialize_repr, serde_repr::Deserialize_repr)]
#[repr(u8)]
pub enum ErrorCode {
    Timeout = 0,
    TemporarilyUnavailable = 11,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorData {
    pub text: String,
    pub code: ErrorCode,
}

impl ErrorData {
    pub fn new(text: String, code: ErrorCode) -> Self {
        Self { text, code }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitOkData {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<T> {
    pub src: String,
    pub dest: String,
    pub body: Body<T>,
}

impl<T> Message<T> {
    pub fn create_response<R>(&self, data: R) -> Message<R> {
        Message {
            src: self.dest.clone(),
            dest: self.src.clone(),
            body: Body {
                data,
                msg_id: Some(gen_next_msg_id()),
                in_reply_to: self.body.msg_id,
            },
        }
    }
}

pub fn gen_next_msg_id() -> MessageId {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
