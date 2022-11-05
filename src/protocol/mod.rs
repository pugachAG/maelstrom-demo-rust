use std::sync::atomic::{Ordering, AtomicU64};

use serde::{Deserialize, Serialize};

pub mod echo;
pub mod broadcast;
pub mod crdts;

pub type MessageId = u64;
pub type NodeId = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct Body<T> {
    pub msg_id: MessageId,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct InitOkData {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<T> {
    pub src: String,
    pub dest: String,
    pub body: T,
}

impl<T> Message<Body<T>> {
    pub fn create_response<D>(&self, data: D) -> Message<Body<D>> {
        Message {
            src: self.dest.clone(),
            dest: self.src.clone(),
            body: Body {
                data,
                msg_id: gen_next_msg_id(),
                in_reply_to: Some(self.body.msg_id),
            },
        }
    }
}

pub fn gen_next_msg_id() -> MessageId {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
