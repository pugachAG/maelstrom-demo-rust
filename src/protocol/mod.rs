use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

pub mod echo;

pub type MessageId = usize;

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestBody<T> {
    pub msg_id: MessageId,
    #[serde(flatten)]
    pub data: T,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseBody<T> {
    pub msg_id: MessageId,
    pub in_reply_to: MessageId,
    #[serde(flatten)]
    pub data: T,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitData {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitOkData {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<T> {
    pub src: String,
    pub dest: String,
    pub body: T,
}

impl<T> Message<RequestBody<T>> {
    pub fn create_response<D>(&self, data: D) -> Message<ResponseBody<D>> {
        Message {
            src: self.dest.clone(),
            dest: self.src.clone(),
            body: ResponseBody {
                data,
                msg_id: gen_next_msg_id(),
                in_reply_to: self.body.msg_id,
            },
        }
    }
}

pub fn gen_next_msg_id() -> MessageId {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
