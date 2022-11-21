use std::sync::atomic::{AtomicU32, Ordering};

use crate::io::{non_blocking::receive_msg, send_msg};
use crate::protocol::{txn_list_append::*, gen_next_msg_id, Body, ErrorCode, ErrorData, NodeId};

use serde_json::Value;

pub mod local_state;
pub mod shared_state;
pub mod single_node;
pub mod splitted_state;

pub struct NodeConfig {
    pub node_id: NodeId,
}

pub async fn init_node() -> NodeConfig {
    let msg: Message = receive_msg().await;
    match msg.body.data {
        BodyData::Init(ref data) => {
            eprintln!("Received init msg: {data:?}");
            send_msg(&msg.create_response(BodyData::InitOk));
            NodeConfig {
                node_id: data.node_id.clone(),
            }
        }
        _ => panic!("Expected init msg, got {:?}", msg),
    }
}

pub fn gen_next_storage_key(config: &NodeConfig) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    format!(
        "{}-{}",
        config.node_id,
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

pub fn create_storage_req(node_id: &NodeId, storage: &str, data: BodyData) -> Message {
    Message {
        src: node_id.clone(),
        dest: storage.to_string(),
        body: Body {
            msg_id: Some(gen_next_msg_id()),
            in_reply_to: None,
            data,
        },
    }
}

pub fn handle_read_resp(resp: Option<Message>) -> Result<Option<Value>, ErrorData> {
    handle_timeout(resp, "read").map(|resp_msg| match resp_msg.body.data {
        BodyData::ReadOk { value } => Some(value),
        BodyData::Error(ErrorData {
            code: ErrorCode::KeyDoesNotExist,
            ..
        }) => None,
        BodyData::Error(data) => handle_unexpected_error(data),
        other => handle_unexpected_resp(other),
    })
}

pub fn handle_write_resp(resp: Option<Message>) -> Result<(), ErrorData> {
    handle_timeout(resp, "write").map(|resp_msg| match resp_msg.body.data {
        BodyData::WriteOk => (),
        BodyData::Error(data) => handle_unexpected_error(data),
        other => handle_unexpected_resp(other),
    })
}

pub fn handle_cas_resp(resp: Option<Message>) -> Result<bool, ErrorData> {
    handle_timeout(resp, "cas").map(|resp_msg| match resp_msg.body.data {
        BodyData::CasOk => true,
        BodyData::Error(ErrorData {
            code: ErrorCode::PreconditionFailed,
            ..
        }) => false,
        BodyData::Error(data) => handle_unexpected_error(data),
        other => handle_unexpected_resp(other),
    })
}

fn handle_unexpected_error<T>(data: ErrorData) -> T {
    panic!(
        "Failed with unexpected error code {:?}: {}",
        data.code, data.text
    )
}

fn handle_unexpected_resp<T>(body: BodyData) -> T {
    panic!("Invalid response: {body:?}");
}

fn handle_timeout(resp: Option<Message>, operation: &str) -> Result<Message, ErrorData> {
    resp.ok_or_else(|| {
        ErrorData::new(
            format!("Storage {operation} timeout"),
            ErrorCode::TemporarilyUnavailable,
        )
    })
}
