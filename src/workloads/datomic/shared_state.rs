use serde_json::{json, Value};
use std::sync::Arc;
use tokio::time::Duration;

use super::local_state::LocalState;
use super::{init_node, NodeConfig};
use crate::io::{non_blocking::receive_msg, send_msg};
use crate::protocol::{datomic::*, gen_next_msg_id, Body, ErrorCode, ErrorData};

type SyncRespHandler = crate::io::sync_resp::SyncRespHandler<BodyData>;

pub fn run() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main());
}

async fn main() {
    let config = init_node().await;
    let handler = Arc::new(Handler::new(config));
    loop {
        let msg: Message = receive_msg().await;
        let handler = handler.clone();
        tokio::spawn(async move { handler.handle_msg(msg).await });
    }
}

struct Handler {
    sync_resp: SyncRespHandler,
    config: NodeConfig,
}

const TIMEOUT: Duration = Duration::from_secs(1);
const ROOT_KEY: &str = "root";

impl Handler {
    fn new(config: NodeConfig) -> Self {
        Self {
            sync_resp: SyncRespHandler::new(),
            config,
        }
    }

    async fn handle_msg(&self, msg: Message) {
        match msg.body.data {
            BodyData::Txn(ref txn_data) => {
                let res = self.handle_txn(txn_data).await;
                let resp_body = match res {
                    Ok(data) => BodyData::TxnOk(data),
                    Err(data) => BodyData::Error(data),
                };
                send_msg(&msg.create_response(resp_body));
            }
            BodyData::CasOk | BodyData::ReadOk { value: _ } | BodyData::Error(_) => {
                self.sync_resp.handle(msg).await;
            }
            _ => eprintln!("Ignoring unexpected message {:?}", msg),
        }
    }

    async fn handle_txn(&self, txn_data: &TxnData) -> Result<TxnData, ErrorData> {
        let prev_value = self.read_state().await?;
        let mut state: LocalState = prev_value
            .as_ref()
            .map(|st| serde_json::from_value(st.clone()).unwrap())
            .unwrap_or_else(|| LocalState::default());
        let res_data = state.apply_txn(txn_data);
        let next_value = serde_json::to_value(state).unwrap();
        self.update_state(prev_value, next_value).await?;
        Ok(res_data)
    }

    async fn update_state(&self, prev: Option<Value>, updated: Value) -> Result<(), ErrorData> {
        let resp = self
            .sync_resp
            .send(
                self.create_storage_request(BodyData::Cas {
                    key: json!(ROOT_KEY),
                    from: prev.unwrap_or(Value::Null),
                    to: updated,
                    create_if_not_exists: true,
                }),
                TIMEOUT,
            )
            .await;
        match resp {
            Some(msg) => match msg.body.data {
                BodyData::CasOk => Ok(()),
                BodyData::Error(ErrorData { text, code }) => {
                    let data = match code {
                        ErrorCode::PreconditionFailed => ErrorData::new(
                            "Aborted due to concurrent transaction".to_owned(),
                            ErrorCode::TxnConflict,
                        ),
                        _ => ErrorData::new(text, ErrorCode::Abort),
                    };
                    Err(data)
                }
                other => panic!("Expected cas_ok or error response, got {other:?}"),
            },
            None => Err(ErrorData::new(
                "Timeout while saving the updated state".to_owned(),
                ErrorCode::Crash,
            )),
        }
    }

    async fn read_state(&self) -> Result<Option<Value>, ErrorData> {
        let resp = self
            .sync_resp
            .send(
                self.create_storage_request(BodyData::Read {
                    key: json!(ROOT_KEY),
                }),
                TIMEOUT,
            )
            .await;
        match resp {
            Some(msg) => match msg.body.data {
                BodyData::ReadOk { value } => Ok(Some(value)),
                BodyData::Error(ErrorData {
                    text: _,
                    code: ErrorCode::KeyDoesNotExist,
                }) => Ok(None),
                other => panic!("Expected read_ok response, got {other:?}"),
            },
            None => Err(ErrorData::new("Timeout reading state".to_owned(), ErrorCode::Abort)),
        }
    }

    fn create_storage_request(&self, data: BodyData) -> Message {
        Message {
            src: self.config.node_id.clone(),
            dest: LIN_KV_SERVICE.to_string(),
            body: Body {
                msg_id: Some(gen_next_msg_id()),
                in_reply_to: None,
                data,
            },
        }
    }
}
