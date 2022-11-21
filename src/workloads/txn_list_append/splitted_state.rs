use futures::{future::try_join_all, FutureExt};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use super::local_state::LocalState;
use super::{
    create_storage_req, gen_next_storage_key, handle_cas_resp, handle_read_resp, handle_write_resp,
    init_node, NodeConfig,
};
use crate::io::{non_blocking::receive_msg, send_msg};
use crate::protocol::{txn_list_append::*, ErrorData};

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

const TIMEOUT: Duration = Duration::from_secs(1);
const RETRY_DELAY: Duration = Duration::from_millis(10);
const ROOT_KEY: &str = "root";
type StorageKey = String;

struct Handler {
    sync_resp: SyncRespHandler,
    config: NodeConfig,
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct RootState {
    map: HashMap<KeyValue, StorageKey>,
}

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
                let res = self.process_txn(txn_data).await;
                let resp_body = match res {
                    Ok(data) => BodyData::TxnOk(data),
                    Err(data) => BodyData::Error(data),
                };
                send_msg(&msg.create_response(resp_body));
            }
            BodyData::CasOk | BodyData::ReadOk { .. } | BodyData::WriteOk | BodyData::Error(..) => {
                self.sync_resp.handle(msg).await;
            }
            _ => eprintln!("Ignoring unexpected message {:?}", msg),
        }
    }

    async fn process_txn(&self, txn_data: &TxnData) -> Result<TxnData, ErrorData> {
        loop {
            let prev_root_key = self.read_root_key().await?;
            let root = match prev_root_key {
                Some(ref key) => self.read_lww_storage(key.clone()).await?,
                None => RootState::default(),
            };
            let mut local_state = self.read_txn_values(&root, txn_data).await?;
            let resp = local_state.apply_txn(txn_data);
            let next_root_key = self.update_state(root, local_state, txn_data).await?;
            if self.cas_root_key(prev_root_key, next_root_key).await? {
                return Ok(resp);
            }
        }
    }

    async fn update_state(
        &self,
        mut state: RootState,
        local_state: LocalState,
        txn_data: &TxnData,
    ) -> Result<String, ErrorData> {
        let updated_keys: HashSet<_> = txn_data
            .txn
            .iter()
            .filter(|&f| matches!(f, TxnFunc::Append { .. }))
            .map(|f| f.key())
            .collect();
        let mut values = local_state.map;
        let storage_map = self
            .write_values(
                updated_keys
                    .iter()
                    .map(|k| (*k, values.remove(k).unwrap()))
                    .collect(),
            )
            .await?;
        state.map.extend(storage_map.into_iter());
        self.write_lww_storage(state).await
    }

    async fn read_root_key(&self) -> Result<Option<String>, ErrorData> {
        let req_msg = self.create_lin_req(BodyData::Read {
            key: json!(ROOT_KEY),
        });
        let maybe_value = handle_read_resp(self.sync_rpc(req_msg).await)?;
        Ok(maybe_value.map(|value| match value {
            Value::String(s) => s,
            other => panic!("Unexpected root data type: {other:?}"),
        }))
    }

    async fn cas_root_key(
        &self,
        prev_key: Option<StorageKey>,
        next_key: StorageKey,
    ) -> Result<bool, ErrorData> {
        let req_msg = self.create_lin_req(BodyData::Cas {
            key: json!(ROOT_KEY),
            from: serde_json::to_value(prev_key).unwrap(),
            to: serde_json::to_value(next_key).unwrap(),
            create_if_not_exists: true,
        });
        handle_cas_resp(self.sync_rpc(req_msg).await)
    }

    async fn read_txn_values(
        &self,
        state: &RootState,
        txn_data: &TxnData,
    ) -> Result<LocalState, ErrorData> {
        let txn_keys: HashSet<_> = txn_data.txn.iter().map(|f| f.key()).collect();
        let storage_keys: HashSet<StorageKey> = txn_keys
            .iter()
            .flat_map(|k| state.map.get(k).cloned())
            .collect();
        let mut values = self.read_values(storage_keys).await?;
        let local_state = LocalState {
            map: txn_keys
                .iter()
                .map(|k| {
                    (
                        *k,
                        state
                            .map
                            .get(k)
                            .map(|s| values.remove(s).unwrap())
                            .unwrap_or_else(|| Vec::new()),
                    )
                })
                .collect(),
        };
        Ok(local_state)
    }

    async fn write_values(
        &self,
        values: HashMap<KeyValue, Vec<ElementValue>>,
    ) -> Result<HashMap<KeyValue, StorageKey>, ErrorData> {
        try_join_all(values.into_iter().map(|(key, value)| {
            self.write_lww_storage(value)
                .map(move |res| res.map(|storage_key| (key, storage_key)))
        }))
        .await
        .map(|prs| prs.into_iter().collect())
    }

    async fn read_values(
        &self,
        keys: HashSet<StorageKey>,
    ) -> Result<HashMap<StorageKey, Vec<ElementValue>>, ErrorData> {
        try_join_all(keys.into_iter().map(|key| {
            self.read_lww_storage(key.clone())
                .map(|res| res.map(|val| (key, val)))
        }))
        .await
        .map(|prs| prs.into_iter().collect())
    }

    async fn write_lww_storage<T: serde::Serialize>(
        &self,
        value: T,
    ) -> Result<StorageKey, ErrorData> {
        let key = gen_next_storage_key(&self.config);
        let req_msg = self.create_lww_req(BodyData::Write {
            key: json!(&key),
            value: serde_json::to_value(value).unwrap(),
        });
        handle_write_resp(self.sync_rpc(req_msg).await)?;
        Ok(key)
    }

    async fn read_lww_storage<T: serde::de::DeserializeOwned>(
        &self,
        key: StorageKey,
    ) -> Result<T, ErrorData> {
        loop {
            let req_msg = self.create_lww_req(BodyData::Read { key: json!(key) });
            match handle_read_resp(self.sync_rpc(req_msg).await)? {
                Some(value) => return Ok(serde_json::from_value(value).unwrap()),
                None => {
                    sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    async fn sync_rpc(&self, msg: Message) -> Option<Message> {
        self.sync_resp.send(msg, TIMEOUT).await
    }

    fn create_lww_req(&self, body: BodyData) -> Message {
        create_storage_req(&self.config.node_id, LWW_KV_SERVICE, body)
    }

    fn create_lin_req(&self, body: BodyData) -> Message {
        create_storage_req(&self.config.node_id, LIN_KV_SERVICE, body)
    }
}
