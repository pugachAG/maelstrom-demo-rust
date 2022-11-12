use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use crate::{
    io::{non_blocking::receive_msg, send_msg},
    protocol::{
        crdts::{CommonBodyData, CrdtBody},
        gen_next_msg_id, Body, NodeId,
    },
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::time::{sleep, Duration};

pub mod g_set;
pub mod g_counter;
pub mod pn_counter;

pub trait Crdt {
    type Body: Serialize + DeserializeOwned + Debug;
    type State: Serialize + DeserializeOwned + Debug;

    fn handle_msg(&mut self, body: &Self::Body) -> Option<Self::Body>;
    fn update(&mut self, state: &Self::State);
    fn get_state(&self) -> Self::State;
    fn init(&mut self, _node_id: &NodeId) {}
}

pub fn run<C: Crdt + Send + 'static>(crdt: C) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            CrdtNode::run(crdt).await;
        });
}

#[derive(Debug)]
struct NodeConfig {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
}

type Message<C> = crate::protocol::crdts::Message<<C as Crdt>::Body, <C as Crdt>::State>;

struct CrdtNode<C: Crdt> {
    config: NodeConfig,
    crdt: Arc<Mutex<C>>,
}

impl<C: Crdt + Send + 'static> CrdtNode<C> {
    pub async fn run(mut crdt: C) {
        let config = Self::init_node().await;
        eprintln!("Node init done: {:?}", config);
        crdt.init(&config.node_id);
        let node = CrdtNode {
            config,
            crdt: Arc::new(Mutex::new(crdt)),
        };
        node.start_replication();
        loop {
            node.handle_next_msg().await;
        }
    }

    async fn handle_next_msg(&self) {
        let msg: Message<C> = receive_msg().await;
        match msg.body.data {
            CrdtBody::Common(CommonBodyData::Replicate { ref state }) => {
                self.crdt.lock().unwrap().update(state);
            }
            CrdtBody::Custom(ref body) => {
                let resp_body = { self.crdt.lock().unwrap().handle_msg(body) };
                match resp_body {
                    Some(resp_body) => {
                        let resp: Message<C> = msg.create_response(CrdtBody::Custom(resp_body));
                        send_msg(&resp);
                    }
                    None => {
                        eprintln!("No response to {:?}", msg);
                    }
                }
            }
            _ => eprintln!("Ignoring unexpected message {:?}", msg),
        }
    }

    fn start_replication(&self) {
        let node_id = self.config.node_id.clone();
        let neighbours: Vec<_> = self
            .config
            .node_ids
            .iter()
            .cloned()
            .filter(|id| *id != node_id)
            .collect();
        let crdt = self.crdt.clone();
        tokio::spawn(async move {
            eprintln!("Starting replication for node {node_id}");
            loop {
                for neighbour in &neighbours {
                    Self::replicate_state(&node_id, neighbour, &crdt);
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
    }

    fn replicate_state(src: &String, dest: &String, crdt: &Mutex<C>) {
        let state = { crdt.lock().unwrap().get_state() };
        let msg = Message::<C> {
            src: src.clone(),
            dest: dest.clone(),
            body: Body {
                msg_id: Some(gen_next_msg_id()),
                in_reply_to: None,
                data: CrdtBody::Common(CommonBodyData::Replicate { state }),
            },
        };
        send_msg(&msg);
    }

    async fn init_node() -> NodeConfig {
        let init_msg: Message<C> = receive_msg().await;
        match init_msg.body.data {
            CrdtBody::Common(CommonBodyData::Init(ref data)) => {
                let resp: Message<C> =
                    init_msg.create_response(CrdtBody::Common(CommonBodyData::InitOk));
                send_msg(&resp);
                NodeConfig {
                    node_id: data.node_id.clone(),
                    node_ids: data.node_ids.iter().cloned().collect(),
                }
            }
            _ => panic!("Expected init msg, got {:?}", init_msg),
        }
    }
}
