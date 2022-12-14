use std::collections::{HashMap, HashSet};
use tokio::sync::oneshot;

use crate::{
    io::{non_blocking::receive_msg, send_msg},
    protocol::{broadcast::*, gen_next_msg_id, Body, MessageId, NodeId},
};

pub fn run() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main());
}

struct NodeConfig {
    node_id: NodeId,
    neighbours: Vec<NodeId>,
}

async fn main() {
    eprintln!("Running broadcast workload");
    let config = init_node().await;
    let mut values = HashSet::<BroadcastValue>::new();
    let mut pending_ack = HashMap::<MessageId, oneshot::Sender<()>>::new();
    loop {
        let msg: Message = receive_msg().await;
        match msg.body.data {
            BodyData::Broadcast { message } => {
                if values.insert(message) {
                    for dest in config
                        .neighbours
                        .iter()
                        .filter(|&node_id| msg.src.ne(node_id))
                    {
                        let msg_id = gen_next_msg_id();
                        let send = broadcast(&config.node_id, &dest, message, msg_id);
                        pending_ack.insert(msg_id, send);
                    }
                }
                send_msg(&msg.create_response(BodyData::BroadcastOk));
            }
            BodyData::BroadcastOk => {
                if let Some(send) = pending_ack.remove(&msg.body.in_reply_to.unwrap()) {
                    send.send(()).unwrap();
                }
            }
            BodyData::Read => {
                send_msg(&msg.create_response(BodyData::ReadOk {
                    messages: values.iter().cloned().collect(),
                }));
            }
            _ => eprintln!("Ignoring unexpected message {:?}", msg),
        }
    }
}

fn broadcast(
    src: &NodeId,
    dest: &NodeId,
    val: BroadcastValue,
    msg_id: MessageId,
) -> oneshot::Sender<()> {
    let msg = Message {
        src: src.clone(),
        dest: dest.clone(),
        body: Body {
            msg_id: Some(msg_id),
            data: BodyData::Broadcast { message: val },
            in_reply_to: None,
        },
    };
    let (send, mut recv) = oneshot::channel();
    tokio::spawn(async move {
        loop {
            send_msg(&msg);
            tokio::select! {
                _ = &mut recv => {
                    eprintln!("Received broadcast {} ack from {}", &msg.body.msg_id.unwrap(), &msg.dest);
                    break;
                },
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(1000)) => {
                    eprintln!("Retrying broadcast {} to {}", &msg.body.msg_id.unwrap(), &msg.dest);
                },
            };
        }
    });
    send
}

async fn init_node() -> NodeConfig {
    let init_msg: Message = receive_msg().await;
    let node_id = if let BodyData::Init(ref data) = init_msg.body.data {
        send_msg(&init_msg.create_response(BodyData::InitOk));
        data.node_id.clone()
    } else {
        panic!("Expected init msg, got {:?}", init_msg);
    };
    let topology_msg: Message = receive_msg().await;
    if let BodyData::Topology { ref topology } = topology_msg.body.data {
        send_msg(&topology_msg.create_response(BodyData::TopologyOk));
        NodeConfig {
            neighbours: topology[&node_id].clone(),
            node_id,
        }
    } else {
        panic!("Expected topology msg, got {:?}", topology_msg);
    }
}
