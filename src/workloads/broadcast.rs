use std::collections::{HashMap, HashSet};

use tokio::{
    io::{stdin, AsyncBufReadExt, BufReader, Stdin},
    sync::oneshot,
};

use crate::{
    io::send_msg,
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
    let mut buf_reader = BufReader::new(stdin());
    let config = init_node(&mut buf_reader).await;
    let mut values = HashSet::<BroadcastValue>::new();
    let mut pending_ack = HashMap::<MessageId, oneshot::Sender<()>>::new();
    loop {
        let msg = receive_msg(&mut buf_reader).await;
        match &msg.body.data {
            BodyData::Broadcast(ref data) => {
                if values.insert(data.message) {
                    for dest in config
                        .neighbours
                        .iter()
                        .filter(|&node_id| msg.src.ne(node_id))
                    {
                        let msg_id = gen_next_msg_id();
                        let send = broadcast(&config.node_id, &dest, data.message, msg_id);
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
                send_msg(&msg.create_response(BodyData::ReadOk(ReadOkData {
                    messages: values.iter().cloned().collect(),
                })));
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
            msg_id,
            data: BodyData::Broadcast(BroadcastData { message: val }),
            in_reply_to: None,
        },
    };
    let (send, mut recv) = oneshot::channel();
    tokio::spawn(async move {
        loop {
            send_msg(&msg);
            tokio::select! {
                _ = &mut recv => {
                    eprintln!("Received broadcast {} ack from {}", &msg.body.msg_id, &msg.dest);
                    break;
                },
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(1000)) => {
                    eprintln!("Retrying broadcast {} to {}", &msg.body.msg_id, &msg.dest);
                },
            };
        }
    });
    send
}

async fn init_node(buf_reader: &mut BufReader<Stdin>) -> NodeConfig {
    let init_msg = receive_msg(buf_reader).await;
    let node_id = if let BodyData::Init(ref data) = init_msg.body.data {
        send_msg(&init_msg.create_response(BodyData::InitOk));
        data.node_id.clone()
    } else {
        panic!("Expected init msg, got {:?}", init_msg);
    };
    let topology_msg = receive_msg(buf_reader).await;
    if let BodyData::Topology(ref data) = topology_msg.body.data {
        send_msg(&topology_msg.create_response(BodyData::TopologyOk));
        NodeConfig {
            neighbours: data.topology[&node_id].clone(),
            node_id,
        }
    } else {
        panic!("Expected topolofy msg, got {:?}", topology_msg);
    }
}

async fn receive_msg(buf_reader: &mut BufReader<Stdin>) -> Message {
    let mut buf = String::new();
    buf_reader.read_line(&mut buf).await.unwrap();
    serde_json::from_str(&buf).unwrap()
}