use crate::{
    io::{non_blocking::receive_msg, send_msg},
    protocol::{
        raft::{BodyData, Message},
        NodeId,
    },
};

pub mod local_state;
pub mod single_node;

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
