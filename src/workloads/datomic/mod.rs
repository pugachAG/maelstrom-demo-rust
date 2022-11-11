use crate::io::{non_blocking::receive_msg, send_msg};
use crate::protocol::datomic::*;

pub mod single_node;

pub async fn init_node() {
    let msg: Message = receive_msg().await;
    match msg.body.data {
        BodyData::Init(ref data) => {
            eprintln!("Received init msg: {data:?}");
            send_msg(&msg.create_response(BodyData::InitOk));
        }
        _ => panic!("Expected init msg, got {:?}", msg),
    }
}
