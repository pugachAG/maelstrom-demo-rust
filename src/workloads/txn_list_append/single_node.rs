use super::{init_node, local_state::LocalState};
use crate::io::{non_blocking::receive_msg, send_msg};
use crate::protocol::txn_list_append::*;

pub fn run() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main());
}

async fn main() {
    init_node().await;
    let mut state = LocalState::default();
    loop {
        let msg: Message = receive_msg().await;
        match msg.body.data {
            BodyData::Txn(ref txn_data) => {
                let resp = msg.create_response(BodyData::TxnOk(state.apply_txn(txn_data)));
                send_msg(&resp);
            }
            _ => eprintln!("Ignoring unexpected message {:?}", msg),
        }
    }
}
