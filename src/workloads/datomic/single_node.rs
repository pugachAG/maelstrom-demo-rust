use std::collections::HashMap;

use super::init_node;
use crate::io::{non_blocking::receive_msg, send_msg};
use crate::protocol::datomic::*;

pub fn run() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main());
}

async fn main() {
    init_node().await;
    let mut state = State::default();
    loop {
        let msg: Message = receive_msg().await;
        match msg.body.data {
            BodyData::Txn(ref data) => {
                let resp = msg.create_response(BodyData::TxnOk(TxnData {
                    txn: data.txn.iter().map(|func| state.apply(func)).collect(),
                }));
                send_msg(&resp);
            }
            _ => eprintln!("Ignoring unexpected message {:?}", msg),
        }
    }
}

#[derive(Default)]
struct State {
    mp: HashMap<KeyValue, Vec<ElementValue>>,
}

impl State {
    fn apply(&mut self, func: &TxnFunc) -> TxnFunc {
        match func {
            TxnFunc::Read { key, value: _ } => TxnFunc::Read {
                key: *key,
                value: self.mp.get(key).map(|v| v.clone()),
            },
            TxnFunc::Append { key, element } => {
                self.mp.entry(*key).or_default().push(*element);
                func.clone()
            }
        }
    }
}
