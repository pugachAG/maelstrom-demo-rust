use std::collections::HashMap;

use crate::protocol::datomic::*;

#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct LocalState {
    map: HashMap<KeyValue, Vec<ElementValue>>,
}

impl LocalState {
    pub fn apply_txn(&mut self, txn_data: &TxnData) -> TxnData {
        TxnData {
            txn: txn_data
                .txn
                .iter()
                .map(|func| self.apply_func(func))
                .collect(),
        }
    }

    fn apply_func(&mut self, func: &TxnFunc) -> TxnFunc {
        match func {
            TxnFunc::Read { key, value: _ } => TxnFunc::Read {
                key: *key,
                value: self.map.get(key).map(|v| v.clone()),
            },
            TxnFunc::Append { key, element } => {
                self.map.entry(*key).or_default().push(*element);
                func.clone()
            }
        }
    }
}
