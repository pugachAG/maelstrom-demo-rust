use std::collections::HashMap;

use super::Crdt;
use crate::protocol::{crdts::counter::*, NodeId};

pub fn run() {
    eprintln!("Running PN-Counter workload");
    super::run(PnCounter {
        node_id: None,
        values: HashMap::new(),
    });
}

type CounterValue = i64;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct CounterState {
    pos: u64,
    neg: u64,
}

struct PnCounter {
    node_id: Option<String>,
    values: HashMap<NodeId, CounterState>,
}

impl Crdt for PnCounter {
    type Body = CounterBodyData<CounterValue>;
    type State = HashMap<String, CounterState>;

    fn init(&mut self, node_id: &NodeId) {
        self.node_id = Some(node_id.clone());
        self.values.insert(node_id.clone(), CounterState::default());
    }

    fn handle_msg(&mut self, body: &Self::Body) -> Option<Self::Body> {
        match body {
            CounterBodyData::Add { delta } => {
                let state = self.values.get_mut(self.node_id.as_ref().unwrap()).unwrap();
                if *delta > 0 {
                    state.pos += delta.unsigned_abs();
                } else {
                    state.neg += delta.unsigned_abs();
                }
                Some(CounterBodyData::AddOk)
            }
            CounterBodyData::Read => Some(CounterBodyData::ReadOk {
                value: self
                    .values
                    .iter()
                    .map(|(_, state)| state.pos as i64 - state.neg as i64)
                    .sum::<CounterValue>(),
            }),
            _ => None,
        }
    }

    fn update(&mut self, state: &Self::State) {
        for (node, upd_state) in state {
            let cur_state = self.values.entry(node.clone()).or_default();
            cur_state.pos = std::cmp::max(cur_state.pos, upd_state.pos);
            cur_state.neg = std::cmp::max(cur_state.neg, upd_state.neg);
        }
    }

    fn get_state(&self) -> Self::State {
        self.values.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}
