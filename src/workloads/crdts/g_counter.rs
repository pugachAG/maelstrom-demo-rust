use std::collections::HashMap;

use super::Crdt;
use crate::protocol::{crdts::counter::*, NodeId};

pub fn run() {
    eprintln!("Running G-Counter workload");
    super::run(GCounter {
        node_id: None,
        values: HashMap::new(),
    });
}

type CounterValue = u64;

struct GCounter {
    node_id: Option<String>,
    values: HashMap<NodeId, CounterValue>,
}

impl Crdt for GCounter {
    type Body = CounterBodyData<CounterValue>;
    type State = HashMap<String, CounterValue>;

    fn init(&mut self, node_id: &NodeId) {
        self.node_id = Some(node_id.clone());
        self.values.insert(node_id.clone(), 0);
    }

    fn handle_msg(&mut self, body: &Self::Body) -> Option<Self::Body> {
        match body {
            CounterBodyData::Add { delta } => {
                *self.values.get_mut(self.node_id.as_ref().unwrap()).unwrap() += delta;
                Some(CounterBodyData::AddOk)
            }
            CounterBodyData::Read => Some(CounterBodyData::ReadOk {
                value: self.values.iter().map(|(_, &cnt)| cnt).sum::<CounterValue>()
            }),
            _ => None,
        }
    }

    fn update(&mut self, state: &Self::State) {
        for (node, &upd_cnt) in state {
            let cur_cnt = self.values.entry(node.clone()).or_insert(0);
            if upd_cnt > *cur_cnt {
                *cur_cnt = upd_cnt;
            }
        }
    }

    fn get_state(&self) -> Self::State {
        self.values.iter().map(|(k, &v)| (k.clone(), v)).collect()
    }
}