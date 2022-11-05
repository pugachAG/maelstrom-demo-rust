use std::collections::HashSet;

use super::Crdt;
use crate::protocol::crdts::g_set::*;

pub fn run() {
    eprintln!("Running G-Set workload");
    super::run(GSet {
        values: HashSet::new(),
    });
}

struct GSet {
    values: HashSet<ElementValue>,
}

impl Crdt for GSet {
    type Body = GSetBodyData;
    type State = GsetState;

    fn handle_msg(&mut self, body: &Self::Body) -> Option<Self::Body> {
        match body {
            GSetBodyData::Add { element } => {
                self.values.insert(*element);
                Some(GSetBodyData::AddOk)
            }
            GSetBodyData::Read => Some(GSetBodyData::ReadOk {
                value: self.get_state(),
            }),
            _ => None,
        }
    }

    fn update(&mut self, state: &Self::State) {
        self.values.extend(state.iter());
    }

    fn get_state(&self) -> Self::State {
        self.values.iter().cloned().collect()
    }
}
