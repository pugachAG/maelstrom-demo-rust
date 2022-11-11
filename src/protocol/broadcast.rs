use std::collections::HashMap;

use serde::{self, Deserialize, Serialize};

use super::{InitData, NodeId};

pub type BroadcastValue = i32;
pub type Message = super::Message<BodyData>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BodyData {
    Init(InitData),
    InitOk,
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
    TopologyOk,
    Broadcast {
        message: BroadcastValue,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<BroadcastValue>,
    },
}
