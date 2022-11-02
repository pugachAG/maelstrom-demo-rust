use std::collections::HashMap;

use serde::{self, Deserialize, Serialize};

use super::{InitData, Body, NodeId};

pub type BroadcastValue = i32;
pub type Message = super::Message<Body<BodyData>>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BodyData {
    Init(InitData),
    InitOk,
    Topology(TopologyData),
    TopologyOk,
    Broadcast(BroadcastData),
    BroadcastOk,
    Read,
    ReadOk(ReadOkData),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopologyData {
    pub topology: HashMap<NodeId, Vec<NodeId>>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BroadcastData {
    pub message: BroadcastValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadOkData {
    pub messages: Vec<BroadcastValue>,
}