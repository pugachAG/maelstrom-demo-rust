use serde::{self, Deserialize, Serialize};
use serde_json::Value;

use super::{InitData, Body};

pub type Message = super::Message<Body<BodyData>>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BodyData {
    Init(InitData),
    InitOk,
    Echo(EchoData),
    EchoOk(EchoData),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EchoData {
    pub echo: Value,
}
