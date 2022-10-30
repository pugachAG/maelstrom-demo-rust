use serde::{self, Deserialize, Serialize};
use serde_json::Value;

use super::{InitData, RequestBody};

pub type RequestMessage = super::Message<RequestBody<RequestData>>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum RequestData {
    Init(InitData),
    Echo(EchoData),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum ResponseData {
    InitOk,
    EchoOk(EchoData),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EchoData {
    pub echo: Value,
}
