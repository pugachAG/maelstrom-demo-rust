use serde::{self, Serialize, Deserialize};
use serde_json::Value;

use super::{InitData, RequestBody, ResponseBody};

pub type RequestMessage = super::Message<RequestBody<RequestData>>;
pub type ResponseMessage = super::Message<ResponseBody<ResponseData>>;

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