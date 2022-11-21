use serde::{self, Deserialize, Serialize};

pub type Key = u32;
pub type Value = u32;

pub type Message = super::Message<BodyData>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BodyData {
    Init(super::InitData),
    InitOk,
    Read(ReadData),
    ReadOk(ReadOkData),
    Write(WriteData),
    WriteOk,
    Cas(CasData),
    CasOk,
    Error(super::ErrorData),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadData {
    pub key: Key
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadOkData {
    pub value: Value
}


#[derive(Debug, Serialize, Deserialize)]
pub struct WriteData {
    pub key: Key,
    pub value: Value
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CasData {
    pub key: Key,
    pub from: Value,
    pub to: Value,
}