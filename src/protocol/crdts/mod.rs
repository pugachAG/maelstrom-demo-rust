use serde::{self, Deserialize, Serialize};

pub mod g_set;
pub mod counter;

pub type Message<T, S> = crate::protocol::Message<CrdtBody<T, S>>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CrdtBody<T, S> {
    Common(CommonBodyData<S>),
    Custom(T),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum CommonBodyData<S> {
    Init(super::InitData),
    InitOk,
    Replicate {
        state: S,
    },
}