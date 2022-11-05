use serde::{self, Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum CounterBodyData<T> {
    Add {
        delta: T,
    },
    AddOk,
    Read,
    ReadOk{
        value: T,
    },
}