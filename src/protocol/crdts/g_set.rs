use serde::{self, Deserialize, Serialize};

pub type ElementValue = i32;
pub type GsetState = Vec<ElementValue>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum GSetBodyData {
    Add {
        element: ElementValue,
    },
    AddOk,
    Read,
    ReadOk{
        value: Vec<ElementValue>,
    },
}