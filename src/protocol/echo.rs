use serde::{self, Serialize, Deserialize};
use serde_json::Value;

use super::{InitData, MessageId, InitOkData};

pub type Message = super::Message<Body>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum Body {
    Init(InitData),
    InitOk(InitOkData),
    Echo {
        echo: Value,
        msg_id: MessageId,
    },
    EchoOk {
        echo: Value,
        msg_id: MessageId,
        in_reply_to: MessageId,
    }
}