use std::{collections::HashMap, fmt::Debug, sync::Mutex};

use crate::protocol::{Message, MessageId};
use serde::Serialize;
use tokio::{sync::oneshot, time::Duration};

use super::send_msg;

pub struct SyncRespHandler<T> {
    pending: Mutex<HashMap<MessageId, oneshot::Sender<Message<T>>>>,
}

impl<T: Debug> SyncRespHandler<T> {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub async fn handle(&self, msg: Message<T>) {
        if let Some(ref msg_id) = msg.body.in_reply_to {
            if let Some(sender) = self.pending.lock().unwrap().remove(msg_id) {
                sender
                    .send(msg)
                    .unwrap_or_else(|resp| eprintln!("Failed to handle response: {resp:?}"));
            }
        }
    }

    pub async fn send<R: Serialize>(&self, msg: Message<R>, timeout: Duration) -> Option<Message<T>> {
        let (send, recv) = oneshot::channel();
        self.pending.lock().unwrap().insert(msg.body.msg_id.unwrap(), send);
        send_msg(&msg);
        match tokio::time::timeout(timeout, recv).await {
            Ok(Ok(resp)) => Some(resp),
            _ => None,
        }
    }
}
