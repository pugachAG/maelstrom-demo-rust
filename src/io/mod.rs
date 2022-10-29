use std::io::stdin;

use serde::{de::DeserializeOwned, Serialize};

use crate::protocol::Message;

pub trait NodeIo {
    fn receive<T: DeserializeOwned>(&self) -> Message<T>;
    fn send<T: Serialize>(&self, msg: &Message<T>);
}

pub struct StandardStreamsJsonIo {}

impl NodeIo for StandardStreamsJsonIo {
    fn receive<T: DeserializeOwned>(&self) -> Message<T> {
        let mut buf = String::new();
        stdin().read_line(&mut buf).unwrap();
        serde_json::from_str(&buf).unwrap()
    }

    fn send<T: Serialize>(&self, msg: &Message<T>) {
        let s = serde_json::to_string(msg).unwrap();
        println!("{}", s);
    }
}