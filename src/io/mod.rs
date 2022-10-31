use std::io::stdin;

use serde::{de::DeserializeOwned, Serialize};

use crate::protocol::Message;

pub fn receive_msg<T: DeserializeOwned>() -> Message<T> {
    let mut buf = String::new();
    stdin().read_line(&mut buf).unwrap();
    serde_json::from_str(&buf).unwrap()
}

pub fn send_msg<T: Serialize>(msg: &Message<T>) {
    let s = serde_json::to_string(msg).unwrap();
    println!("{}", s);
}