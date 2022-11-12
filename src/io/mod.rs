use crate::protocol::Message;

pub mod sync_resp;

pub mod blocking {
    use crate::protocol::Message;

    pub fn receive_msg<T: serde::de::DeserializeOwned>() -> Message<T> {
        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf).unwrap();
        serde_json::from_str(&buf).unwrap()
    }
}

pub mod non_blocking {
    use crate::protocol::Message;

    pub async fn receive_msg<T: serde::de::DeserializeOwned>() -> Message<T> {
        use once_cell::sync::Lazy;
        use tokio::{
            io::{AsyncBufReadExt, BufReader, Stdin},
            sync::Mutex,
        };
        static BUF_READER: Lazy<Mutex<BufReader<Stdin>>> = Lazy::new(|| {
            Mutex::new(BufReader::new(tokio::io::stdin()))
        });
        let mut buf = String::new();
        let mut reader = BUF_READER.lock().await;
        reader.read_line(&mut buf).await.unwrap();
        serde_json::from_str(&buf).unwrap()
    }
}

pub fn send_msg<T: serde::Serialize>(msg: &Message<T>) {
    let s = serde_json::to_string(msg).unwrap();
    println!("{}", s);
}