use super::{init_node, local_state::LocalState};
use crate::io::{non_blocking::receive_msg, send_msg};
use crate::protocol::{raft::*, ErrorData};

pub fn run() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main());
}

async fn main() {
    init_node().await;
    let mut state = LocalState::new();
    loop {
        let msg: Message = receive_msg().await;
        let resp_body = handle_request(&mut state, &msg.body.data);
        let resp = msg.create_response(resp_body);
        send_msg(&resp);
    }
}

fn handle_request(state: &mut LocalState, data: &BodyData) -> BodyData {
    match data {
        BodyData::Read(data) => {
            handle_error(state.read(data).map(|read_ok| BodyData::ReadOk(read_ok)))
        }
        BodyData::Write(data) => {
            state.write(data);
            BodyData::WriteOk
        }
        BodyData::Cas(data) => handle_error(state.cas(data).map(|_| BodyData::CasOk)),
        other => panic!("Unknown request body: {:?}", other),
    }
}

fn handle_error(res: Result<BodyData, ErrorData>) -> BodyData {
    match res {
        Ok(data) => data,
        Err(err) => BodyData::Error(err),
    }
}
