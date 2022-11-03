use crate::{protocol::echo::*, io::{blocking::receive_msg, send_msg}};

pub fn run() {
    eprintln!("Running echo workload");
    init();
    loop {
        handle_echo();
    }
}

fn init() {
    let msg: Message = receive_msg();
    if let BodyData::Init(ref init) = msg.body.data {
        eprintln!("Init node {}", init.node_id);
        let resp_msg = msg.create_response(BodyData::InitOk);
        send_msg(&resp_msg);
    } else {
        panic!("Expected init msg");
    }
}

fn handle_echo() {
    let msg: Message = receive_msg();
    if let BodyData::Echo(ref echo_data) = msg.body.data {
        let resp_msg = msg.create_response(BodyData::EchoOk(EchoData {
            echo: echo_data.echo.clone(),
        }));
        send_msg(&resp_msg);
    } else {
        panic!("Expected echo msg");
    }
}
