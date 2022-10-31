use crate::{io::NodeIo, protocol::echo::*};

pub fn run(io: &impl NodeIo) {
    eprintln!("Running echo workload");
    init(io);
    loop {
        handle_echo(io);
    }
}

fn init(io: &impl NodeIo) {
    let msg: Message = io.receive();
    if let BodyData::Init(ref init) = msg.body.data {
        eprintln!("Init node {}", init.node_id);
        let resp_msg = msg.create_response(BodyData::InitOk);
        io.send(&resp_msg);
    } else {
        panic!("Expected init msg");
    }
}

fn handle_echo(io: &impl NodeIo) {
    let msg: Message = io.receive();
    if let BodyData::Echo(ref echo_data) = msg.body.data {
        let resp_msg = msg.create_response(BodyData::EchoOk(EchoData {
            echo: echo_data.echo.clone(),
        }));
        io.send(&resp_msg);
    } else {
        panic!("Expected echo msg");
    }
}
