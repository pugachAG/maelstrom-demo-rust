use crate::{io::NodeIo, protocol::echo::*};

pub fn run(io: &impl NodeIo) {
    eprintln!("Running echo workload");
    init(io);
    loop {
        handle_echo(io);
    }
}

fn init(io: &impl NodeIo) {
    let msg: RequestMessage = io.receive();
    if let RequestData::Init(ref init) = msg.body.data {
        eprintln!("Init node {}", init.node_id);
        let resp_msg = msg.create_response(ResponseData::InitOk);
        io.send(&resp_msg);
    } else {
        panic!("Expected init msg");
    }
}

fn handle_echo(io: &impl NodeIo) {
    let msg: RequestMessage = io.receive();
    if let RequestData::Echo(ref echo_data) = msg.body.data {
        let resp_msg = msg.create_response(ResponseData::EchoOk(EchoData {
            echo: echo_data.echo.clone(),
        }));
        io.send(&resp_msg);
    } else {
        panic!("Expected echo msg");
    }
}
