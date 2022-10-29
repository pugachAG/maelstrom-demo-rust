use crate::{io::NodeIo, protocol::echo::*};

pub struct EchoWorkload<Io: NodeIo> {
    io: Io,
}

impl<Io: NodeIo> EchoWorkload<Io> {
    pub fn new(io: Io) -> Self {
        Self { io }
    }

    pub fn run(&mut self) {
        eprintln!("Running echo workload");
        self.init();
        loop {
            self.handle_echo();
        }
    }

    fn handle_echo(&mut self) {
        let msg = self.receive_msg();
        if let RequestData::Echo(ref echo_data) = msg.body.data {
            let resp_msg = msg.create_response(ResponseData::EchoOk(EchoData {
                echo: echo_data.echo.clone(),
            }));
            self.send_response(&resp_msg);
        } else {
            panic!("Expected echo msg");
        }
    }

    fn init(&mut self) {
        let msg = self.receive_msg();
        if let RequestData::Init(ref init) = msg.body.data {
            eprintln!("Init node {}", init.node_id);
            let resp_msg = msg.create_response(ResponseData::InitOk);
            self.send_response(&resp_msg);
        } else {
            panic!("Expected init msg");
        }
    }

    fn send_response(&self, msg: &ResponseMessage) {
        eprintln!("Sending {:?}", msg);
        self.io.send(&msg);
    }

    fn receive_msg(&mut self) -> RequestMessage {
        eprintln!("Waiting for the next message");
        let msg = self.io.receive();
        eprintln!("Received {:?}", msg);
        msg
    }
}
