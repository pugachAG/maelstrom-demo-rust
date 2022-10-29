use crate::{
    io::NodeIo,
    protocol::{
        echo::{Body, Message},
        InitOkData, MessageId,
    },
};

pub struct EchoWorkload<Io: NodeIo> {
    io: Io,
    next_msg_id: MessageId,
}

impl<Io: NodeIo> EchoWorkload<Io> {
    pub fn new(io: Io) -> Self {
        Self { io, next_msg_id: 0 }
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
        if let Body::Echo { ref echo, msg_id } = msg.body {
            let resp_body = Body::EchoOk {
                echo: echo.clone(),
                msg_id: self.gen_msg_id(),
                in_reply_to: msg_id,
            };
            self.send_response(&msg, resp_body);
        } else {
            panic!("Expected echo msg");
        }
    }

    fn init(&mut self) {
        let msg = self.receive_msg();
        if let Body::Init(ref init) = msg.body {
            eprintln!("Init node {}", init.node_id);
            let resp_body = Body::InitOk(InitOkData {
                msg_id: self.gen_msg_id(),
                in_reply_to: init.msg_id,
            });
            self.send_response(&msg, resp_body);
        } else {
            panic!("Expected init msg");
        }
    }

    fn send_response(&mut self, req: &Message, body: Body) {
        let resp = Message {
            src: req.dest.clone(),
            dest: req.src.clone(),
            body,
        };
        self.io.send(&resp);
    }

    fn gen_msg_id(&mut self) -> MessageId {
        let res = self.next_msg_id;
        self.next_msg_id += 1;
        res
    }

    fn receive_msg(&mut self) -> Message {
        eprintln!("Waiting for the next message");
        let msg = self.io.receive();
        eprintln!("Received {:?}", &msg);
        msg
    }
}
