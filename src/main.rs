use io::StandardStreamsJsonIo;
use workloads::echo::EchoWorkload;

mod io;
mod protocol;
mod workloads;

fn main() {
    let io = StandardStreamsJsonIo {};
    EchoWorkload::new(io).run();
}
