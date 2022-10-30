use io::StandardStreamsJsonIo;

mod io;
mod protocol;
mod workloads;

fn main() {
    let io = StandardStreamsJsonIo {};
    workloads::echo::run(&io);
}
