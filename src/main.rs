mod io;
mod protocol;
mod workloads;

fn main() {
    workloads::echo::run();
}
