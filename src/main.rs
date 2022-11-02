mod io;
mod protocol;
mod workloads;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 1 {
        panic!("Expected single argument, got {:?}", args);
    }
    match args[0].as_str() {
        "echo" => workloads::echo::run(),
        "broadcast" => workloads::broadcast::run(),
        other => panic!("Unknown workload '{}'", other),
    }
}
