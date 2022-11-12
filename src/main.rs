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
        "g-set" => workloads::crdts::g_set::run(),
        "g-counter" => workloads::crdts::g_counter::run(),
        "pn-counter" => workloads::crdts::pn_counter::run(),
        "txn-list-append-single-node" => workloads::datomic::single_node::run(),
        "txn-list-append-shared-state" => workloads::datomic::shared_state::run(),
        other => panic!("Unknown workload '{}'", other),
    }
}
