mod io;
mod protocol;
mod raft;
mod workloads;

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
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
        "txn-list-append-single-node" => workloads::txn_list_append::single_node::run(),
        "txn-list-append-shared-state" => workloads::txn_list_append::shared_state::run(),
        "txn-list-append-splitted-state" => workloads::txn_list_append::splitted_state::run(),
        "lin-kv-single-node" => workloads::lin_kv::single_node::run(),
        other => panic!("Unknown workload '{}'", other),
    }
}
