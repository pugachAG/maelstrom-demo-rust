use std::fmt::Debug;

use once_cell::sync::OnceCell;

use crate::raft::api::{NodeId, NodeRole};

use super::driver::{ClusterDriver, DriverConfig, LogEntryValue, DEFAULT_WAIT_TIMEOUT};

pub trait DriverExt<T> {
    fn get_all_nodes(&self) -> Vec<NodeId>;
    fn get_leaders(&self) -> Vec<NodeId>;
    fn get_leader(&self) -> NodeId;
    fn has_leader(&self) -> bool;
    fn get_followers(&self) -> Vec<NodeId>;
    fn get_any_follower(&self) -> NodeId;
    fn wait_for_leader(&mut self) -> NodeId;
    fn set_node_rpc_drop_ratio(&mut self, node_id: NodeId, ratio: f64);
    fn set_all_nodes_rpc_drop_ratio(&mut self, ratio: f64);
    fn set_bidirectional_rpc_drop_ratio(&mut self, node_a: NodeId, node_b: NodeId, ratio: f64);
    fn connect_nodes(&mut self, node_a: NodeId, node_b: NodeId);
    fn connect_node(&mut self, node_id: NodeId);
    fn connect_all_nodes(&mut self);
    fn disconnect_nodes(&mut self, node_a: NodeId, node_b: NodeId);
    fn disconnect_node(&mut self, node_id: NodeId);
    fn disconnect_all_nodes(&mut self);
    fn wait_node_value_committed(&mut self, node_id: NodeId, value: T);
}

impl<T: Clone + Debug + Eq> DriverExt<T> for ClusterDriver<T> {
    fn get_all_nodes(&self) -> Vec<NodeId> {
        self.get_config().cluster.clone()
    }

    fn get_leaders(&self) -> Vec<NodeId> {
        self.get_all_nodes()
            .into_iter()
            .filter(|node| matches!(self.get_raft_state(node).get_role(), NodeRole::Leader(_)))
            .collect()
    }

    fn get_leader(&self) -> NodeId {
        let leaders = self.get_leaders();
        assert_eq!(
            leaders.len(),
            1,
            "Expected single leader, got {} {:?}",
            leaders.len(),
            leaders
        );
        leaders.into_iter().next().unwrap()
    }

    fn has_leader(&self) -> bool {
        !self.get_leaders().is_empty()
    }

    fn get_followers(&self) -> Vec<NodeId> {
        let leaders = self.get_leaders();
        self.get_all_nodes()
            .into_iter()
            .filter(|node| !leaders.contains(node))
            .collect()
    }

    fn get_any_follower(&self) -> NodeId {
        self.get_followers().pop().unwrap()
    }

    fn set_node_rpc_drop_ratio(&mut self, node_id: NodeId, ratio: f64) {
        for other_node in self.get_config().cluster.clone() {
            self.set_bidirectional_rpc_drop_ratio(other_node, node_id.clone(), ratio);
        }
    }

    fn set_bidirectional_rpc_drop_ratio(&mut self, node_a: NodeId, node_b: NodeId, ratio: f64) {
        self.set_rpc_drop_ratio(node_a.clone(), node_b.clone(), ratio);
        self.set_rpc_drop_ratio(node_b, node_a, ratio);
    }

    fn connect_nodes(&mut self, node_a: NodeId, node_b: NodeId) {
        self.set_bidirectional_rpc_drop_ratio(node_a, node_b, 0.0);
    }

    fn connect_node(&mut self, node_id: NodeId) {
        log::info!("Connect {}", node_id);
        self.set_node_rpc_drop_ratio(node_id, 0.0);
    }

    fn disconnect_nodes(&mut self, node_a: NodeId, node_b: NodeId) {
        self.set_bidirectional_rpc_drop_ratio(node_a, node_b, 1.0);
    }

    fn disconnect_node(&mut self, node_id: NodeId) {
        log::info!("Disconnect {}", node_id);
        self.set_node_rpc_drop_ratio(node_id, 1.0);
    }

    fn set_all_nodes_rpc_drop_ratio(&mut self, ratio: f64) {
        for node in self.get_config().cluster.clone() {
            self.set_node_rpc_drop_ratio(node, ratio);
        }
    }

    fn connect_all_nodes(&mut self) {
        log::info!("Connect all nodes");
        self.set_all_nodes_rpc_drop_ratio(0.0);
    }

    fn disconnect_all_nodes(&mut self) {
        log::info!("Disconnect all nodes");
        self.set_all_nodes_rpc_drop_ratio(1.0);
    }

    fn wait_for_leader(&mut self) -> NodeId {
        assert!(
            self.wait(
                |driver| driver.has_leader()
                    && driver.get_followers().into_iter().all(|follower| {
                        driver.get_raft_state(&follower).get_leader().is_some()
                    }),
                DEFAULT_WAIT_TIMEOUT
            ),
            "Failed to elect cluster leader"
        );
        self.get_leader()
    }

    fn wait_node_value_committed(&mut self, node_id: NodeId, value: T) {
        assert!(
            self.wait(
                |driver| { driver.get_committed_values(&node_id).contains(&value) },
                DEFAULT_WAIT_TIMEOUT
            ),
            "Failed replicate value"
        );
    }
}

pub fn ensure_logging_enabled() {
    static INSTANCE: OnceCell<()> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .format_timestamp(None)
            .is_test(true)
            .init()
    });
}

pub fn start_default_cluster() -> ClusterDriver<LogEntryValue> {
    ensure_logging_enabled();
    let mut driver = ClusterDriver::new(DriverConfig::default());
    driver.start();
    driver
}

pub fn start_default_cluster_with_leader() -> ClusterDriver<LogEntryValue> {
    let mut driver = start_default_cluster();
    driver.wait_for_leader();
    driver
}