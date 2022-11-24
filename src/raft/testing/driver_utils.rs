use std::fmt::Debug;

use crate::raft::api::{NodeId, NodeRole};

use super::driver::{ClusterDriver, DriverConfig, LogEntryValue, DEFAULT_WAIT_TIMEOUT};

pub trait DriverExt {
    fn get_leaders(&self) -> Vec<NodeId>;
    fn get_leader(&self) -> NodeId;
    fn has_leader(&self) -> bool;
    fn wait_for_leader(&mut self) -> NodeId;
    fn set_node_rpc_drop_ratio(&mut self, node_id: NodeId, ratio: f64);
    fn set_all_nodes_rpc_drop_ratio(&mut self, ratio: f64);
    fn connect_node(&mut self, node_id: NodeId);
    fn connect_all_nodes(&mut self);
    fn disconnect_node(&mut self, node_id: NodeId);
    fn disconnect_all_nodes(&mut self);
}

impl<T: Clone + Debug> DriverExt for ClusterDriver<T> {
    fn get_leaders(&self) -> Vec<NodeId> {
        self.get_config()
            .cluster
            .iter()
            .filter(|&node| matches!(self.get_raft_state(node).get_role(), NodeRole::Leader(_)))
            .cloned()
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

    fn set_node_rpc_drop_ratio(&mut self, node_id: NodeId, ratio: f64) {
        for other_node in self.get_config().cluster.clone() {
            self.set_rpc_drop_ratio(other_node.clone(), node_id.clone(), ratio);
            self.set_rpc_drop_ratio(node_id.clone(), other_node, ratio);
        }
    }

    fn connect_node(&mut self, node_id: NodeId) {
        self.set_node_rpc_drop_ratio(node_id, 0.0);
    }

    fn disconnect_node(&mut self, node_id: NodeId) {
        self.set_node_rpc_drop_ratio(node_id, 1.0);
    }

    fn set_all_nodes_rpc_drop_ratio(&mut self, ratio: f64) {
        for node in self.get_config().cluster.clone() {
            self.set_node_rpc_drop_ratio(node, ratio);
        }
    }

    fn connect_all_nodes(&mut self) {
        self.set_all_nodes_rpc_drop_ratio(0.0);
    }

    fn disconnect_all_nodes(&mut self) {
        self.set_all_nodes_rpc_drop_ratio(1.0);
    }

    fn wait_for_leader(&mut self) -> NodeId {
        assert!(
            self.wait(|driver| driver.has_leader(), DEFAULT_WAIT_TIMEOUT),
            "Failed to elect cluster leader"
        );
        self.get_leader()
    }
}

pub fn start_default_cluster() -> ClusterDriver<LogEntryValue> {
    let mut driver = ClusterDriver::new(DriverConfig::default());
    driver.start();
    driver
}
