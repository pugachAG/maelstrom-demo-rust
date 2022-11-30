#[cfg(test)]
mod replication_tests {
    use crate::raft::testing::{
        driver::{DEFAULT_ELECTION_TIMEOUT, DEFAULT_RPC_LATENCY, DEFAULT_WAIT_TIMEOUT},
        driver_utils::{start_default_cluster, start_default_cluster_with_leader, DriverExt},
    };

    #[test]
    pub fn propose_single_value_from_leader() {
        let mut driver = start_default_cluster();
        let leader = driver.wait_for_leader();
        driver.propose_value(&leader, 42);
        driver.advance_time(DEFAULT_RPC_LATENCY);
        assert!(driver.get_committed_values(&leader).is_empty());
        driver.advance_time(DEFAULT_RPC_LATENCY);
        assert_eq!(driver.get_committed_values(&leader), &vec![42]);
        for follower in driver.get_followers() {
            assert!(driver.get_committed_values(&follower).is_empty());
        }
        driver.advance_time(2 * DEFAULT_RPC_LATENCY);
        for node in driver.get_all_nodes() {
            assert_eq!(driver.get_committed_values(&node), &vec![42]);
        }
    }

    #[test]
    pub fn propose_single_value_from_follower() {
        let mut driver = start_default_cluster_with_leader();
        driver.propose_value(&driver.get_any_follower(), 42);
        driver.advance_time(5 * DEFAULT_RPC_LATENCY);
        for node in driver.get_all_nodes() {
            assert_eq!(driver.get_committed_values(&node), &vec![42]);
        }
    }

    #[test]
    pub fn drop_value_after_reconnect() {
        let mut driver = start_default_cluster();
        let old_leader = driver.wait_for_leader();
        driver.disconnect_node(old_leader.clone());
        driver.propose_value(&old_leader, 1);
        assert!(
            driver.wait(
                |driver| driver.get_leaders().len() == 2,
                DEFAULT_WAIT_TIMEOUT
            ),
            "New leader should be elected"
        );
        let new_leader = driver
            .get_leaders()
            .into_iter()
            .filter(|node| node != &old_leader)
            .next()
            .unwrap();
        let follower = driver.get_any_follower();
        driver.propose_value(&new_leader, 2);
        driver.wait_node_value_committed(follower.clone(), 2);
        driver.connect_node(old_leader.clone());
        driver.wait_node_value_committed(old_leader.clone(), 2);
        assert_eq!(driver.get_committed_values(&old_leader), vec![2]);
    }

    #[test]
    pub fn should_not_commit_with_older_term() {
        let mut driver = start_default_cluster();
        let leader = driver.wait_for_leader();
        let follower = driver.get_any_follower();
        driver.disconnect_all_nodes();
        driver.propose_value(&leader, 42);
        driver.advance_time(10 * DEFAULT_ELECTION_TIMEOUT);
        driver.connect_nodes(leader.clone(), follower.clone());
        assert_eq!(
            driver.wait_for_leader(),
            leader,
            "The same leader should be elected after reconnect due to longer log len"
        );
        driver.advance_time(10 * DEFAULT_ELECTION_TIMEOUT);
        assert!(
            driver.get_committed_values(&leader).is_empty(),
            "Should not commit entries with a previous term"
        );
        driver.propose_value(&leader, 740);
        driver.wait_node_value_committed(leader.clone(), 740);
        assert_eq!(driver.get_committed_values(&leader), vec![42, 740]);
        driver.wait_node_value_committed(follower.clone(), 740);
        assert_eq!(driver.get_committed_values(&follower), vec![42, 740]);
    }
}
