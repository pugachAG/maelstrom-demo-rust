#[cfg(test)]
mod leader_election_tests {
    use crate::raft::testing::{
        driver::{DEFAULT_ELECTION_TIMEOUT, DEFAULT_WAIT_TIMEOUT},
        driver_utils::{start_default_cluster, DriverExt},
    };

    #[test]
    pub fn initial_election() {
        let mut driver = start_default_cluster();
        assert!(driver.wait(|driver| driver.has_leader(), DEFAULT_ELECTION_TIMEOUT * 3));
        let leader = driver.get_leader();
        assert_eq!(driver.get_raft_state(&leader).get_current_term(), 1);
    }

    #[test]
    pub fn elections_without_majority() {
        let mut driver = start_default_cluster();
        driver.disconnect_all_nodes();
        driver.advance_time(DEFAULT_ELECTION_TIMEOUT * 10);
        assert_eq!(driver.get_leaders().len(), 0);
    }

    #[test]
    pub fn election_after_losing_leader() {
        let mut driver = start_default_cluster();
        let initial_leader = driver.wait_for_leader();
        driver.disconnect_node(initial_leader.clone());
        assert!(
            driver.wait(
                |driver| driver.get_leaders().len() == 2,
                DEFAULT_WAIT_TIMEOUT
            ),
            "Failed to elected a new leader"
        );
        driver.connect_node(initial_leader);
        assert!(
            driver.wait(
                |driver| driver.get_leaders().len() == 1,
                DEFAULT_WAIT_TIMEOUT
            ),
            "The old leader failed to recognize the new one after reconnect"
        );
        let new_leader = driver.get_leader();
        assert_eq!(driver.get_raft_state(&new_leader).get_current_term(), 2);
    }
}
