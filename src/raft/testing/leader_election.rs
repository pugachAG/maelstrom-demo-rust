use super::driver::{ClusterDriver, DriverConfig, LogEntryValue};

#[cfg(test)]
mod leader_election_tests {
    use crate::raft::testing::{driver::{DEFAULT_ELECTION_TIMEOUT}, leader_election::start_cluster};

    #[test]
    pub fn initial_election() {
        let mut driver = start_cluster();
        driver.advance_time(DEFAULT_ELECTION_TIMEOUT * 5);
        let leaders = driver.get_leaders();
        assert_eq!(leaders.len(), 1);
        let leader = driver.get_raft_state(&leaders[0]);
        assert_eq!(leader.get_current_term(), 1);
    }


    #[test]
    pub fn elections_without_majority() {
        let mut driver = start_cluster();
        for node in driver.get_config().cluster.clone() {
            driver.disconnect_node(&node);
        }
        driver.advance_time(DEFAULT_ELECTION_TIMEOUT * 10);
        assert_eq!(driver.get_leaders().len(), 0);
    }

    #[test]
    pub fn election_after_losing_leader() {
        let mut driver = start_cluster();
        driver.advance_time(DEFAULT_ELECTION_TIMEOUT * 5);
        let initial_leader = driver.get_leaders().into_iter().next().unwrap();
        driver.disconnect_node(&initial_leader);
        driver.advance_time(DEFAULT_ELECTION_TIMEOUT * 5);
        assert_eq!(driver.get_leaders().len(), 2);
        let new_leader = driver.get_leaders().into_iter().filter(|node| node != &initial_leader).next().unwrap();
        driver.connect_node(&initial_leader);
        driver.advance_time(DEFAULT_ELECTION_TIMEOUT * 5);
        assert_eq!(driver.get_leaders().len(), 1);
        assert_eq!(driver.get_leaders()[0], new_leader);
        assert_eq!(driver.get_raft_state(&new_leader).get_current_term(), 2);
    }
}

fn start_cluster() -> ClusterDriver<LogEntryValue> {
    let mut driver = ClusterDriver::new(DriverConfig::default());
    driver.start();
    driver
}
