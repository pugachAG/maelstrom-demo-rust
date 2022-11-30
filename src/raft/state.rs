use std::{collections::HashSet, time::Duration};

use super::api::*;
use log::Level;
use rand::prelude::*;

pub struct RaftStateMachine<T> {
    config: NodeConfig,
    role: NodeRole,
    current_term: Term,
    commit_len: usize,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    log: Vec<LogEntry<T>>,
}

impl<T: Clone> RaftStateMachine<T> {
    pub fn new(config: NodeConfig) -> Self {
        Self {
            config,
            role: NodeRole::Follower,
            commit_len: 0,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            log: Vec::new(),
        }
    }

    pub fn start(&self) -> SideEffects<T> {
        self.log(Level::Info, "Start node".to_owned());
        vec![self.set_election_timer()]
    }

    pub fn on_event(&mut self, event: Event<T>) -> SideEffects<T> {
        match event {
            Event::TimerUp => match self.role {
                NodeRole::Follower => self.start_new_election(),
                NodeRole::Candidate(_) => self.start_new_election(),
                NodeRole::Leader(_) => self.send_heartbeat(),
            },
            Event::ReceivedRpc(rpc) => match rpc {
                Rpc::VoteRequest(rpc) => self.handle_vote_request(rpc),
                Rpc::VoteResponse(rpc) => self.handle_vote_response(rpc),
                Rpc::ReplicateLogRequest(rpc) => self.handle_replicate_log_request(rpc),
                Rpc::ReplicateLogResponse(rpc) => self.handle_replicate_log_response(rpc),
                Rpc::ProposeValueRequest(rpc) => self.handle_propose_value_request(rpc),
            },
        }
    }

    pub fn get_leader(&self) -> &Option<NodeId> {
        &self.leader_id
    }

    pub fn get_role(&self) -> &NodeRole {
        &self.role
    }

    pub fn get_current_term(&self) -> Term {
        self.current_term
    }

    fn start_new_election(&mut self) -> SideEffects<T> {
        self.role = NodeRole::Candidate(CandidateState {
            votes_received: HashSet::from([self.config.node_id.clone()]),
        });
        self.current_term += 1;
        self.log(
            Level::Info,
            format!("Start election term={}", self.current_term),
        );
        self.voted_for = Some(self.config.node_id.clone());
        let mut effects = Vec::new();
        effects.extend(self.other_nodes().map(|node_id| SideEffect::SendRpc {
            to: node_id.clone(),
            rpc: Rpc::VoteRequest(VoteRequestRpc {
                candidate_id: self.config.node_id.clone(),
                term: self.current_term,
                last_log: self.last_log_id(),
            }),
        }));
        effects.push(self.set_election_timer());
        effects
    }

    fn handle_vote_request(&mut self, rpc: VoteRequestRpc) -> SideEffects<T> {
        self.log(
            Level::Info,
            format!(
                "Handle VoteRequest {} | term={} | last_log={:?}",
                rpc.candidate_id, rpc.term, rpc.last_log
            ),
        );
        let mut effects = Vec::new();
        let term_ok = rpc.term >= self.current_term;
        self.maybe_advance_current_term(rpc.term);
        let vote_ok = match self.voted_for {
            Some(ref node_id) => *node_id == rpc.candidate_id,
            None => true,
        };
        let log_ok = rpc.last_log >= self.last_log_id();
        let vote_granted = term_ok && vote_ok && log_ok;
        if vote_granted && self.voted_for.is_none() {
            self.voted_for = Some(rpc.candidate_id.clone());
        }
        self.log(
            Level::Info,
            format!(
                "Send VoteResponse {} | term={} | vote=granted={} (voted_for={:?}, log_ok={})",
                rpc.candidate_id, self.current_term, vote_granted, self.voted_for, log_ok
            ),
        );
        if vote_granted {
            effects.push(self.set_election_timer());
        }
        effects.push(SideEffect::SendRpc {
            to: rpc.candidate_id,
            rpc: Rpc::VoteResponse(VoteResponseRpc {
                node_id: self.config.node_id.clone(),
                vote_granted,
                current_term: self.current_term,
            }),
        });
        effects
    }

    fn handle_vote_response(&mut self, rpc: VoteResponseRpc) -> SideEffects<T> {
        let mut effects = Vec::new();
        self.log(
            Level::Info,
            format!(
                "Handle VoteResponse {} | term={} | vote_granted={}",
                rpc.node_id, rpc.current_term, rpc.vote_granted
            ),
        );
        if self.current_term == rpc.current_term && rpc.vote_granted {
            if let NodeRole::Candidate(ref mut state) = self.role {
                state.votes_received.insert(rpc.node_id);
                let total_votes = state.votes_received.len();
                self.log(Level::Info, format!("Accept vote total={}", total_votes));
                if total_votes >= self.nodes_majority() {
                    self.log(
                        Level::Info,
                        format!("Elected leader term={}", self.current_term),
                    );
                    self.transition_to_leader();
                    effects.append(&mut self.replicate_log_all_nodes());
                    effects.push(self.set_heartbeat_timer());
                }
            }
        } else if self.maybe_advance_current_term(rpc.current_term) {
            effects.push(self.set_election_timer());
        }
        effects
    }

    fn handle_replicate_log_request(&mut self, rpc: ReplicateLogRequestRpc<T>) -> SideEffects<T> {
        let mut effects = Vec::new();
        let term_ok = rpc.term >= self.current_term;
        if term_ok {
            effects.push(self.set_election_timer());
            self.maybe_advance_current_term(rpc.term);
            self.leader_id = Some(rpc.leader_id.clone());
        }
        let log_ok = self.log.len() >= rpc.prev_log.index
            && (rpc.prev_log.term == 0 || self.log_id_at(rpc.prev_log.index) == rpc.prev_log);
        let success = term_ok && log_ok;
        self.log(
            if success && rpc.entries.len() == 0 {
                Level::Debug
            } else {
                Level::Info
            },
            format!(
                "Handle ReplicateLogRequest {} | term={} | success={} (term_ok={}, log_ok={}) | entries_len={}",
                rpc.leader_id, rpc.term, success, term_ok, log_ok, rpc.entries.len()
            ),
        );
        if success {
            self.log.truncate(rpc.prev_log.index);
            self.log.extend(rpc.entries.into_iter());
            if self.commit_len < rpc.commit_len {
                effects.append(&mut self.commit_entries(rpc.commit_len));
            }
        }
        effects.push(SideEffect::SendRpc {
            to: rpc.leader_id,
            rpc: Rpc::ReplicateLogResponse(ReplicateLogResponseRpc {
                request_term: rpc.term,
                node_id: self.config.node_id.clone(),
                current_term: self.current_term,
                log_len: self.log.len(),
                success,
            }),
        });
        effects
    }

    fn handle_replicate_log_response(&mut self, rpc: ReplicateLogResponseRpc) -> SideEffects<T> {
        let mut effects = Vec::new();
        if self.maybe_advance_current_term(rpc.current_term) {
            effects.push(self.set_election_timer());
        } else if self.current_term == rpc.request_term {
            let leader_state = match self.role {
                NodeRole::Leader(ref mut state) => state,
                _ => panic!("Expected leader role"),
            };
            let replication = leader_state.replication.get_mut(&rpc.node_id).unwrap();
            if rpc.success {
                if rpc.log_len > replication.match_index {
                    let prev_match_index = replication.match_index;
                    let new_match_index = rpc.log_len;
                    replication.match_index = new_match_index;
                    replication.next_index = new_match_index + 1;
                    effects.append(&mut self.maybe_commit_leader_entries());
                    self.log(
                        Level::Info,
                        format!(
                            "Advance match index {} {} -> {}",
                            rpc.node_id, prev_match_index, new_match_index
                        ),
                    );
                }
            } else {
                if replication.next_index > 1 {
                    replication.next_index = replication.next_index - 1;
                }
                effects.push(self.replicate_log(rpc.node_id));
            }
        }
        effects
    }

    fn handle_propose_value_request(&mut self, rpc: ProposeValueRequestRpc<T>) -> SideEffects<T> {
        let mut effects = Vec::new();
        self.log(
            Level::Info,
            format!("Propose value term={}", self.current_term),
        );
        if matches!(self.role, NodeRole::Leader(_)) {
            self.log.push(LogEntry {
                data: rpc.value,
                term: self.current_term,
            });
            self.log(
                Level::Info,
                format!("Append log value {:?}", self.last_log_id()),
            );
            effects.append(&mut self.replicate_log_all_nodes());
            effects.push(self.set_heartbeat_timer());
        } else if let Some(ref leader) = self.leader_id {
            self.log(Level::Info, format!("Forward log value to {}", leader));
            effects.push(SideEffect::SendRpc {
                to: leader.clone(),
                rpc: Rpc::ProposeValueRequest(rpc),
            });
        } else {
            self.log(Level::Warn, "Drop value".to_owned());
        }
        effects
    }

    fn send_heartbeat(&self) -> SideEffects<T> {
        let mut effects = self.replicate_log_all_nodes();
        effects.push(self.set_heartbeat_timer());
        effects
    }

    fn replicate_log_all_nodes(&self) -> SideEffects<T> {
        self.other_nodes()
            .map(|node| self.replicate_log(node.clone()))
            .collect()
    }

    fn replicate_log(&self, node_id: NodeId) -> SideEffect<T> {
        let leader_state = match self.role {
            NodeRole::Leader(ref state) => state,
            _ => panic!("Can only replicate in the leader role"),
        };
        let next_index = leader_state.replication[&node_id].next_index;
        let prev_log = self.log_id_at(next_index - 1);
        self.log(
            Level::Debug,
            format!(
                "Replicate log {} | term={} | prev_log={:?}",
                node_id, self.current_term, prev_log
            ),
        );
        SideEffect::SendRpc {
            to: node_id,
            rpc: Rpc::ReplicateLogRequest(ReplicateLogRequestRpc {
                leader_id: self.config.node_id.clone(),
                term: self.current_term,
                commit_len: self.commit_len,
                prev_log,
                entries: self.logs_tail(next_index),
            }),
        }
    }

    fn maybe_commit_leader_entries(&mut self) -> SideEffects<T> {
        let mut effects = Vec::new();
        let new_commit_len = self.leader_majority_match_index();
        if new_commit_len > self.commit_len
            && self.log[new_commit_len - 1].term == self.current_term
        {
            effects.append(&mut self.commit_entries(new_commit_len));
            effects.append(&mut self.replicate_log_all_nodes());
        }
        effects
    }

    fn maybe_advance_current_term(&mut self, term: Term) -> bool {
        if self.current_term < term {
            self.log(
                Level::Info,
                format!("Advance term {} -> {}", self.current_term, term),
            );
            self.current_term = term;
            self.role = NodeRole::Follower;
            self.voted_for = None;
            self.leader_id = None;
            true
        } else {
            false
        }
    }

    fn commit_entries(&mut self, new_commit_len: usize) -> SideEffects<T> {
        let mut effects = Vec::new();
        for i in self.commit_len..new_commit_len {
            let entry = &self.log[i];
            self.log(
                Level::Info,
                format!("Comitting entry index={} | term={}", i + 1, entry.term),
            );
            effects.push(SideEffect::ValueCommitted {
                value: entry.data.clone(),
            });
        }
        self.commit_len = new_commit_len;
        effects
    }

    fn transition_to_leader(&mut self) {
        self.role = NodeRole::Leader(LeaderState {
            replication: self
                .other_nodes()
                .map(|node| {
                    (
                        node.clone(),
                        NodeReplicationState {
                            next_index: self.last_log_id().index + 1,
                            match_index: 0,
                        },
                    )
                })
                .collect(),
        });
    }

    fn nodes_majority(&self) -> usize {
        (self.config.cluster.len() + 2) / 2
    }

    fn leader_majority_match_index(&self) -> LogIndex {
        let leader_state = match self.role {
            NodeRole::Leader(ref state) => state,
            _ => panic!("Expected leader role"),
        };
        let mut indices = leader_state
            .replication
            .values()
            .map(|r| r.match_index)
            .chain(std::iter::once(self.log.len()))
            .collect::<Vec<_>>();
        indices.sort();
        indices.reverse();
        indices[self.nodes_majority() - 1]
    }

    fn last_log_id(&self) -> LogEntryId {
        LogEntryId {
            index: self.log.len(),
            term: self.log.last().map(|e| e.term).unwrap_or(0),
        }
    }

    fn logs_tail(&self, from_index: LogIndex) -> Vec<LogEntry<T>> {
        self.log[from_index - 1..].iter().cloned().collect()
    }

    fn log_id_at(&self, log_index: LogIndex) -> LogEntryId {
        LogEntryId {
            term: if log_index > 0 {
                self.log[log_index - 1].term
            } else {
                0
            },
            index: log_index,
        }
    }

    fn other_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.config
            .cluster
            .iter()
            .filter(|&id| id != &self.config.node_id)
    }

    fn set_election_timer(&self) -> SideEffect<T> {
        let delta_millis = thread_rng().gen_range(0..self.config.election_timeout.as_nanos());
        let duration = self.config.election_timeout + Duration::from_nanos(delta_millis as u64);
        self.log(
            Level::Debug,
            format!("Set election timer for {}ms", duration.as_millis()),
        );
        SideEffect::SetTimer { duration }
    }

    fn set_heartbeat_timer(&self) -> SideEffect<T> {
        let duration = self.config.heartbeat_interval;
        self.log(
            Level::Debug,
            format!("Set heartbeat timer for {}ms", duration.as_millis()),
        );
        SideEffect::SetTimer { duration }
    }

    fn log(&self, lvl: Level, msg: String) {
        log::log!(lvl, "[{}] {}", self.config.node_id, msg);
    }
}
