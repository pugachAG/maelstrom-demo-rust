use std::{collections::HashSet, time::Duration};

use super::api::*;
use rand::prelude::*;

pub struct RaftStateMachine<T> {
    config: NodeConfig,
    role: NodeRole,
    current_term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    log: Vec<LogEntry<T>>,
}

impl<T: Clone> RaftStateMachine<T> {
    pub fn new(config: NodeConfig) -> Self {
        Self {
            config,
            role: NodeRole::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            log: Vec::new(),
        }
    }

    pub fn start(&self) -> SideEffects<T> {
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
            },
        }
    }

    pub fn get_role(&self) -> &NodeRole {
        &self.role
    }

    pub fn get_current_term(&self) -> Term {
        self.current_term
    }

    fn handle_vote_request(&mut self, rpc: VoteRequestRpc) -> SideEffects<T> {
        let mut effects = Vec::new();
        let term_ok = rpc.term >= self.current_term;
        let updated_term = self.maybe_advance_current_term(rpc.term);
        let vote_ok = match self.voted_for {
            Some(ref node_id) => *node_id == rpc.candidate_id,
            None => true,
        };
        let log_ok = rpc.last_log >= self.last_log_id();
        let vote_granted = term_ok && vote_ok && log_ok;
        if vote_granted && self.voted_for.is_none() {
            self.voted_for = Some(rpc.candidate_id.clone());
        }
        if vote_granted || updated_term {
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
        if self.current_term == rpc.current_term && rpc.vote_granted {
            if let NodeRole::Candidate(ref mut state) = self.role {
                state.votes_received.insert(rpc.node_id);
                if state.votes_received.len() >= self.nodes_majority() {
                    self.transition_to_leader();
                    effects.append(&mut self.replicate_log_all_nodes());
                    effects.push(self.set_heartbeat_timer());
                }
            }
        } else {
            if self.maybe_advance_current_term(rpc.current_term) {
                effects.push(self.set_election_timer());
            }
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
        if success {
            while self.log.len() > rpc.prev_log.index {
                self.log.pop();
            }
            self.log.extend(rpc.entries.into_iter());
        }
        effects.push(SideEffect::SendRpc {
            to: rpc.leader_id,
            rpc: Rpc::ReplicateLogResponse(ReplicateLogResponseRpc {
                node_id: self.config.node_id.clone(),
                current_term: self.current_term,
                log_len: self.log.len(),
                success,
            }),
        });
        effects
    }

    fn handle_replicate_log_response(&mut self, _rpc: ReplicateLogResponseRpc) -> SideEffects<T> {
        vec![]
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
        SideEffect::SendRpc {
            to: node_id,
            rpc: Rpc::ReplicateLogRequest(ReplicateLogRequestRpc {
                leader_id: self.config.node_id.clone(),
                term: self.current_term,
                prev_log: self.log_id_at(next_index - 1),
                entries: self.logs_tail(next_index),
            }),
        }
    }

    fn maybe_advance_current_term(&mut self, term: Term) -> bool {
        if self.current_term < term {
            self.current_term = term;
            self.role = NodeRole::Follower;
            self.voted_for = None;
            self.leader_id = None;
            true
        } else {
            false
        }
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

    fn start_new_election(&mut self) -> SideEffects<T> {
        self.role = NodeRole::Candidate(CandidateState {
            votes_received: HashSet::from([self.config.node_id.clone()]),
        });
        self.current_term += 1;
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

    fn nodes_majority(&self) -> usize {
        (self.config.cluster.len() + 1) / 2
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
        SideEffect::SetTimer {
            duration: self.config.election_timeout + Duration::from_nanos(delta_millis as u64),
        }
    }

    fn set_heartbeat_timer(&self) -> SideEffect<T> {
        SideEffect::SetTimer {
            duration: self.config.heartbeat_interval,
        }
    }
}
