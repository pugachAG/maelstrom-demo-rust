use rand::prelude::*;
use std::{
    collections::{BinaryHeap, HashMap},
    time::{Duration, Instant},
};

use crate::raft::{
    api::{Event, NodeConfig, NodeId, ProposeValueRequestRpc, Rpc, SideEffect, SideEffects},
    state::RaftStateMachine,
};

pub type LogEntryValue = u32;

pub const DEFAULT_ELECTION_TIMEOUT: Duration = Duration::from_millis(200);
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
pub const DEFAULT_RPC_LATENCY: Duration = Duration::from_millis(10);
pub const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

pub struct DriverConfig {
    pub cluster: Vec<NodeId>,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub rpc_latency: Duration,
}

impl DriverConfig {
    pub fn with_nodes(node_cnt: usize) -> Self {
        Self {
            cluster: (1..=node_cnt).map(|i| format!("node_{i}")).collect(),
            election_timeout: DEFAULT_ELECTION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            rpc_latency: DEFAULT_RPC_LATENCY,
        }
    }
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self::with_nodes(3)
    }
}

#[derive(Debug)]
struct TimedEvent<T> {
    time: Instant,
    node: NodeId,
    index: usize,
    event: Event<T>,
}

impl<T> PartialEq for TimedEvent<T> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.index == other.index
    }
}

impl<T> Eq for TimedEvent<T> {}

impl<T> PartialOrd for TimedEvent<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for TimedEvent<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time
            .cmp(&other.time)
            .then(self.node.cmp(&other.node))
            .then(self.index.cmp(&other.index))
            .reverse()
    }
}

struct NodeState<T> {
    node_id: NodeId,
    raft: RaftStateMachine<T>,
    next_event_index: usize,
    timer_event_index: usize,
    committed_values: Vec<T>,
}

pub struct ClusterDriver<T> {
    config: DriverConfig,
    time: Instant,
    events: BinaryHeap<TimedEvent<T>>,
    nodes: HashMap<NodeId, NodeState<T>>,
    rpc_drop_ratio: HashMap<(NodeId, NodeId), f64>,
}

impl<T: Clone + std::fmt::Debug> ClusterDriver<T> {
    pub fn new(config: DriverConfig) -> Self {
        Self {
            time: Instant::now(),
            nodes: config
                .cluster
                .iter()
                .map(|node_id| (node_id.clone(), NodeState::new(node_id, &config)))
                .collect(),
            events: BinaryHeap::new(),
            rpc_drop_ratio: config
                .cluster
                .iter()
                .flat_map(|node_from| {
                    config
                        .cluster
                        .iter()
                        .map(|node_to| ((node_from.clone(), node_to.clone()), 0.0))
                })
                .collect(),
            config,
        }
    }

    pub fn start(&mut self) {
        for node in self.config.cluster.clone() {
            for effect in self.get_node_mut(&node).raft.start() {
                self.handle_side_effect(self.time, &node, effect);
            }
        }
    }

    pub fn advance_time(&mut self, duration: Duration) {
        self.time += duration;
        while let Some(item) = self.pop_next_expired_event() {
            self.process_event(item);
        }
    }

    pub fn wait<P: Fn(&mut Self) -> bool>(&mut self, f: P, timeout: Duration) -> bool {
        let start = self.time;
        while !f(self) {
            if let Some(item) = self.events.pop() {
                self.time = item.time;
                self.process_event(item);
            } else {
                return false;
            }
            if self.time - start > timeout {
                return false;
            }
        }
        true
    }

    pub fn propose_value(&mut self, node_id: &NodeId, value: T) {
        let time = self.time;
        let event = self.get_node_mut(node_id).create_event(
            time,
            Event::ReceivedRpc(Rpc::ProposeValueRequest(ProposeValueRequestRpc { value })),
        );
        self.events.push(event);
    }

    pub fn get_config(&self) -> &DriverConfig {
        &self.config
    }

    pub fn get_raft_state(&self, node_id: &NodeId) -> &RaftStateMachine<T> {
        &self.nodes.get(node_id).unwrap().raft
    }

    pub fn get_committed_values(&self, node_id: &NodeId) -> &[T] {
        &self.nodes.get(node_id).unwrap().committed_values
    }

    pub fn set_rpc_drop_ratio(&mut self, node_from: NodeId, node_to: NodeId, drop_ratio: f64) {
        *self.rpc_drop_ratio.get_mut(&(node_from, node_to)).unwrap() = drop_ratio;
    }

    fn process_event(&mut self, item: TimedEvent<T>) {
        //eprintln!("[Driver][Event][{}][{}]: {:#?}", item.node, item.index, item.event);
        let effects = self
            .get_node_mut(&item.node)
            .process_event(item.index, item.event);
        for effect in effects {
            //eprintln!("[Driver][SideEffect][{}]: {:?}", item.node, effect);
            self.handle_side_effect(item.time, &item.node, effect)
        }
    }

    fn handle_side_effect(&mut self, time: Instant, node: &NodeId, effect: SideEffect<T>) {
        match effect {
            SideEffect::SetTimer { duration } => {
                let timer_up_event = self
                    .get_node_mut(node)
                    .create_event(time + duration, Event::TimerUp);
                self.events.push(timer_up_event);
            }
            SideEffect::SendRpc { to, rpc } => {
                let drop_prob = self.rpc_drop_ratio[&(node.clone(), to.clone())];
                if random::<f64>() > drop_prob {
                    let latency = self.config.rpc_latency;
                    let rpc_event = self
                        .get_node_mut(&to)
                        .create_event(time + latency, Event::ReceivedRpc(rpc));
                    self.events.push(rpc_event);
                }
            }
            SideEffect::ValueCommitted { value } => {
                self.get_node_mut(node).committed_values.push(value);
            }
        }
    }

    fn pop_next_expired_event(&mut self) -> Option<TimedEvent<T>> {
        if self
            .events
            .peek()
            .map_or(false, |item| item.time <= self.time)
        {
            self.events.pop()
        } else {
            None
        }
    }

    fn get_node_mut(&mut self, node_id: &NodeId) -> &mut NodeState<T> {
        self.nodes.get_mut(node_id).unwrap()
    }
}

impl<T: Clone> NodeState<T> {
    fn new(node_id: &NodeId, config: &DriverConfig) -> Self {
        Self {
            node_id: node_id.clone(),
            raft: RaftStateMachine::new(NodeConfig {
                node_id: node_id.clone(),
                cluster: config.cluster.clone(),
                election_timeout: config.election_timeout,
                heartbeat_interval: config.heartbeat_interval,
            }),
            next_event_index: 1,
            timer_event_index: 0,
            committed_values: Vec::new(),
        }
    }

    fn process_event(&mut self, index: usize, event: Event<T>) -> SideEffects<T> {
        if matches!(event, Event::TimerUp) && index != self.timer_event_index {
            return vec![];
        }
        self.raft.on_event(event)
    }

    fn create_event(&mut self, time: Instant, event: Event<T>) -> TimedEvent<T> {
        let index = self.next_event_index;
        self.next_event_index += 1;
        if matches!(event, Event::TimerUp) {
            self.timer_event_index = index;
        }
        TimedEvent {
            time,
            node: self.node_id.clone(),
            index,
            event,
        }
    }
}
