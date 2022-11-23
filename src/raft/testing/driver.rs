use rand::prelude::*;
use std::{
    collections::{BinaryHeap, HashMap},
    time::{Duration, Instant},
};

use crate::raft::{
    api::{Event, NodeConfig, NodeId, NodeRole, SideEffect, SideEffects},
    state::RaftStateMachine,
};

pub type LogEntryValue = u32;

pub const DEFAULT_ELECTION_TIMEOUT: Duration = Duration::from_millis(200);
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
pub const DEFAULT_RPC_LATENCY: Duration = Duration::from_millis(10);
pub const NODE_1: &str = "n1";
pub const NODE_2: &str = "n2";
pub const NODE_3: &str = "n3";

pub struct DriverConfig {
    pub cluster: Vec<NodeId>,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub rpc_latency: Duration,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            cluster: Vec::from([NODE_1, NODE_2, NODE_3].map(|s| s.to_owned())),
            election_timeout: DEFAULT_ELECTION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            rpc_latency: DEFAULT_RPC_LATENCY,
        }
    }
}

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

struct NodeDriver<T> {
    node_id: NodeId,
    raft: RaftStateMachine<T>,
    next_event_index: usize,
    timer_event_index: usize,
}

pub struct ClusterDriver<T> {
    config: DriverConfig,
    time: Instant,
    events: BinaryHeap<TimedEvent<T>>,
    nodes: HashMap<NodeId, NodeDriver<T>>,
    rpc_drop_ratio: HashMap<(NodeId, NodeId), f64>,
}

impl<T: Clone + std::fmt::Debug> ClusterDriver<T> {
    pub fn new(config: DriverConfig) -> Self {
        Self {
            time: Instant::now(),
            nodes: config
                .cluster
                .iter()
                .map(|node_id| (node_id.clone(), NodeDriver::new(node_id, &config)))
                .collect(),
            events: BinaryHeap::new(),
            rpc_drop_ratio: dbg!(config
                .cluster
                .iter()
                .flat_map(|node_from| config
                    .cluster
                    .iter()
                    .map(|node_to| ((node_from.clone(), node_to.clone()), 0.0)))
                .collect()),
            config,
        }
    }

    pub fn start(&mut self) {
        for node in self.config.cluster.clone() {
            for effect in self.get_node(&node).raft.start() {
                self.process_side_effect(self.time, &node, effect);
            }
        }
    }

    pub fn advance_time(&mut self, duration: Duration) {
        self.time += duration;
        while let Some(item) = self.pop_next_expired_event() {
            let effects = self
                .get_node(&item.node)
                .process_event(item.index, item.event);
            for effect in effects {
                self.process_side_effect(item.time, &item.node, effect)
            }
        }
    }

    pub fn get_config(&self) -> &DriverConfig {
        &self.config
    }

    pub fn get_leaders(&self) -> Vec<NodeId> {
        self.config
            .cluster
            .iter()
            .filter(|&node| matches!(self.get_raft_state(node).get_role(), NodeRole::Leader(_)))
            .cloned()
            .collect()
    }

    pub fn get_raft_state(&self, node_id: &NodeId) -> &RaftStateMachine<T> {
        &self.nodes.get(node_id).unwrap().raft
    }

    pub fn connect_node(&mut self, node_id: &NodeId) {
        self.set_node_rpc_drop_ratio(node_id, 0.0)
    }

    pub fn disconnect_node(&mut self, node_id: &NodeId) {
        self.set_node_rpc_drop_ratio(node_id, 1.0)
    }

    fn set_node_rpc_drop_ratio(&mut self, node_id: &NodeId, ratio: f64) {
        for ((node_from, node_to), drop_prob) in &mut self.rpc_drop_ratio {
            if node_from == node_id || node_to == node_id {
                *drop_prob = ratio;
            }
        }
    }

    fn process_side_effect(&mut self, time: Instant, node: &NodeId, effect: SideEffect<T>) {
        eprintln!("Processing side effect from {node}: {effect:?}");
        match effect {
            SideEffect::SetTimer { duration } => {
                let timer_up_event = self
                    .get_node(node)
                    .create_event(time + duration, Event::TimerUp);
                self.events.push(timer_up_event);
            }
            SideEffect::SendRpc { to, rpc } => {
                let drop_prob = self.rpc_drop_ratio[&(node.clone(), to.clone())];
                if random::<f64>() > drop_prob {
                    let latency = self.config.rpc_latency;
                    let rpc_event = self
                        .get_node(&to)
                        .create_event(time + latency, Event::ReceivedRpc(rpc));
                    self.events.push(rpc_event);
                }
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

    fn get_node(&mut self, node_id: &NodeId) -> &mut NodeDriver<T> {
        self.nodes.get_mut(node_id).unwrap()
    }
}

impl<T: Clone> NodeDriver<T> {
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
