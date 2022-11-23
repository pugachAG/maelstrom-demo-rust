use std::{collections::{HashSet, HashMap}, time::Duration};

pub type Term = u32;
pub type NodeId = String;
pub type LogIndex = usize;

pub struct NodeConfig {
    pub node_id: NodeId,
    pub cluster: Vec<NodeId>,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogEntryId {
    pub term: Term,
    pub index: LogIndex,
}

#[derive(Debug, Clone)]
pub struct LogEntry<T> {
    pub data: T,
    pub term: Term,
}

pub enum NodeRole {
    Follower,
    Candidate(CandidateState),
    Leader(LeaderState),
}

pub struct CandidateState {
    pub votes_received: HashSet<NodeId>,
}

pub struct NodeReplicationState {
    pub next_index: LogIndex,
    pub match_index: LogIndex,
}

pub struct LeaderState {
    pub replication: HashMap<NodeId, NodeReplicationState>,
}

pub enum Event<T> {
    TimerUp,
    ReceivedRpc(Rpc<T>),
}

#[derive(Debug)]
pub enum SideEffect<T> {
    SetTimer{
        duration: Duration
    },
    SendRpc {
        to: NodeId,
        rpc: Rpc<T>,
    }
}

pub type SideEffects<T> = Vec<SideEffect<T>>;

#[derive(Debug)]
pub enum Rpc<T> {
    VoteRequest(VoteRequestRpc),
    VoteResponse(VoteResponseRpc),
    ReplicateLogRequest(ReplicateLogRequestRpc<T>),
    ReplicateLogResponse(ReplicateLogResponseRpc),
}

#[derive(Debug)]
pub struct ReplicateLogRequestRpc<T> {
    pub leader_id: NodeId,
    pub term: Term,
    pub prev_log: LogEntryId,
    pub entries: Vec<LogEntry<T>>,
}

#[derive(Debug)]
pub struct ReplicateLogResponseRpc {
    pub node_id: NodeId,
    pub current_term: Term,
    pub log_len: usize,
    pub success: bool,
}

#[derive(Debug)]
pub struct VoteRequestRpc {
    pub candidate_id: NodeId,
    pub term: Term,
    pub last_log: LogEntryId,
}

#[derive(Debug)]
pub struct VoteResponseRpc {
    pub node_id: NodeId,
    pub vote_granted: bool,
    pub current_term: Term,
}
