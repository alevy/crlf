use core::time::Duration;
use std::collections::HashMap;
use std::error::Error;
use std::net::{IpAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::ops::Range;
use std::sync::mpsc::channel;
use std::sync::{Arc, Condvar, Mutex};
use std::{cmp, thread};

use crlf::serde::{Deserialize, Serialize};
use crlf::service;
use log::{debug, info};
use rand::Rng;

const MIN_ELECTION_TIMEOUT: u64 = 150;
const MAX_ELECTION_TIMEOUT: u64 = 300;
const ELECTION_TIMEOUT_RANGE: Range<u64> = MIN_ELECTION_TIMEOUT..MAX_ELECTION_TIMEOUT;

pub trait StateMachine: Send {
    type Operation: Send;
    type Result: Send;

    fn apply(&mut self, command: &Self::Operation) -> Self::Result;
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(crate = "crlf::serde")]
pub enum KvOperation {
    Read { key: u64 },
    Write { key: u64, value: u64 },
}

pub type KvStateMachine = HashMap<u64, u64>;

impl StateMachine for KvStateMachine {
    type Operation = KvOperation;
    type Result = Option<u64>;

    fn apply(&mut self, command: &KvOperation) -> Option<u64> {
        match command {
            KvOperation::Read { key } => self.get(key).map(Clone::clone),
            KvOperation::Write { key, value } => self.insert(*key, *value),
        }
    }
}

// --

type Term = u64;
type NodeId = (IpAddr, u16);
type LogIdx = usize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftNode<STM: StateMachine, const N: usize = 3> {
    node_ids: [NodeId; N],
    tickled: bool,

    // Volatile State
    role: Role<N>,
    commit_idx: Option<LogIdx>,
    last_applied: Option<LogIdx>,
    current_leader_id: Option<NodeId>,

    // Persistent State
    state_machine: STM,
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<(Term, STM::Operation)>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct LeaderState<const N: usize> {
    next_idx: [LogIdx; N],
    match_idx: [LogIdx; N],
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum Role<const N: usize> {
    Follower,
    Candidate,
    Leader(LeaderState<N>),
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(crate = "crlf::serde")]
pub enum AppendEntriesError {
    StaleTerm(Term),
    InconsistentLog,
}

#[service]
pub trait RaftSvc {
    fn append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_idx: Option<LogIdx>,
        prev_log_term: Term,
        entries: Vec<(Term, KvOperation)>,
        leader_commit_idx: Option<LogIdx>,
    ) -> Result<(), AppendEntriesError>;

    fn request_vote(
        &mut self,
        term: Term,
        candidate_id: NodeId,
        log_len: usize,
        last_log_term: Option<Term>,
    ) -> (Term, bool);
}

impl<const N: usize> RaftNode<KvStateMachine, N> {
    pub fn new(node_ids: [NodeId; N], state_machine: KvStateMachine) -> Self {
        RaftNode {
            node_ids,
            tickled: true,
            role: Role::Follower,
            commit_idx: None,
            last_applied: None,
            current_leader_id: None,
            state_machine,
            current_term: 0,
            voted_for: None,
            log: Default::default(),
        }
    }

    fn start_raft_node<A: ToSocketAddrs>(
        me: &Arc<Mutex<Self>>,
        listen_addr: A,
    ) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(listen_addr)?;
        let me = me.clone();
        thread::spawn(move || {
            for connection in listener.incoming() {
                if let Ok(connection) = connection {
                    let me = me.clone();
                    thread::spawn(move || {
                        let mut server = raft_svc::server::RaftSvc {
                            sender: connection.try_clone().unwrap(),
                            receiver: connection,
                            inner: me,
                        };

                        server.run()
                    });
                }
            }
        });
        Ok(())
    }

    fn start_frontend<A: ToSocketAddrs>(
        me: &Arc<Mutex<Self>>,
        listen_addr: A,
    ) -> Result<(), Box<dyn Error>> {
        let client_listener = TcpListener::bind(listen_addr)?;
        let me = me.clone();
        thread::spawn(move || {
            for connection in client_listener.incoming() {
                if let Ok(connection) = connection {
                    let me = me.clone();
                    thread::spawn(move || {
                        let mut server = raft_frontend::server::RaftFrontend {
                            sender: connection.try_clone().unwrap(),
                            receiver: connection,
                            inner: me,
                        };

                        server.run()
                    });
                }
            }
        });
        Ok(())
    }

    fn do_election(me: &Arc<Mutex<Self>>) {
        // Start election
        let (current_term, new_term, candidate_id, log_len, last_log_term, other_nodes_ids) = {
            let mut state = me.lock().unwrap();

            state.current_term += 1;
            state.voted_for = Some(state.node_ids[0]);

            let last_log_term = state.log.last().map(|e| e.0);
            (
                state.current_term,
                None,
                state.node_ids[0],
                state.log.len(),
                last_log_term,
                state.node_ids.clone(),
            )
        };

        struct Election {
            new_term: Option<Term>,
            for_votes: usize,
            total_votes: usize,
        }
        let election = Arc::new(Mutex::new(Election {
            new_term,
            for_votes: 1,
            total_votes: 1,
        }));
        let cvar = Arc::new(Condvar::new());

        for node_id in other_nodes_ids[1..].iter() {
            let updater = election.clone();
            let cvar = cvar.clone();
            let node_id = node_id.clone();
            thread::spawn(move || {
                let res: Result<(), Box<dyn Error>> = (|| {
                    let transport = TcpStream::connect(node_id)?;
                    let mut client = raft_svc::client::RaftSvc { transport };
                    let (learned_term, success) =
                        client.request_vote(current_term, candidate_id, log_len, last_log_term)?;

                    let mut updated = updater.lock()?;
                    updated.total_votes += 1;
                    if learned_term > current_term {
                        updated.new_term = Some(learned_term);
                    } else if success {
                        updated.for_votes += 1;
                    }
                    Ok(())
                })();
                if let Err(e) = res {
                    debug!("Errored {:?}", e);
                }
                cvar.notify_one();
            });
        }

        let mut rng = rand::thread_rng();
        let ms = rng.gen_range(ELECTION_TIMEOUT_RANGE);

        // Wait for either all votes or majority votes
        let (updated, timed_out) = cvar
            .wait_timeout_while(election.lock().unwrap(), Duration::from_millis(ms), |e| {
                e.total_votes < N && e.for_votes < N / 2 + 1 && e.new_term.is_none()
            })
            .unwrap();

        if timed_out.timed_out() {
            debug!("Timed out, try again...");
        } else if updated.for_votes >= N / 2 + 1 {
            // I won! I am a leader!
            info!("Elected as leader");
            let mut state = me.lock().unwrap();
            if let Role::Candidate = state.role {
                let leader_state = LeaderState {
                    next_idx: [state.log.len(); N],
                    match_idx: [0; N],
                };
                state.role = Role::Leader(leader_state);
            } else {
                debug!("I guess I'm following now?");
            }
        } else if updated.new_term.is_some() {
            debug!("Term update!");
            let mut state = me.lock().unwrap();
            if let Some(new_term) = updated.new_term {
                state.current_term = new_term;
                state.voted_for = None;
                state.role = Role::Follower;
            }
        } else {
            debug!(
                "I lost {:?} {} {}. I'm probably a follower already",
                updated.new_term, updated.total_votes, updated.for_votes
            );
        }
    }

    fn do_leader_keepalive(me: &Arc<Mutex<Self>>) {
        let (term, leader_id, log_len, last_log_term, other_nodes_ids, leader_commit_idx) = {
            let state = me.lock().unwrap();

            let last_log_term = state.log.last().map(|e| e.0);
            (
                state.current_term,
                state.node_ids[0],
                state.log.len(),
                last_log_term,
                state.node_ids.clone(),
                state.commit_idx,
            )
        };

        let prev_log_idx = if log_len > 0 { Some(log_len - 1) } else { None };

        thread::scope(|t| {
            for node_id in other_nodes_ids[1..].iter() {
                debug!("Sending!!!");
                t.spawn(move || {
                    let res: Result<(), Box<dyn Error>> = (|| {
                        let transport = TcpStream::connect(node_id)?;
                        let mut client = raft_svc::client::RaftSvc { transport };
                        let res = client.append_entries(
                            term,
                            leader_id,
                            prev_log_idx,
                            last_log_term.unwrap_or(0),
                            vec![],
                            leader_commit_idx,
                        )?;
                        debug!("Send result {term} {:?}", res);
                        Ok(())
                    })();
                    if let Err(e) = res {
                        debug!("Errored {:?}", e);
                    }
                });
            }
        });
        debug!("Done sending");
        let timeout = Duration::from_millis(MIN_ELECTION_TIMEOUT / 3);
        thread::sleep(timeout);
    }

    fn do_follower_timeout(me: &Arc<Mutex<Self>>) {
        let mut state = me.lock().unwrap();
        if state.tickled {
            state.tickled = false;
            drop(state);
            let mut rng = rand::thread_rng();
            let ms = rng.gen_range(ELECTION_TIMEOUT_RANGE);
            let timeout = Duration::from_millis(ms);
            thread::sleep(timeout);
        } else {
            // Timeout! I should start an election
            debug!("Timeout!");
            state.role = Role::Candidate;
        }
    }

    pub fn start(mut self) -> Result<(), Box<dyn Error>> {
        self.role = Role::Follower;

        let raft_listen_address = self.node_ids[0];
        let mut client_address = self.node_ids[0].clone();
        client_address.1 = client_address.1 + 10;

        let me = Arc::new(Mutex::new(self));

        Self::start_raft_node(&me, raft_listen_address)?;
        Self::start_frontend(&me, client_address)?;

        loop {
            let current_role: Role<N> = { me.lock().unwrap().role.clone() };
            match current_role {
                Role::Follower => {
                    Self::do_follower_timeout(&me);
                }
                Role::Candidate => {
                    Self::do_election(&me);
                }
                Role::Leader(_) => {
                    Self::do_leader_keepalive(&me);
                }
            }
        }
    }
}

impl<T: RaftSvc> RaftSvc for Arc<Mutex<T>> {
    fn append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_idx: Option<LogIdx>,
        prev_log_term: Term,
        entries: Vec<(Term, KvOperation)>,
        leader_commit_idx: Option<LogIdx>,
    ) -> Result<(), AppendEntriesError> {
        self.lock().unwrap().append_entries(
            term,
            leader_id,
            prev_log_idx,
            prev_log_term,
            entries,
            leader_commit_idx,
        )
    }

    fn request_vote(
        &mut self,
        term: Term,
        candidate_id: NodeId,
        log_len: usize,
        last_log_term: Option<Term>,
    ) -> (Term, bool) {
        self.lock()
            .unwrap()
            .request_vote(term, candidate_id, log_len, last_log_term)
    }
}

impl<const N: usize> RaftSvc for RaftNode<KvStateMachine, N> {
    fn append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_idx: Option<LogIdx>,
        prev_log_term: Term,
        mut entries: Vec<(Term, KvOperation)>,
        leader_commit_idx: Option<LogIdx>,
    ) -> Result<(), AppendEntriesError> {
        if term >= self.current_term {
            self.tickled = true;
            self.role = Role::Follower;
            self.current_term = term;
            if self.current_leader_id != Some(leader_id) {
                info!("Leader {:?} elected", leader_id);
                self.current_leader_id = Some(leader_id);
            }
        }

        if term < self.current_term {
            Err(AppendEntriesError::StaleTerm(self.current_term))
        } else if prev_log_idx
            .map(|pli| {
                self.log
                    .get(pli)
                    .map(|(t, _)| *t == prev_log_term)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
        {
            Err(AppendEntriesError::InconsistentLog)
        } else {
            self.log.truncate(prev_log_idx.map(|l| l + 1).unwrap_or(0));
            self.log.append(&mut entries);
            if let Some(lci) = leader_commit_idx {
                if self.commit_idx.map(|ci| lci > ci).unwrap_or(true) {
                    self.commit_idx = Some(cmp::min(lci, self.log.len() - 1));
                    if let Some((last_applied, commit_idx)) = self.last_applied.zip(self.commit_idx)
                    {
                        for (_, ref entry) in Vec::from(&self.log[last_applied..commit_idx]) {
                            self.state_machine.apply(entry);
                        }
                    }
                }
            }
            Ok(())
        }
    }

    fn request_vote(
        &mut self,
        term: Term,
        candidate_id: NodeId,
        log_len: usize,
        last_log_term: Option<Term>,
    ) -> (Term, bool) {
        self.tickled = true;
        debug!("Vote requested from {candidate_id:?} at {term}");
        if term > self.current_term {
            debug!("I am a follower now! {} {}", term, self.current_term);
            self.role = Role::Follower;
            self.voted_for = None;
            self.current_term = term;
        }

        if term >= self.current_term
            && self
                .voted_for
                .map(|cid| cid == candidate_id)
                .unwrap_or(true)
            && (self.log.len() < log_len
                || (self.log.len() == log_len && self.log.last().map(|e| e.0) == last_log_term))
        {
            self.voted_for = Some(candidate_id);
            self.current_term = term;
            debug!("Yup {:?}", candidate_id);
            (term, true)
        } else {
            debug!(
                "Nope {term} {} {:?} {} {}",
                self.current_term,
                self.voted_for,
                self.log.len(),
                log_len
            );
            (self.current_term, false)
        }
    }
}

#[service]
pub trait RaftFrontend {
    fn do_op(&mut self, command: KvOperation) -> Option<u64>;
}

impl<const N: usize> RaftFrontend for Arc<Mutex<RaftNode<KvStateMachine, N>>> {
    fn do_op(&mut self, command: KvOperation) -> Option<u64> {
        let mystate = {
            let mut state = self.lock().unwrap();

            if let Role::Leader(leader_state) = state.role.clone() {
                let current_term = state.current_term;
                state.log.push((current_term, command.clone()));

                let last_log_term = state.log.last().map(|e| e.0);
                let last_log_idx = if state.log.len() > 0 {
                    Some(state.log.len() - 1)
                } else {
                    None
                };
                Some((
                    current_term,
                    state.node_ids[0],
                    last_log_idx,
                    last_log_term,
                    state.node_ids.clone(),
                    state.commit_idx,
                    leader_state,
                ))
            } else {
                None
            }
        };

        if let Some((
            current_term,
            leader_id,
            last_log_idx,
            last_log_term,
            node_ids,
            leader_commit_idx,
            mut leader_state,
        )) = mystate
        {
            let (send_succeeded, recv_succeeded) = channel();

            // Send to replicas
            for (i, node_id) in node_ids[1..].iter().enumerate() {
                let command = command.clone();
                let node_id = node_id.clone();
                let send_succeeded = send_succeeded.clone();
                thread::spawn(move || {
                    let res: Result<(), Box<dyn Error>> = (|| {
                        let mut next_idx = leader_state.next_idx[i];
                        let mut match_idx = leader_state.match_idx[i];
                        while last_log_idx.map(|lli| lli >= next_idx).unwrap_or(false) {
                            let prev_log_idx = if next_idx > 0 {
                                Some(next_idx - 1)
                            } else {
                                None
                            };

                            let transport = TcpStream::connect(node_id)?;
                            let mut client = raft_svc::client::RaftSvc { transport };
                            let res = client.append_entries(
                                current_term,
                                leader_id,
                                prev_log_idx,
                                last_log_term.unwrap_or(0),
                                vec![(current_term, command.clone())],
                                leader_commit_idx,
                            )?;
                            debug!("Send result {current_term} {:?}", res);
                            match res {
                                Err(AppendEntriesError::InconsistentLog) => {
                                    next_idx -= 1;
                                    continue;
                                }
                                Err(AppendEntriesError::StaleTerm(_term)) => {
                                    // TODO: Update term and become follower
                                    panic!("WTF");
                                }
                                Ok(_) => {
                                    next_idx += 1;
                                    match_idx += 1;
                                    break;
                                }
                            }
                        }
                        leader_state.next_idx[i] = next_idx;
                        leader_state.match_idx[i] = match_idx;
                        let _ = send_succeeded.send(());
                        Ok(())
                    })();
                    if let Err(e) = res {
                        debug!("Errored {:?}", e);
                    }
                });
            }

            let mut succeeded_count = 0;
            for () in recv_succeeded.iter() {
                succeeded_count += 1;
                if succeeded_count > N / 2 {
                    break;
                }
            }
            // Execute
            let mut state = self.lock().unwrap();
            state.role = Role::Leader(leader_state);
            let res = state.state_machine.apply(&command);

            res
        } else {
            // TODO Not a leader!
            None
        }
    }
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;

    use super::*;

    const NODE_IDS: [(IpAddr, u16); 2] = [
        (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1111),
        (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1112),
    ];

    #[test]
    fn append_entries_empty() {
        let mut node = RaftNode {
            node_ids: NODE_IDS,
            tickled: false,
            role: Role::Follower,
            commit_idx: None,
            last_applied: None,
            current_leader_id: None,
            state_machine: <KvStateMachine as Default>::default(),
            current_term: 0,
            voted_for: Some(([127, 0, 0, 1].into(), 1112)),
            log: Default::default(),
        };

        let mut node_expected = node.clone();

        let res = node.append_entries(1, NODE_IDS[1], None, 0, vec![], None);
        assert_eq!(Ok(()), res);

        node_expected.tickled = true;
        node_expected.current_term = 1;
        node_expected.current_leader_id = Some(NODE_IDS[1]);

        assert_eq!(node, node_expected);
    }
}
