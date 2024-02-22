use core::time::Duration;
use std::collections::HashMap;
use std::error::Error;
use std::net::{IpAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::ops::Range;
use std::sync::mpsc::channel;
use std::sync::{Arc, Condvar, Mutex};
use std::{cmp, thread};

use crlf::serde::{Deserialize, Serialize};
use crlf::{service, Pipelined, Reconnector};
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

pub struct RaftNode<STM: StateMachine> {
    node_ids: Vec<NodeId>,
    node_connections: Vec<Pipelined<raft_svc::rpc::Request, raft_svc::rpc::Response>>,
    tickled: bool,

    // Volatile State
    role: Role,
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
struct LeaderState {
    next_idx: Vec<LogIdx>,
    match_idx: Vec<LogIdx>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum Role {
    Follower,
    Candidate,
    Leader(LeaderState),
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

impl RaftNode<KvStateMachine> {
    pub fn new(node_ids: Vec<NodeId>, state_machine: KvStateMachine) -> Self {
	let node_connections = node_ids.iter().map(|node_id| {
	    let node_id = node_id.clone();
	    let reconnector = Arc::new(Mutex::new(Reconnector::new(move || {
		TcpStream::connect(node_id).map(|stream| {
		    let _ = stream.set_nodelay(true);
		    stream
		}).ok()
	    })));

	    Pipelined::new_client(reconnector.clone(), reconnector)
	}).collect();
        RaftNode {
            node_ids,
	    node_connections,
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
		    connection.set_nodelay(true).unwrap();
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
		    connection.set_nodelay(true).unwrap();
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
        let (current_term, new_term, candidate_id, log_len, last_log_term, mut node_connections) = {
            let mut state = me.lock().unwrap_or_else(|e| e.into_inner());

            state.current_term += 1;
            state.voted_for = Some(state.node_ids[0]);

            let last_log_term = state.log.last().map(|e| e.0);
            (
                state.current_term,
                None,
                state.node_ids[0],
                state.log.len(),
                last_log_term,
                state.node_connections.clone(),
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

	let num_nodes = node_connections.len();
        for transport in node_connections.drain(1..) {
            let updater = election.clone();
            let cvar = cvar.clone();
            thread::spawn(move || {
                let res: Result<(), Box<dyn Error>> = (|| {
                    let mut client = raft_svc::client::RaftSvc { transport };
                    let (learned_term, success) =
                        client.request_vote(current_term, candidate_id, log_len, last_log_term)?;

		    let mut updated = updater.lock().unwrap_or_else(|e| e.into_inner());
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
            .wait_timeout_while(election.lock().unwrap_or_else(|e| e.into_inner()), Duration::from_millis(ms), |e| {
                e.total_votes < num_nodes && e.for_votes < num_nodes / 2 + 1 && e.new_term.is_none()
            })
            .unwrap();

        if timed_out.timed_out() {
            debug!("Timed out, try again...");
        } else if updated.for_votes >= num_nodes / 2 + 1 {
            // I won! I am a leader!
            info!("Elected as leader");
	    let mut state = me.lock().unwrap_or_else(|e| e.into_inner());
            if let Role::Candidate = state.role {
		let mut next_idx = Vec::new();
		next_idx.resize(num_nodes, state.log.len());

		let mut match_idx = Vec::new();
		match_idx.resize(num_nodes, 0);

                let leader_state = LeaderState {
                    next_idx,
                    match_idx,
                };
                state.role = Role::Leader(leader_state);
            } else {
                debug!("I guess I'm following now?");
            }
        } else if updated.new_term.is_some() {
            debug!("Term update!");
	    let mut state = me.lock().unwrap_or_else(|e| e.into_inner());
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
        let (term, leader_id, log_len, last_log_term, mut node_connections, leader_commit_idx) = {
	    let state = me.lock().unwrap_or_else(|e| e.into_inner());

            let last_log_term = state.log.last().map(|e| e.0);
            (
                state.current_term,
                state.node_ids[0],
                state.log.len(),
                last_log_term,
                state.node_connections.clone(),
                state.commit_idx,
            )
        };

        let prev_log_idx = if log_len > 0 { Some(log_len - 1) } else { None };

	for transport in node_connections.drain(1..) {
	    debug!("Sending!!!");
	    thread::spawn(move || {
		let res: Result<(), Box<dyn Error>> = (|| {
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
        debug!("Done sending");
        let timeout = Duration::from_millis(MIN_ELECTION_TIMEOUT / 3);
        thread::sleep(timeout);
    }

    fn do_follower_timeout(me: &Arc<Mutex<Self>>) {
	let mut state = me.lock().unwrap_or_else(|e| e.into_inner());
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
            let current_role = { me.lock().unwrap_or_else(|e| e.into_inner()).role.clone() };
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
        self.lock().unwrap_or_else(|e| e.into_inner()).append_entries(
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
            .unwrap_or_else(|e| e.into_inner())
            .request_vote(term, candidate_id, log_len, last_log_term)
    }
}

impl RaftSvc for RaftNode<KvStateMachine> {
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
	    if entries.len() > 0 {
		Err(AppendEntriesError::InconsistentLog)
	    } else {
		Ok(())
	    }
        } else {
            self.log.append(&mut entries);
            if let Some(lci) = leader_commit_idx {
                if self.commit_idx.map(|ci| lci > ci).unwrap_or(true) {
                    self.commit_idx = Some(cmp::min(lci, self.log.len() - 1));
		    if let Some(commit_idx) = self.commit_idx {
			let first_to_apply = if let Some(last_applied) = self.last_applied {
			    last_applied + 1
			} else {
			    0
			};
			for (_, ref entry) in Vec::from(&self.log[first_to_apply..commit_idx]) {
			    self.state_machine.apply(entry);
			}
			self.last_applied = Some(commit_idx);
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

impl RaftFrontend for Arc<Mutex<RaftNode<KvStateMachine>>> {
    fn do_op(&mut self, command: KvOperation) -> Option<u64> {
        let mystate = {
            let mut state = self.lock().unwrap_or_else(|e| e.into_inner());

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
                    state.node_connections.clone(),
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
            mut node_connections,
            leader_commit_idx,
            leader_state,
        )) = mystate
        {
            let (send_succeeded, recv_succeeded) = channel();

	    let num_nodes = node_connections.len();
            // Send to replicas
            for (i, transport) in node_connections.drain(1..).enumerate() {
                let command = command.clone();
                let send_succeeded = send_succeeded.clone();
		let mut next_idx = leader_state.next_idx[i];
		let mut match_idx = leader_state.match_idx[i];
                thread::spawn(move || {
                    let res: Result<(), Box<dyn Error>> = (|| {
                        while last_log_idx.map(|lli| lli >= next_idx).unwrap_or(false) {
                            let prev_log_idx = if next_idx > 0 {
                                Some(next_idx - 1)
                            } else {
                                None
                            };

                            let mut client = raft_svc::client::RaftSvc { transport: transport.clone() };
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
                        //leader_state.next_idx[i] = next_idx;
                        //leader_state.match_idx[i] = match_idx;
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
                if succeeded_count > num_nodes / 2 {
                    break;
                }
            }
            // Execute
            let mut state = self.lock().unwrap_or_else(|e| e.into_inner());
            state.role = Role::Leader(leader_state);
	    state.commit_idx = last_log_idx;
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
    }
}
