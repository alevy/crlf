use core::time::Duration;
use std::collections::HashMap;
use std::error::Error;
use std::net::{IpAddr, TcpListener, TcpStream};
use std::ops::Range;
use std::sync::mpsc::channel;
use std::sync::{Arc, Condvar, Mutex};
use std::{thread, cmp};

use crlf::serde::{Deserialize, Serialize};
use crlf::service;
use log::{debug, info};
use rand::Rng;

const MIN_ELECTION_TIMEOUT: u64 = 150;
const MAX_ELECTION_TIMEOUT: u64 = 300;
const ELECTION_TIMEOUT_RANGE: Range<u64> = MIN_ELECTION_TIMEOUT..MAX_ELECTION_TIMEOUT;

pub trait StateMachine: Send {
    type Command: Send;
    type Result: Send;

    fn apply(&mut self, command: &Self::Command) -> Self::Result;
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(crate = "crlf::serde")]
pub enum KvCommand {
    Read { key: u64 },
    Write { key: u64, value: u64 },
}

pub type KvStateMachine = HashMap<u64, u64>;

impl StateMachine for KvStateMachine {
    type Command = KvCommand;
    type Result = Option<u64>;

    fn apply(&mut self, command: &KvCommand) -> Option<u64> {
        match command {
            KvCommand::Read { key } => self.get(key).map(Clone::clone),
            KvCommand::Write { key, value } => self.insert(*key, *value),
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
    log: Vec<(Term, STM::Command)>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct LeaderState<const N: usize> {
    //next_idx: [LogIdx; N],
    //match_idx: [LogIdx; N],
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum Role<const N: usize> {
    Follower,
    Candidate,
    Leader(LeaderState<N>),
}

#[service]
pub trait RaftSvc {
    fn append_entries(
        &mut self,
        term: crate::node::Term,
        leader_id: crate::node::NodeId,
        prev_log_idx: Option<crate::node::LogIdx>,
        prev_log_term: crate::node::Term,
        entries: Vec<(crate::node::Term, crate::node::KvCommand)>,
        leader_commit_idx: Option<crate::node::LogIdx>,
    ) -> (crate::node::Term, bool);

    fn request_vote(
        &mut self,
        term: crate::node::Term,
        candidate_id: crate::node::NodeId,
        log_len: usize,
        last_log_term: Option<crate::node::Term>,
    ) -> (crate::node::Term, bool);
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

    pub fn start(mut self) -> Result<(), Box<dyn Error>> {
        self.role = Role::Follower;

        let listener = TcpListener::bind(self.node_ids[0])?;

        let me = Arc::new(Mutex::new(self));

        let me2 = me.clone();
        thread::spawn(move || {
            let me = me2;
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

        let mut rng = rand::thread_rng();
        loop {
            let current_role: Role<N> = { me.lock().unwrap().role.clone() };
            match current_role {
                Role::Follower => {
                    let mut state = me.lock().unwrap();
                    if !(*state).tickled {
                        // Timeout! I should start an election
                        debug!("Timeout!");
                        state.role = Role::Candidate;
                    } else {
                        state.tickled = false;
			drop(state);
                        let ms = rng.gen_range(ELECTION_TIMEOUT_RANGE);
                        let timeout = Duration::from_millis(ms);
                        thread::sleep(timeout);
                    }
                }
                Role::Candidate => {
                    // Start election
                    let (
                        current_term,
                        new_term,
                        candidate_id,
                        log_len,
                        last_log_term,
                        other_nodes_ids,
                    ) = {
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

                    struct Updater {
                        new_term: Option<Term>,
                        for_votes: usize,
                        total_votes: usize,
                        timeout: bool,
                    }
                    let updater = Arc::new(Mutex::new(Updater {
                        new_term,
                        for_votes: 1,
                        total_votes: 1,
                        timeout: false,
                    }));
                    let cvar = Arc::new(Condvar::new());

                    for node_id in other_nodes_ids[1..].iter() {
                        let updater = updater.clone();
                        let cvar = cvar.clone();
                        let node_id = node_id.clone();
                        thread::spawn(move || {
                            let res: Result<(), Box<dyn Error>> = (|| {
                                let stream = TcpStream::connect(node_id)?;
                                let mut client = raft_svc::client::RaftSvc {
                                    sender: stream.try_clone()?,
                                    receiver: stream,
                                };
                                let (learned_term, success) = client
                                    .request_vote(
                                        current_term,
                                        candidate_id,
                                        log_len,
                                        last_log_term,
                                    )?;

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

		    let (stop_timeout, timeout) = channel();
                    {
                        let updater = updater.clone();
                        let cvar = cvar.clone();
                        thread::spawn(move || {
                            let mut rng = rand::thread_rng();
                            let ms = rng.gen_range(ELECTION_TIMEOUT_RANGE);
			    if let Err(_) = timeout.recv_timeout(Duration::from_millis(ms)) {
				let mut updated = updater.lock().unwrap();
				updated.timeout = true;
				cvar.notify_one();
			    }
                        });
                    }

                    let mut updated = updater.lock().unwrap();
                    // Wait for either all votes or majority votes
                    while updated.total_votes < N
                        && updated.for_votes < N / 2 + 1
                        && updated.new_term.is_none()
                        && !updated.timeout
                    {
                        updated = cvar.wait(updated).unwrap();
                    }

		    let _ = stop_timeout.send(());

                    if updated.for_votes >= N / 2 + 1 {
                        // I won! I am a leader!
			info!("Elected as leader");
                        let mut state = me.lock().unwrap();
                        if let Role::Candidate = state.role {
                            state.role = Role::Leader(LeaderState {
				//next_idx: [0; N],
				//match_idx: [0; N],
			    });
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
                            "I lost {:?} {} {} {}... what do I do now?",
                            updated.new_term,
                            updated.total_votes,
                            updated.for_votes,
                            updated.timeout
                        );
                    }
                }
                Role::Leader(_) => {
                    let (
                        term,
                        leader_id,
                        log_len,
                        last_log_term,
                        other_nodes_ids,
                        leader_commit_idx,
                    ) = {
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
                                    let stream = TcpStream::connect(node_id)?;
                                    let mut client = raft_svc::client::RaftSvc {
                                        sender: stream.try_clone()?,
                                        receiver: stream,
                                    };
                                    let (new_term, success) = client
                                        .append_entries(
                                            term,
                                            leader_id,
                                            prev_log_idx,
                                            last_log_term.unwrap_or(0),
                                            vec![],
                                            leader_commit_idx,
                                        )?;
                                    debug!("Send result {} {} {}", term, new_term, success);
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
        entries: Vec<(Term, KvCommand)>,
        leader_commit_idx: Option<LogIdx>,
    ) -> (Term, bool) {
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
        mut entries: Vec<(Term, KvCommand)>,
        leader_commit_idx: Option<LogIdx>,
    ) -> (Term, bool) {
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
            (self.current_term, false)
        } else if prev_log_idx
            .map(|pli| {
                self.log
                    .get(pli)
                    .map(|(t, _)| *t == prev_log_term)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
        {
            (self.current_term, false)
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
            (self.current_term, true)
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn append_entries_incompatible_log() {
        let mut node = RaftNode {
            node_ids: [([127, 0, 0, 1].into(), 1111), ([127,0,0,1].into(), 1112)],
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

	let (new_term, success) = node.append_entries(1, node.node_ids[1].clone(), None, 0, vec![], None);
        assert_eq!(new_term, 1, "Term");
        assert!(success, "Success");

        let node_expected = RaftNode {
            node_ids: [([127, 0, 0, 1].into(), 1111), ([127,0,0,1].into(), 1112)],
            tickled: true,
            role: Role::Follower,
            commit_idx: None,
            last_applied: None,
            current_leader_id: Some(([127, 0, 0, 1].into(), 1112)),
            state_machine: <KvStateMachine as Default>::default(),
            current_term: 1,
            voted_for: Some(([127, 0, 0, 1].into(), 1112)),
            log: Default::default(),
        };

	assert_eq!(node, node_expected);
    }
}
