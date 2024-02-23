fn main() -> Result<(), Box<dyn std::error::Error>> {

    std::thread::scope(|t| {
	let mut nodes = vec![
		([127, 0, 0, 1].into(), 1111),
		([127, 0, 0, 1].into(), 1112),
		([127, 0, 0, 1].into(), 1113),
	    ];


	for _ in 0..nodes.len() {
	    let ns = nodes.clone();
	    t.spawn(move || {
		crlf_raft::RaftNode::<crlf_raft::KvStateMachine>::new(
		    ns,
		    Default::default(),
		)
		.start().unwrap();
	    });
	    nodes.rotate_left(1);
	}
    });

    Ok(())
}
