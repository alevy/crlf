use clap::{Args, Parser};
use std::{net::IpAddr, net::TcpStream, error::Error};

use crlf_raft::{raft_frontend::client::RaftFrontend, KvCommand};

#[derive(Clone, Args)]
struct NodeId {
    ip: IpAddr,
    port: u16,
}

#[derive(Parser)]
struct Cli {
    node_host: IpAddr,
    node_ip: u16,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let stream = TcpStream::connect((cli.node_host, cli.node_ip))?;

    let mut client = RaftFrontend {
        sender: stream.try_clone().unwrap(),
        receiver: stream,
    };

    let result0 = client.do_op(KvCommand::Read { key: 123456 } )?;
    let incr_val = result0.map(|r| r + 1).unwrap_or(0);
    println!("{:?}", result0);

    println!("{:?}", client.do_op(KvCommand::Write { key: 123456, value: incr_val } )?);

    println!("{:?}", client.do_op(KvCommand::Read { key: 123456 } )?);
    Ok(())
}
