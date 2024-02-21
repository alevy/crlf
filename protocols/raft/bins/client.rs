use clap::{Args, Parser};
use std::{error::Error, net::IpAddr, net::TcpStream};

use crlf_raft::{raft_frontend::client::RaftFrontend, KvOperation};

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

    let transport = TcpStream::connect((cli.node_host, cli.node_ip))?;

    let mut client = RaftFrontend { transport };

    let result0 = client.do_op(KvOperation::Read { key: 123456 })?;
    let incr_val = result0.map(|r| r + 1).unwrap_or(0);
    println!("{:?}", result0);

    println!(
        "{:?}",
        client.do_op(KvOperation::Write {
            key: 123456,
            value: incr_val
        })?
    );

    println!("{:?}", client.do_op(KvOperation::Read { key: 123456 })?);
    Ok(())
}