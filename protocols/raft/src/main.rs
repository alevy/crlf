use clap::{Args, Parser};
use std::{net::IpAddr, error::Error};

pub mod node;

#[derive(Clone, Args)]
struct NodeId {
    ip: IpAddr,
    port: u16,
}

#[derive(Parser)]
struct Cli {
    myhost: IpAddr,
    myip: u16,

    node1_host: IpAddr,
    node1_ip: u16,

    node2_host: IpAddr,
    node2_ip: u16,
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let cli = Cli::parse();

    node::RaftNode::<node::KvStateMachine, 3>::new(
        [
            (cli.myhost, cli.myip),
            (cli.node1_host, cli.node1_ip),
            (cli.node2_host, cli.node2_ip),
        ],
        Default::default(),
    )
    .start()
}
