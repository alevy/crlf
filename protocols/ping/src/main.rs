use std::{
    net::{TcpListener, TcpStream},
    time::{Instant, SystemTime},
};

use crlf::{self, service};

#[service]
pub trait PingSvc {
    fn ping(&mut self) -> std::time::SystemTime;
}

pub struct PingImpl;

impl PingSvc for PingImpl {
    fn ping(&mut self) -> SystemTime {
        SystemTime::now()
    }
}

#[derive(clap::Parser)]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(clap::Subcommand)]
enum Mode {
    Client {
        host: String,
        port: u16,
        count: usize,
    },
    Server {
        port: u16,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use clap::Parser;
    let cli = Cli::parse();
    match cli.mode {
        Mode::Server { port } => {
            let listener = TcpListener::bind(("0.0.0.0", port))?;
            for connection in listener.incoming() {
                if let Ok(write_stream) = connection {
                    write_stream.set_nodelay(true)?;
                    let read_stream = write_stream.try_clone()?;
                    std::thread::spawn(move || {
                        server::PingSvc {
                            sender: write_stream,
                            receiver: read_stream,
                            inner: PingImpl,
                        }
                        .run()
                    });
                }
            }
        }
        Mode::Client { host, port, count } => {
            let write_stream = TcpStream::connect((host.as_str(), port))?;
            write_stream.set_nodelay(true)?;
            let read_stream = write_stream.try_clone()?;
            let mut client = client::PingSvc {
                sender: write_stream,
                receiver: read_stream,
            };
            let mut i = 0;
            for _ in 0..count {
                i += 1;
                let start = Instant::now();
                let dt = client
                    .ping()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_micros();
                let end = start.elapsed().as_micros();
                println!("{i:2} {end:5}uS at {dt:?}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let (s1, r1) = std::os::unix::net::UnixStream::pair().unwrap();
        let (s2, r2) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut myclient = client::PingSvc {
            sender: s1,
            receiver: r2,
        };
        std::thread::spawn(move || {
            let mut myserver = server::PingSvc {
                sender: s2,
                receiver: r1,
                inner: PingImpl,
            };

            myserver.run();
        });
        let result = myclient.ping(123);
        assert_eq!(123, result);
    }
}
