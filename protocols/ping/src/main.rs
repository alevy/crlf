use std::{
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};

use crlf::{self, service};

#[service]
pub trait PingSvc {
    fn ping(&mut self, n: usize) -> usize;
}

pub struct PingImpl;

impl PingSvc for PingImpl {
    fn ping(&mut self, n: usize) -> usize {
        n
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
        payload: usize,
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
        Mode::Client {
            host,
            port,
            payload,
        } => {
            let write_stream = TcpStream::connect((host.as_str(), port))?;
            let read_stream = write_stream.try_clone()?;
            let mut client = client::PingSvc {
                sender: write_stream,
                receiver: read_stream,
            };
            let start = Instant::now();
            for _ in 0..100 {
                let r = client.ping(payload);
            }
            let end = start.elapsed();
            println!("{}ms:\t{}", end.as_micros() as f64 / 1000f64 / 100f64, 123);
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
