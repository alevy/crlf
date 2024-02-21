use std::{
    net::TcpStream,
    sync::mpsc::{channel, Receiver, Sender},
    thread::JoinHandle,
};

pub use bincode;
pub use crlf_service::service;

pub extern crate serde;
pub extern crate serde_derive;
pub use serde_derive::{Deserialize, Serialize};

pub trait SendTransport<T> {
    type Token;

    fn send(&mut self, data: T) -> Result<Self::Token, Box<(dyn std::error::Error + 'static)>>;
}

pub trait RecvTransport<T> {
    type Token;
    fn recv(&mut self, token: Self::Token) -> Result<T, Box<(dyn std::error::Error + 'static)>>;
}

impl<Message: serde::Serialize> SendTransport<Message> for TcpStream {
    type Token = ();
    fn send(&mut self, request: Message) -> Result<(), Box<(dyn std::error::Error + 'static)>> {
        bincode::serialize_into(&mut *self, &request)?;
        Ok(())
    }
}

impl<Message: serde::de::DeserializeOwned> RecvTransport<Message> for TcpStream {
    type Token = ();

    fn recv(&mut self, _token: ()) -> Result<Message, Box<(dyn std::error::Error + 'static)>> {
        let response = bincode::deserialize_from(&mut *self)?;
        Ok(response)
    }
}

pub trait ClientTransport<Request, Response> {
    fn invoke(
        &mut self,
        request: Request,
    ) -> Result<Response, Box<(dyn std::error::Error + 'static)>>;
}

impl<I, Request, Response> ClientTransport<Request, Response> for I
where
    I: SendTransport<Request, Token = <I as RecvTransport<Response>>::Token>
        + RecvTransport<Response>,
{
    fn invoke(
        &mut self,
        request: Request,
    ) -> Result<Response, Box<(dyn std::error::Error + 'static)>> {
        let token = self.send(request)?;
        Ok(self.recv(token)?)
    }
}

pub struct Pipelined<ST, RT, Request, Response> {
    send_runner: JoinHandle<ST>,
    recieve_runner: JoinHandle<RT>,
    request_sender: Sender<(Request, Sender<Response>)>,
}

impl<ST: Send + 'static, RT: Send + 'static, Request: Send + 'static, Response: Send + 'static>
    Pipelined<ST, RT, Request, Response>
where
    ST: SendTransport<Request, Token = ()>,
    RT: RecvTransport<Response, Token = ()>,
{
    pub fn new_client(mut send_transport: ST, mut recv_transport: RT) -> Self {
        let (request_sender, request_reciever) = channel::<(Request, Sender<Response>)>();

        let (enqueue_token, dequeue_token) = channel();

        let send_runner = std::thread::spawn(move || {
            for (request, response_sender) in request_reciever {
                match send_transport.send(request) {
                    Err(_) => break,
                    Ok(token) => {
                        enqueue_token.send((response_sender, token)).unwrap();
                    }
                }
            }
            send_transport
        });

        let recieve_runner = {
            std::thread::spawn(move || {
                while let Ok((response_sender, token)) = dequeue_token.recv() {
                    let response = recv_transport.recv(token).unwrap();
                    let _ = response_sender.send(response);
                }
                recv_transport
            })
        };

        Pipelined {
            send_runner,
            recieve_runner,
            request_sender,
        }
    }

    pub fn wait(self) -> std::thread::Result<(ST, RT)> {
        let sender = self.send_runner.join()?;
        let receiver = self.recieve_runner.join()?;
        Ok((sender, receiver))
    }
}

impl<Request: 'static, Response: 'static, ST, RT> SendTransport<Request>
    for Pipelined<ST, RT, Request, Response>
where
    ST: SendTransport<Request> + Send,
{
    type Token = Receiver<Response>;

    fn send(
        &mut self,
        request: Request,
    ) -> Result<Receiver<Response>, Box<(dyn std::error::Error + 'static)>> {
        let (response_sender, response_receiver) = channel();
        self.request_sender.send((request, response_sender))?;
        Ok(response_receiver)
    }
}

impl<Request: 'static, Response: 'static, ST, RT> RecvTransport<Response>
    for Pipelined<ST, RT, Request, Response>
where
    ST: SendTransport<Request> + Send,
{
    type Token = Receiver<Response>;
    fn recv(
        &mut self,
        token: Receiver<Response>,
    ) -> Result<Response, Box<(dyn std::error::Error + 'static)>> {
        Ok(token.recv()?)
    }
}

#[cfg(test)]
mod tests {}
