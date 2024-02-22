use std::{
    net::TcpStream,
    sync::{mpsc::{channel, Receiver, Sender}, Mutex, Arc},
    time::Duration,
};

pub use bincode;
pub use crlf_service::service;

pub extern crate serde;
pub extern crate serde_derive;
pub use serde_derive::{Deserialize, Serialize};

pub trait SendTransport<T> {
    type Token;

    fn send(&mut self, data: T) -> Result<Self::Token, (T, Box<(dyn std::error::Error + 'static)>)>;
}

impl<Message: serde::Serialize> SendTransport<Message> for TcpStream {
    type Token = ();
    fn send(&mut self, request: Message) -> Result<(), (Message, Box<(dyn std::error::Error + 'static)>)> {
        bincode::serialize_into(&mut *self, &request).map_err(|e| (request, e.into()))?;
        Ok(())
    }
}

pub trait RecvTransport<T> {
    type Token;
    fn recv(&mut self, token: Self::Token) -> Result<T, Box<(dyn std::error::Error + 'static)>>;
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
        let token = self.send(request).map_err(|e| e.1)?;
        Ok(self.recv(token)?)
    }
}

pub struct Reconnector<T> {
    inner: Option<T>,
    backoff_ms: u64,
    backoff_factor: u64,
    connector: Box<dyn Fn() -> Option<T> + Send>,
}

impl<T> Reconnector<T> {
    pub fn new<F: Fn() -> Option<T> + 'static + Send>(connector: F) -> Self {
	Self {
	    inner: None,
	    backoff_ms: 1,
	    backoff_factor: 2,
	    connector: Box::new(connector),
	}
    }

    pub fn ensure(&mut self) -> &mut T {
	loop {
	    match self.inner {
		Some(ref mut c) => {
		    return c
		},
		None => {
		    self.backoff_ms *= self.backoff_factor;
		    std::thread::sleep(Duration::from_millis(self.backoff_ms));
		    self.inner = (self.connector)();
		    self.backoff_ms = 1;
		}
	    }
	}
    }
}

impl<T: SendTransport<Message>, Message> SendTransport<Message> for Arc<Mutex<Reconnector<T>>> {
    type Token = T::Token;

    fn send(&mut self, mut request: Message) -> Result<Self::Token, (Message, Box<(dyn std::error::Error + 'static)>)> {
	let mut me = self.lock().unwrap_or_else(|e| e.into_inner());
	loop {
	    let transport = me.ensure();
	    match transport.send(request) {
		Ok(token) => return Ok(token),
		Err((req, _)) => {
		    me.inner = None;
		    request = req
		},
	    }
	}
    }
}

impl<T: RecvTransport<Message>, Message> RecvTransport<Message> for Arc<Mutex<Reconnector<T>>> {
    type Token = T::Token;

    fn recv(
        &mut self,
        token: Self::Token,
    ) -> Result<Message, Box<(dyn std::error::Error + 'static)>> {
	let mut me = self.lock().unwrap_or_else(|e| e.into_inner());
	let transport = me.ensure();
	Ok(transport.recv(token)?)
    }
}

#[derive(Debug)]
pub struct Pipelined<Request, Response> {
    request_sender: Sender<(Request, Sender<Response>)>,
}

impl<R1, R2> Clone for Pipelined<R1, R2> {
    fn clone(&self) -> Self {
        Self {
	    request_sender: self.request_sender.clone()
	}
    }
}

impl<Request: Send + 'static, Response: Send + 'static>
    Pipelined<Request, Response>
{
    pub fn new_client<ST: SendTransport<Request, Token = ()> + Send + 'static, RT: RecvTransport<Response, Token = ()> + Send + 'static>(mut send_transport: ST, mut recv_transport: RT) -> Self  where ST: {
        let (request_sender, request_reciever) = channel::<(Request, Sender<Response>)>();

        let (enqueue_token, dequeue_token) = channel();

        std::thread::spawn(move || {
            for (request, response_sender) in request_reciever {
                match send_transport.send(request) {
                    Err(_) => break,
                    Ok(token) => {
                        let _ = enqueue_token.send((response_sender, token));
                    }
                }
            }
            send_transport
        });

	std::thread::spawn(move || {
	    while let Ok((response_sender, token)) = dequeue_token.recv() {
		if let Ok(response) = recv_transport.recv(token) {
		    let _ = response_sender.send(response);
		}
	    }
	    recv_transport
	});

        Pipelined {
            request_sender,
        }
    }
}

impl<Request: 'static, Response: 'static> SendTransport<Request>
    for Pipelined<Request, Response>
{
    type Token = Receiver<Response>;

    fn send(
        &mut self,
        request: Request,
    ) -> Result<Receiver<Response>, (Request, Box<(dyn std::error::Error + 'static)>)> {
        let (response_sender, response_receiver) = channel();
        let _ = self.request_sender.send((request, response_sender));
        Ok(response_receiver)
    }
}

impl<Request: 'static, Response: 'static> RecvTransport<Response>
    for Pipelined<Request, Response>
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
