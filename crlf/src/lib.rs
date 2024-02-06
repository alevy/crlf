pub use bincode;
pub use crlf_service::service;

pub extern crate serde;
pub extern crate serde_derive;
pub use serde_derive::{Deserialize, Serialize};

#[cfg(test)]
mod tests {}
