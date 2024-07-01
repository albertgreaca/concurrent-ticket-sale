//! 🏗 Infrastructure for handling requests, etc.
#![warn(missing_docs)]

mod request;

pub use request::{RawRequest, Request, RequestHandler, RequestKind, RequestMethod};

/// Configuration of the ticket sales system
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Amount of initially available tickets
    pub tickets: u32,
    /// Timeout in seconds after which reservations expire
    pub timeout: u32,
    /// Number of initial servers
    pub initial_servers: u32,
    /// Time in seconds the estimator takes to contact all servers
    pub estimator_roundtrip_time: u32,

    /// Run the implementation for the bonus exercise
    pub bonus: bool,
}
