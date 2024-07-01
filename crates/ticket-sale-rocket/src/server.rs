//! Implementation of the server

use ticket_sale_core::Request;
use uuid::Uuid;

use super::database::Database;

/// A server in the ticket sales system
pub struct Server {
    /// The server's ID
    id: Uuid,

    /// The database
    database: Database,
}

impl Server {
    /// Create a new [`Server`]
    pub fn new(database: Database) -> Server {
        let id = Uuid::new_v4();
        Self { id, database }
    }

    /// Handle a [`Request`]
    fn handle_request(&mut self, rq: Request) {
        todo!()
    }
}
