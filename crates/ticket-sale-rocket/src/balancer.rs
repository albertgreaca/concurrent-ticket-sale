//! Implementation of the load balancer

use ticket_sale_core::{Request, RequestHandler, RequestKind};

/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new() -> Self {
        todo!()
    }
}

impl RequestHandler for Balancer {
    // 📌 Hint: Look into the `RequestHandler` trait definition for specification
    // docstrings of `handle()` and `shutdown()`.

    fn handle(&self, rq: Request) {
        match rq.kind() {
            RequestKind::GetNumServers => {
                todo!()
            }
            RequestKind::SetNumServers => {
                todo!()
            }
            RequestKind::GetServers => {
                todo!()
            }

            RequestKind::Debug => {
                // 📌 Hint: You can use `rq.url()` and `rq.method()` to
                // implement multiple debugging commands.
                rq.respond_with_string("Happy Debugging! 🚫🐛");
            }

            _ => {
                todo!()
            }
        }
    }

    fn shutdown(self) {
        todo!()
    }
}
