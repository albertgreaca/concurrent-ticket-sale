//! Implementation of the load balancer

use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use ticket_sale_core::{Request, RequestHandler, RequestKind};

use super::coordinator::Coordinator;
use super::estimator::Estimator;
/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    coordinator: Arc<Mutex<Coordinator>>,
    estimator: Arc<Estimator>,
    other_thread: JoinHandle<()>,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(
        coordinator: Arc<Mutex<Coordinator>>,
        estimator: Arc<Estimator>,
        other_thread: JoinHandle<()>,
    ) -> Self {
        Self {
            coordinator,
            estimator,
            other_thread,
        }
    }
}

impl RequestHandler for Balancer {
    // 📌 Hint: Look into the `RequestHandler` trait definition for specification
    // docstrings of `handle()` and `shutdown()`.

    fn handle(&self, mut rq: Request) {
        match rq.kind() {
            RequestKind::GetNumServers => {
                rq.respond_with_int(self.coordinator.lock().unwrap().get_num_active_servers());
            }
            RequestKind::SetNumServers => {
                match rq.read_u32() {
                    Some(n) => {
                        self.coordinator.lock().unwrap().scale_to(n);
                        rq.respond_with_int(n);
                    }
                    None => {
                        rq.respond_with_err("no. of servers is None");
                    }
                };
            }
            RequestKind::GetServers => {
                rq.respond_with_server_list(self.coordinator.lock().unwrap().get_active_servers());
            }
            RequestKind::Debug => {
                // 📌 Hint: You can use `rq.url()` and `rq.method()` to
                // implement multiple debugging commands.
                rq.respond_with_string("Happy Debugging! 🚫🐛");
            }
            _ => {
                let server_no = match rq.server_id() {
                    Some(n) => n,
                    None => {
                        let guard = self.coordinator.lock().unwrap();
                        let x = guard.get_random_server();
                        rq.set_server_id(x);
                        x
                    }
                };
                let mut guard = self.coordinator.lock().unwrap();
                let server = guard.get_server_mut(server_no);
                if (*rq.kind() != RequestKind::ReserveTicket && server.get_status() == 1)
                    || server.get_status() == 0
                {
                    server.handle_request(rq);
                } else {
                    let x = guard.get_random_server();
                    rq.set_server_id(x);
                    let server = guard.get_server_mut(x);
                    server.handle_request(rq);
                }
                drop(guard);
            }
        }
    }

    fn shutdown(self) {
        drop(self.estimator);
        self.other_thread.join().unwrap();
    }
}
