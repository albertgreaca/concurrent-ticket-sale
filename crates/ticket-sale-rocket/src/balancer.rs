//! Implementation of the load balancer

use std::sync::Arc;
use std::thread::JoinHandle;

use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestHandler, RequestKind};

use super::coordinator::Coordinator;
use super::estimator::Estimator;
/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    coordinator: Arc<Coordinator>,
    estimator: Arc<Mutex<Estimator>>,
    other_thread: JoinHandle<()>,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(
        coordinator: Arc<Coordinator>,
        estimator: Arc<Mutex<Estimator>>,
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
                rq.respond_with_int(self.coordinator.get_num_active_servers());
            }
            RequestKind::SetNumServers => {
                match rq.read_u32() {
                    Some(n) => {
                        self.coordinator.scale_to(n);
                        rq.respond_with_int(n);
                    }
                    None => {
                        rq.respond_with_err("no. of servers is None");
                    }
                };
            }
            RequestKind::GetServers => {
                rq.respond_with_server_list(self.coordinator.get_active_servers().as_slice());
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
                        if *self.coordinator.no_active_servers.lock() == 0 {
                            rq.respond_with_err("no server available");
                            return;
                        }
                        let x = self.coordinator.get_random_server();
                        rq.set_server_id(x);
                        x
                    }
                };
                let server = self.coordinator.get_server(server_no);
                if (*rq.kind() != RequestKind::ReserveTicket && server.lock().get_status() == 1)
                    || server.lock().get_status() == 0
                {
                    server.lock().handle_request(rq);
                } else {
                    if *self.coordinator.no_active_servers.lock() == 0 {
                        rq.respond_with_err("no server available");
                        return;
                    }
                    let x = self.coordinator.get_random_server();
                    rq.set_server_id(x);
                    rq.respond_with_err("server terminating");
                    //let server = guard.get_server_mut(x);
                    //server.handle_request(rq);
                }
            }
        }
    }

    fn shutdown(self) {
        drop(self.estimator);
        self.other_thread.join().unwrap();
    }
}
