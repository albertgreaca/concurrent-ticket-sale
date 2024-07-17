//! Implementation of the load balancer
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;

use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use super::coordinator::Coordinator;
/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    coordinator: Arc<Mutex<Coordinator>>,
    estimator_shutdown: mpsc::Sender<()>,
    estimator_thread: JoinHandle<()>,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(
        coordinator: Arc<Mutex<Coordinator>>,
        estimator_shutdown: mpsc::Sender<()>,
        estimator_thread: JoinHandle<()>,
    ) -> Self {
        Self {
            coordinator,
            estimator_shutdown,
            estimator_thread,
        }
    }
    /// forward a user request to a given server
    fn send_to(&self, server: Uuid, rq: Request) {
        // get the sender channel for the server
        let sender = self.coordinator.lock().get_low_priority_sender(server);
        // send the request
        let _ = sender.send(rq);
    }
}

impl RequestHandler for Balancer {
    // 📌 Hint: Look into the `RequestHandler` trait definition for specification
    // docstrings of `handle()` and `shutdown()`.

    fn handle(&self, mut rq: Request) {
        match rq.kind() {
            RequestKind::GetNumServers => {
                // get the number of servers with status 0
                rq.respond_with_int(self.coordinator.lock().get_num_active_servers());
            }
            RequestKind::GetServers => {
                // get the servers with status 0
                rq.respond_with_server_list(self.coordinator.lock().get_active_servers());
            }
            RequestKind::SetNumServers => {
                match rq.read_u32() {
                    Some(n) => {
                        // set number of servers with status 0 to n
                        self.coordinator
                            .lock()
                            .scale_to(n, self.coordinator.clone());
                        rq.respond_with_int(n);
                    }
                    None => {
                        rq.respond_with_err("Our error: No. of servers is None.");
                    }
                };
            }
            RequestKind::Debug => {
                // 📌 Hint: You can use `rq.url()` and `rq.method()` to
                // implement multiple debugging commands.
                rq.respond_with_string("Happy Debugging! 🚫🐛");
            }
            _ => {
                match rq.server_id() {
                    // request already has a server
                    Some(server) => {
                        // remove servers that terminated
                        self.coordinator.lock().update_servers();
                        // make sure assigned server still exists afterwards
                        if !self.coordinator.lock().map_id_index.contains_key(&server) {
                            // if not, assign a new server and respond with error
                            let new_server = self.coordinator.lock().get_random_server();
                            rq.set_server_id(new_server);
                            rq.respond_with_err("Our error: Server no longer exists.");
                        } else {
                            // if yes, forward the request to the server
                            self.send_to(server, rq);
                        }
                    }
                    // request doesn't have a server
                    None => {
                        // assign a server and forward the request to the server
                        let server = self.coordinator.lock().get_random_server();
                        rq.set_server_id(server);
                        self.send_to(server, rq);
                    }
                };
            }
        }
    }

    fn shutdown(self) {
        // tell the estimator to shut down
        let _ = self.estimator_shutdown.send(());
        self.estimator_thread.join().unwrap();
        // tell servers to shut down
        self.coordinator.lock().shutdown();
    }
}
