//! Implementation of the load balancer
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;

use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestHandler, RequestKind};

use super::coordinator::Coordinator;
/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    coordinator: Arc<Mutex<Coordinator>>,
    estimator_shutdown: mpsc::Sender<()>,
    other_thread: JoinHandle<()>,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(
        coordinator: Arc<Mutex<Coordinator>>,
        estimator_shutdown: mpsc::Sender<()>,
        other_thread: JoinHandle<()>,
    ) -> Self {
        Self {
            coordinator,
            estimator_shutdown,
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
                        rq.respond_with_err("no. of servers is None");
                    }
                };
            }
            RequestKind::Debug => {
                // 📌 Hint: You can use `rq.url()` and `rq.method()` to
                // implement multiple debugging commands.
                rq.respond_with_string("Happy Debugging! 🚫🐛");
            }
            _ => {
                // get server Uuid from request or assign new server
                let server_no = match rq.server_id() {
                    Some(n) => n,
                    None => {
                        if self.coordinator.lock().no_active_servers == 0 {
                            rq.respond_with_err("no server available");
                            return;
                        }
                        let x = self.coordinator.lock().get_random_server();
                        rq.set_server_id(x);
                        x
                    }
                };
                self.coordinator.lock().update_servers();
                if !self
                    .coordinator
                    .lock()
                    .map_id_index
                    .contains_key(&server_no)
                {
                    let x = self.coordinator.lock().get_random_server();
                    rq.set_server_id(x);
                    rq.respond_with_err("No Ticket Reservation allowed anymore on this server");
                } else {
                    // forward the request to the server
                    let server_sender = self.coordinator.lock().get_low_priority_sender(server_no);
                    let _ = server_sender.send(rq);
                }
            }
        }
    }

    fn shutdown(self) {
        self.estimator_shutdown.send(());
        self.other_thread.join().unwrap();
        self.coordinator.lock().shutdown();
    }
}
