//! Implementation of the load balancer

use std::io::{self, Write};
use std::sync::Arc;
use std::thread::JoinHandle;

use parking_lot::Mutex;
use rand::Rng;
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
    estimator_term: Arc<Mutex<u32>>,
    other_thread: JoinHandle<()>,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(
        coordinator: Arc<Mutex<Coordinator>>,
        estimator_term: Arc<Mutex<u32>>,
        other_thread: JoinHandle<()>,
    ) -> Self {
        Self {
            coordinator,
            estimator_term,
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
                rq.respond_with_server_list(
                    self.coordinator.lock().get_active_servers().as_slice(),
                );
            }
            RequestKind::SetNumServers => {
                match rq.read_u32() {
                    Some(n) => {
                        // set number of servers with status 0 to n
                        self.coordinator.lock().scale_to(n);
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
                let mut coordinator_guard = self.coordinator.lock();
                let server_no = match rq.server_id() {
                    Some(n) => n,
                    None => {
                        if *coordinator_guard.no_active_servers.lock() == 0 {
                            rq.respond_with_err("no server available");
                            return;
                        }
                        let x = coordinator_guard.get_random_server();
                        rq.set_server_id(x);
                        x
                    }
                };
                coordinator_guard.update_servers();
                if !coordinator_guard.map_id_index.contains_key(&server_no) {
                    let mut rng = rand::thread_rng();
                    let x = *coordinator_guard.no_active_servers.lock();
                    let new_serv =
                        coordinator_guard.server_id_list.lock()[rng.gen_range(0..x) as usize];
                    rq.set_server_id(new_serv);
                    rq.respond_with_err("No Ticket Reservation allowed anymore on this server");
                } else {
                    // forward the request to the server
                    let server_sender = coordinator_guard.get_low_priority_sender(server_no);
                    let _ = server_sender.send(rq);
                }
            }
        }
    }

    fn shutdown(self) {
        drop(self.estimator_term);
        self.other_thread.join().unwrap();
        self.coordinator.lock().shutdown();
    }
}
