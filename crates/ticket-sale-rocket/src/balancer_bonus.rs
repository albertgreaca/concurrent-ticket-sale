//! Implementation of the load balancer
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;

use crossbeam::channel::Sender;
use dashmap::DashMap;
use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use super::coordinator_bonus::CoordinatorBonus;
/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct BalancerBonus {
    coordinator: Arc<Mutex<CoordinatorBonus>>,
    estimator_shutdown: mpsc::Sender<()>,
    estimator_thread: JoinHandle<()>,
    server_sender: DashMap<Uuid, Sender<Request>>,
}

impl BalancerBonus {
    /// Create a new [`Balancer`]
    pub fn new(
        coordinator: Arc<Mutex<CoordinatorBonus>>,
        estimator_shutdown: mpsc::Sender<()>,
        estimator_thread: JoinHandle<()>,
    ) -> Self {
        Self {
            coordinator,
            estimator_shutdown,
            estimator_thread,
            server_sender: DashMap::new(),
        }
    }
    /// assign a server and forward the request to the server
    fn get_server_sender(&self) -> (Uuid, Sender<Request>) {
        let (server, sender) = self.coordinator.lock().get_random_server_sender();
        if !self.server_sender.contains_key(&server) {
            self.server_sender.insert(server, sender.clone());
        }
        (server, sender)
    }
}

impl RequestHandler for BalancerBonus {
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
                        let sender = if self.server_sender.contains_key(&server) {
                            self.server_sender.get(&server).unwrap().clone()
                        } else {
                            let aux = self.coordinator.lock().get_low_priority_sender(server);
                            self.server_sender.insert(server, aux.clone());
                            aux
                        };
                        let response = sender.send(rq);
                        match response {
                            Ok(_) => {}
                            Err(senderr) => {
                                let mut rq = senderr.into_inner();
                                let (server, _) = self.get_server_sender();
                                rq.set_server_id(server);
                                rq.respond_with_err("Our error: Server no longer exists.")
                            }
                        }
                    }
                    // request doesn't have a server
                    None => {
                        let (server, sender) = self.get_server_sender();
                        rq.set_server_id(server);
                        let _ = sender.send(rq);
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
