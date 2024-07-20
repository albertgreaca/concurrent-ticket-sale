//! Implementation of the load balancer
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;

use crossbeam::channel::Sender;
use dashmap::DashMap;
use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use super::coordinator::Coordinator;
use super::coordinator2::Coordinator2;
/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    coordinator_option: Option<Arc<Mutex<Coordinator>>>,
    coordinator2_option: Option<Arc<Coordinator2>>,
    estimator_shutdown: mpsc::Sender<()>,
    estimator_thread: JoinHandle<()>,
    server_sender: DashMap<Uuid, Sender<Request>>,
    bonus: bool,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(
        coordinator_option: Option<Arc<Mutex<Coordinator>>>,
        coordinator2_option: Option<Arc<Coordinator2>>,
        estimator_shutdown: mpsc::Sender<()>,
        estimator_thread: JoinHandle<()>,
        bonus: bool,
    ) -> Self {
        Self {
            coordinator_option,
            coordinator2_option,
            estimator_shutdown,
            estimator_thread,
            server_sender: DashMap::new(),
            bonus,
        }
    }
    /// assign a server and forward the request to the server
    fn get_server_sender(&self) -> (Uuid, Sender<Request>) {
        let (server, sender) = self
            .coordinator_option
            .clone()
            .unwrap()
            .lock()
            .get_random_server_sender();
        if !self.server_sender.contains_key(&server) {
            self.server_sender.insert(server, sender.clone());
        }
        (server, sender)
    }

    /// forward a user request to a given server
    fn send_to2(&self, server: Uuid, rq: Request) {
        let coordinator2 = self.coordinator2_option.clone().unwrap();
        // get the sender channel for the server
        let sender = coordinator2.get_low_priority_sender(server);
        // send the request
        let _ = sender.send(rq);
    }
}

impl RequestHandler for Balancer {
    // 📌 Hint: Look into the `RequestHandler` trait definition for specification
    // docstrings of `handle()` and `shutdown()`.

    fn handle(&self, mut rq: Request) {
        if !self.bonus {
            match rq.kind() {
                RequestKind::GetNumServers => {
                    // get the number of servers with status 0
                    rq.respond_with_int(
                        self.coordinator_option
                            .clone()
                            .unwrap()
                            .lock()
                            .get_num_active_servers(),
                    );
                }
                RequestKind::GetServers => {
                    // get the servers with status 0
                    rq.respond_with_server_list(
                        self.coordinator_option
                            .clone()
                            .unwrap()
                            .lock()
                            .get_active_servers(),
                    );
                }
                RequestKind::SetNumServers => {
                    let coordinator = self.coordinator_option.clone().unwrap();
                    match rq.read_u32() {
                        Some(n) => {
                            // set number of servers with status 0 to n
                            coordinator.lock().scale_to(n, coordinator.clone());
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
                    let coordinator = self.coordinator_option.clone().unwrap();
                    match rq.server_id() {
                        // request already has a server
                        Some(server) => {
                            let sender = if self.server_sender.contains_key(&server) {
                                self.server_sender.get(&server).unwrap().clone()
                            } else {
                                let aux = coordinator.lock().get_low_priority_sender(server);
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
        } else {
            match rq.kind() {
                RequestKind::GetNumServers => {
                    // get the number of servers with status 0
                    rq.respond_with_int(
                        self.coordinator2_option
                            .clone()
                            .unwrap()
                            .get_num_active_servers(),
                    );
                }
                RequestKind::GetServers => {
                    // get the servers with status 0
                    rq.respond_with_server_list(
                        self.coordinator2_option
                            .clone()
                            .unwrap()
                            .get_active_servers()
                            .as_slice(),
                    );
                }
                RequestKind::SetNumServers => {
                    let coordinator2 = self.coordinator2_option.clone().unwrap();
                    match rq.read_u32() {
                        Some(n) => {
                            // set number of servers with status 0 to n
                            coordinator2.scale_to(n, coordinator2.clone());
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
                    let coordinator2 = self.coordinator2_option.clone().unwrap();
                    match rq.server_id() {
                        // request already has a server
                        Some(server) => {
                            // remove servers that terminated
                            coordinator2.update_servers();
                            // make sure assigned server still exists afterwards
                            if !coordinator2.map_id_index.lock().contains_key(&server) {
                                // if not, assign a new server and respond with error
                                let new_server = coordinator2.get_random_server();
                                rq.set_server_id(new_server);
                                rq.respond_with_err("Our error: Server no longer exists.");
                            } else {
                                // if yes, forward the request to the server
                                self.send_to2(server, rq);
                            }
                        }
                        // request doesn't have a server
                        None => {
                            // assign a server and forward the request to the server
                            let server = coordinator2.get_random_server();
                            rq.set_server_id(server);
                            self.send_to2(server, rq);
                        }
                    };
                }
            }
        }
    }

    fn shutdown(self) {
        if !self.bonus {
            // tell the estimator to shut down
            let _ = self.estimator_shutdown.send(());
            self.estimator_thread.join().unwrap();
            // tell servers to shut down
            self.coordinator_option.clone().unwrap().lock().shutdown();
        } else {
            // tell the estimator to shut down
            let _ = self.estimator_shutdown.send(());
            self.estimator_thread.join().unwrap();
            // tell servers to shut down
            self.coordinator2_option.clone().unwrap().shutdown();
        }
    }
}
