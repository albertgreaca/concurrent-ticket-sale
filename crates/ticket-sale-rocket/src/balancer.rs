//! Implementation of the load balancer
#![allow(clippy::map_entry)]
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;

use crossbeam::channel::Sender;
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
    other_thread: JoinHandle<()>,
    server_sender: Mutex<HashMap<Uuid, Sender<Option<Request>>>>,
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
            server_sender: Mutex::new(HashMap::new()),
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
                match rq.server_id() {
                    Some(x) => {
                        if self.server_sender.lock().contains_key(&x) {
                            let sender = self.server_sender.lock()[&x].clone();
                            let aux = sender.send(None);
                            match aux {
                                Ok(_) => {
                                    sender.send(Some(rq));
                                }
                                Err(_) => {
                                    let coordinator_guard = self.coordinator.lock();
                                    let x = coordinator_guard.get_random_server();
                                    rq.set_server_id(x);
                                    if !self.server_sender.lock().contains_key(&x) {
                                        self.server_sender.lock().insert(
                                            x,
                                            coordinator_guard.get_low_priority_sender(x),
                                        );
                                    }
                                    rq.respond_with_err(
                                        "No Ticket Reservation allowed anymore on this server",
                                    );
                                }
                            }
                        } else {
                            panic!("what the fuck");
                        }
                    }
                    None => {
                        let coordinator_guard = self.coordinator.lock();
                        let x = coordinator_guard.get_random_server();
                        rq.set_server_id(x);
                        if !self.server_sender.lock().contains_key(&x) {
                            self.server_sender
                                .lock()
                                .insert(x, coordinator_guard.get_low_priority_sender(x));
                        }
                        let server_sender = self.server_sender.lock()[&x].clone();
                        let _ = server_sender.send(Some(rq));
                    }
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
