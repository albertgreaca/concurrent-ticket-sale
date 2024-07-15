//! Implementation of the load balancer
use std::collections::HashMap;
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;

use crossbeam::channel::{Receiver, Sender};
use parking_lot::Mutex;
use rand::Rng;
use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use crate::coordinator::Coordinator;
/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    coordinator: Arc<Mutex<Coordinator>>,
    estimator_shutdown: mpsc::Sender<()>,
    estimator_thread: JoinHandle<()>,
    no_active_servers: Mutex<u32>,
    pub map_id_index: Mutex<HashMap<Uuid, usize>>,
    server_id_list: Mutex<Vec<Uuid>>,
    low_priority_sender_list: Mutex<Vec<Sender<Request>>>,
    terminated_receiver: Receiver<Uuid>,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(
        coordinator: Arc<Mutex<Coordinator>>,
        estimator_shutdown: mpsc::Sender<()>,
        estimator_thread: JoinHandle<()>,
        no_active_servers: Mutex<u32>,
        map_id_index: Mutex<HashMap<Uuid, usize>>,
        server_id_list: Mutex<Vec<Uuid>>,
        low_priority_sender_list: Mutex<Vec<Sender<Request>>>,
        terminated_receiver: Receiver<Uuid>,
    ) -> Self {
        Self {
            coordinator,
            estimator_shutdown,
            estimator_thread,
            no_active_servers,
            map_id_index,
            server_id_list,
            low_priority_sender_list,
            terminated_receiver,
        }
    }
    /// forward a user request to a given server
    fn send_to(&self, server: Uuid, rq: Request) {
        // get the sender channel for the server
        let sender =
            self.low_priority_sender_list.lock()[self.map_id_index.lock()[&server]].clone();
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
                rq.respond_with_int(*self.no_active_servers.lock());
            }
            RequestKind::GetServers => {
                // get the servers with status 0
                rq.respond_with_server_list(
                    &self.server_id_list.lock()[0..*self.no_active_servers.lock() as usize],
                );
            }
            RequestKind::SetNumServers => {
                match rq.read_u32() {
                    Some(n) => {
                        // set number of servers with status 0 to n
                        self.coordinator
                            .lock()
                            .scale_to(n, self.coordinator.clone());
                        *self.no_active_servers.lock() = n;
                        *self.server_id_list.lock() =
                            self.coordinator.lock().server_id_list.clone();
                        *self.map_id_index.lock() = self.coordinator.lock().map_id_index.clone();
                        *self.low_priority_sender_list.lock() =
                            self.coordinator.lock().low_priority_sender_list.clone();
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
                        while let Ok(uuid) = self.terminated_receiver.try_recv() {
                            self.map_id_index.lock().remove(&uuid);
                        }
                        // make sure assigned server still exists afterwards
                        if !self.map_id_index.lock().contains_key(&server) {
                            // if not, assign a new server and respond with error
                            let mut rng = rand::thread_rng();
                            let new_server = self.server_id_list.lock()
                                [rng.gen_range(0..*self.no_active_servers.lock()) as usize];
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
                        let mut rng = rand::thread_rng();
                        let server = self.server_id_list.lock()
                            [rng.gen_range(0..*self.no_active_servers.lock()) as usize];
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
