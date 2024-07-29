//! Implementation of the bonus balancer

#![allow(clippy::while_let_loop)]
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;

use crossbeam::channel::{Receiver, Sender};
use dashmap::DashMap;
use parking_lot::Mutex;
use rand::Rng;
use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use super::coordinator_bonus::CoordinatorBonus;
use super::enums::UserSessionStatus;

pub struct BalancerBonus {
    coordinator: Arc<Mutex<CoordinatorBonus>>,

    // Sender for telling the estimator to shut down
    estimator_shutdown_sender: mpsc::Sender<()>,

    // Thread the estimator runs in
    estimator_thread: JoinHandle<()>,

    // Maps from server id to its low priority sender
    server_sender: DashMap<Uuid, Sender<(Request, bool)>>,

    // Number of requests per customer
    no_requests: DashMap<Uuid, u32>,

    // Mapped value is 0 => no user session
    active_user_sessions: DashMap<Uuid, u32>,

    // Receiver for which users start/end a session
    user_session_receiver: Receiver<UserSessionStatus>,
}

impl BalancerBonus {
    /// Create a new [`BalancerBonus`]
    pub fn new(
        coordinator: Arc<Mutex<CoordinatorBonus>>,
        estimator_shutdown_sender: mpsc::Sender<()>,
        estimator_thread: JoinHandle<()>,
        user_session_receiver: Receiver<UserSessionStatus>,
    ) -> Self {
        Self {
            coordinator,
            estimator_shutdown_sender,
            estimator_thread,
            server_sender: DashMap::new(),
            no_requests: DashMap::new(),
            active_user_sessions: DashMap::new(),
            user_session_receiver,
        }
    }

    /// Get the id and low priority sender of a random server
    fn get_server_sender(&self) -> (Uuid, Sender<(Request, bool)>) {
        // Get the random pair from the coordinator
        let (server, sender) = self.coordinator.lock().get_random_server_sender();

        // If we don't store it yet, insert it
        if !self.server_sender.contains_key(&server) {
            self.server_sender.insert(server, sender.clone());
        }

        (server, sender)
    }

    /// Update the user sessions
    fn update_active_user_sessions(&self) {
        loop {
            // While there is an update
            match self.user_session_receiver.try_recv() {
                Ok(user_session_status) => {
                    // Add or remove the user
                    match user_session_status {
                        UserSessionStatus::Activated { user } => {
                            self.active_user_sessions
                                .entry(user)
                                .and_modify(|value| *value += 1)
                                .or_insert(1);
                        }
                        UserSessionStatus::Deactivated { user } => {
                            self.active_user_sessions
                                .entry(user)
                                .and_modify(|value| *value -= 1);
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
    }
}

impl RequestHandler for BalancerBonus {
    /// Handle a given request
    fn handle(&self, mut rq: Request) {
        match rq.kind() {
            RequestKind::GetNumServers => {
                // Get the number of non-terminating servers
                rq.respond_with_int(self.coordinator.lock().get_num_active_servers());
            }
            RequestKind::GetServers => {
                // Get the non-terminating servers
                rq.respond_with_server_list(self.coordinator.lock().get_active_servers());
            }
            RequestKind::SetNumServers => {
                match rq.read_u32() {
                    Some(n) => {
                        // Set number of active servers to n
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
                //self.update_active_user_sessions();

                let customer = rq.customer_id();

                self.no_requests
                    .entry(customer)
                    .and_modify(|value| *value += 1)
                    .or_insert(1);

                let mut rng = rand::thread_rng();
                let number = rng.gen_range(0..10000);

                // User is in an active session or unlucky => try to keep the same server
                //if self.active_user_sessions.lock().contains(&customer) || number >= 100 {
                match rq.server_id() {
                    // Request already has a server
                    Some(server) => {
                        // Get the low priority sender for this server
                        let sender = if self.server_sender.contains_key(&server) {
                            // If it is in the map, get it from there
                            self.server_sender.get(&server).unwrap().clone()
                        } else {
                            // Otherwise, get it from the coordinator
                            let aux = self.coordinator.lock().get_low_priority_sender(server);
                            // And insert it in the map
                            self.server_sender.insert(server, aux.clone());
                            aux
                        };
                        // Attempt to forward the request
                        let response = if *self.no_requests.get(&customer).unwrap() <= 100 {
                            sender.send((rq, false))
                        } else {
                            sender.send((rq, true))
                        };
                        match response {
                            Ok(_) => {}
                            Err(senderr) => {
                                // Not forwarded => server terminated => assign new server
                                let mut rq = senderr.into_inner().0;
                                let (server, _) = self.get_server_sender();
                                rq.set_server_id(server);
                                rq.respond_with_err("Our error: Server no longer exists.")
                            }
                        }
                    }
                    // Request doesn't have a server
                    None => {
                        // Assign a server and forward the request to the server
                        let (server, sender) = self.get_server_sender();
                        rq.set_server_id(server);
                        let _ = if *self.no_requests.get(&customer).unwrap() <= 100 {
                            sender.send((rq, false))
                        } else {
                            sender.send((rq, true))
                        };
                    }
                }
                //} else {
                //    // Assign a new server and forward the request to the server
                //    let (server, sender) = self.get_server_sender();
                //    rq.set_server_id(server);
                //    let _ = sender.send(rq);
                //}
            }
        }
    }

    /// Shut down the system
    fn shutdown(self) {
        // Tell the estimator to shut down
        let _ = self.estimator_shutdown_sender.send(());
        // Wait for it to finish
        self.estimator_thread.join().unwrap();
        // Tell servers to shut down
        self.coordinator.lock().shutdown();
    }
}
