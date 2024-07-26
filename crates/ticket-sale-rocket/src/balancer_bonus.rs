//! Implementation of the bonus balancer

use std::collections::HashSet;
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc};
use crossbeam::channel::Receiver as crossbeam_recv;
use std::thread::JoinHandle;

use crossbeam::channel::Sender;
use dashmap::DashMap;
use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use super::coordinator_bonus::CoordinatorBonus;

pub struct BalancerBonus {
    coordinator: Arc<Mutex<CoordinatorBonus>>,

    // Sender for telling the estimator to shut down
    estimator_shutdown_sender: mpsc::Sender<()>,

    // Thread the estimator runs in
    estimator_thread: JoinHandle<()>,

    // Maps from server id to its low priority sender
    server_sender: DashMap<Uuid, Sender<Request>>,
    
    active_user_sessions: Mutex<HashSet<Uuid>>,
    user_session_finished_recv: crossbeam_recv<Uuid>,

}

impl BalancerBonus {
    /// Create a new [`BalancerBonus`]
    pub fn new(
        coordinator: Arc<Mutex<CoordinatorBonus>>,
        estimator_shutdown_sender: mpsc::Sender<()>,
        estimator_thread: JoinHandle<()>,
        user_session_finished_recv: crossbeam_recv<Uuid>
    ) -> Self {
        Self {
            coordinator,
            estimator_shutdown_sender,
            estimator_thread,
            server_sender: DashMap::new(),
            active_user_sessions: Mutex::new(HashSet::new()),
            user_session_finished_recv,
        }
    }

    /// Get the id and low priority sender of a random server
    fn get_server_sender(&self) -> (Uuid, Sender<Request>) {
        // Get the random pair from the coordinator
        let (server, sender) = self.coordinator.lock().get_random_server_sender();

        // If we don't store it yet, insert it
        if !self.server_sender.contains_key(&server) {
            self.server_sender.insert(server, sender.clone());
        }

        (server, sender)
    }
    fn update_active_user_sessions(&self) {
        loop {
            match self.user_session_finished_recv.try_recv() {
                Ok(user_id) => {
                    self.active_user_sessions.lock().remove(&user_id);
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
                let cust = rq.customer_id();
                self.update_active_user_sessions();
                if self.active_user_sessions.lock().contains(&cust) {
                    match rq.server_id() {
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
                        let response = sender.send(rq);
                       
                        match response {
                            Ok(_) => {}
                            Err(senderr) => {
                                // Not forwarded => server terminated => assign new server
                                let mut rq = senderr.into_inner();
                                let (server, _) = self.get_server_sender();
                                rq.set_server_id(server);
                                rq.respond_with_err("Our error: Server no longer exists.")
                            }
                        }

                    }

                    None => {
                        panic!("User Session is on server, but request has no server id! Should never happen.")
                    }
                }
            } else {
                let (server, sender) = self.get_server_sender();
                self.active_user_sessions.lock().insert(cust);
                rq.set_server_id(server);
                let _ = sender.send(rq);
            }
            }
        }
    }
  

    fn shutdown(self) {
        // Tell the estimator to shut down
        let _ = self.estimator_shutdown_sender.send(());
        // Wait for it to finish
        self.estimator_thread.join().unwrap();
        // Tell servers to shut down
        self.coordinator.lock().shutdown();
    }
}
