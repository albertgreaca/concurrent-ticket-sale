//! Implementation of the standard balancer

use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;

use parking_lot::{Mutex, MutexGuard, RawMutex};
use ticket_sale_core::{Request, RequestHandler, RequestKind};
use uuid::Uuid;

use super::coordinator_standard::CoordinatorStandard;

pub struct BalancerStandard {
    coordinator: Arc<Mutex<CoordinatorStandard>>,

    // Sender for telling the estimator to shut down
    estimator_shutdown_sender: mpsc::Sender<()>,

    // Thread the estimator runs in
    estimator_thread: JoinHandle<()>,
}

impl BalancerStandard {
    /// Create a new [`BalancerStandard`]
    pub fn new(
        coordinator: Arc<Mutex<CoordinatorStandard>>,
        estimator_shutdown_sender: mpsc::Sender<()>,
        estimator_thread: JoinHandle<()>,
    ) -> Self {
        Self {
            coordinator,
            estimator_shutdown_sender,
            estimator_thread,
        }
    }

    /// Forward a user request to a given server
    fn send_to(
        &self,
        server: Uuid,
        rq: Request,
        coordinator_guard: MutexGuard<CoordinatorStandard>,
    ) {
        // Get the low priority sender channel for the server
        let sender = coordinator_guard.get_low_priority_sender(server);
        // Send the request
        let _ = sender.send(rq);
    }
}

impl RequestHandler for BalancerStandard {
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
                let mut coordinator_guard = self.coordinator.lock();
                match rq.server_id() {
                    // Request already has a server
                    Some(server) => {
                        // Update non-terminating servers in the coordinator
                        coordinator_guard.update_servers();
                        // Make sure assigned server still exists afterwards
                        if !coordinator_guard.map_id_index.contains_key(&server) {
                            // If not, assign a new server and respond with error
                            let new_server = coordinator_guard.get_random_server();
                            rq.set_server_id(new_server);
                            rq.respond_with_err("Our error: Server no longer exists.");
                        } else {
                            // If yes, forward the request to the server
                            self.send_to(server, rq, coordinator_guard);
                        }
                    }
                    // Request doesn't have a server
                    None => {
                        // Assign a server and forward the request to the server
                        let server = coordinator_guard.get_random_server();
                        rq.set_server_id(server);
                        self.send_to(server, rq, coordinator_guard);
                    }
                };
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
