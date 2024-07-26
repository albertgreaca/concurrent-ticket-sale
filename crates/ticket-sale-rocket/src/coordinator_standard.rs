//! Implementation of the standard coordinator

use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam::channel::{unbounded, Receiver, Sender};
use parking_lot::Mutex;
use rand::Rng;
use ticket_sale_core::Request;
use uuid::Uuid;

use super::database::Database;
use super::server_standard::ServerStandard;
use super::serverrequest::HighPriorityServerRequest;
use super::serverstatus::EstimatorServerStatus;
/// Coordinator orchestrating all the components of the system
pub struct CoordinatorStandard {
    database: Arc<Mutex<Database>>,

    /// The reservation timeout
    reservation_timeout: u32,

    /// Number of non-terminating servers
    pub no_active_servers: u32,

    /// Map between the id of a server and its index in the lists
    pub map_id_index: HashMap<Uuid, usize>,

    /// Lists containing the id, sender for low/high priority requests and thread for each
    /// server
    pub server_id_list: Vec<Uuid>,
    pub low_priority_sender_list: Vec<Sender<Request>>,
    high_priority_sender_list: Vec<Sender<HighPriorityServerRequest>>,
    thread_list: Vec<JoinHandle<()>>,

    /// Channel for notifying the coordinator of each server's termination
    coordinator_terminated_sender: Sender<Uuid>,
    coordinator_terminated_receiver: Receiver<Uuid>,

    /// Sender for servers to send their number of tickets to the estimator
    estimator_tickets_sender: Sender<u32>,

    /// Sender for servers to notify the estimator of their activation/termination
    estimator_scaling_sender: Sender<EstimatorServerStatus>,
}

impl CoordinatorStandard {
    /// Create the [`CoordinatorStandard`]
    pub fn new(
        database: Arc<Mutex<Database>>,
        reservation_timeout: u32,
        estimator_tickets_sender: Sender<u32>,
        estimator_scaling_sender: Sender<EstimatorServerStatus>,
    ) -> Self {
        let (coordinator_terminated_sender, coordinator_terminated_receiver) = unbounded();
        Self {
            database,
            reservation_timeout,
            no_active_servers: 0,
            map_id_index: HashMap::new(),
            server_id_list: Vec::new(),
            low_priority_sender_list: Vec::new(),
            high_priority_sender_list: Vec::new(),
            thread_list: Vec::new(),
            coordinator_terminated_sender,
            coordinator_terminated_receiver,
            estimator_tickets_sender,
            estimator_scaling_sender,
        }
    }

    /// Get the number of servers that are non-terminating
    pub fn get_num_active_servers(&self) -> u32 {
        self.no_active_servers
    }

    /// Get ids corresponding to non-terminating servers
    pub fn get_active_servers(&self) -> &[Uuid] {
        &self.server_id_list[0..self.no_active_servers as usize]
    }

    /// Get the id of a random non-terminating server
    pub fn get_random_server(&self) -> Uuid {
        let mut rng = rand::thread_rng();
        self.server_id_list[rng.gen_range(0..self.no_active_servers) as usize]
    }

    /// Get the channel for sending user requests to the server with the given id
    pub fn get_low_priority_sender(&self, id: Uuid) -> Sender<Request> {
        if self.map_id_index.contains_key(&id) {
            self.low_priority_sender_list[*self.map_id_index.get(&id).unwrap()].clone()
        } else {
            panic!("Our panic: Low priority sender not found.");
        }
    }

    /// Remove terminated servers from lists
    pub fn update_servers(&mut self) {
        // While there is a server that just terminated
        while let Ok(uuid) = self.coordinator_terminated_receiver.try_recv() {
            // Find its position in the lists
            let index = *self.map_id_index.get(&uuid).unwrap();
            let n = self.server_id_list.len();

            // If it is not the last one, swap it with the last one
            if index != n - 1 {
                self.server_id_list.swap(index, n - 1);
                self.low_priority_sender_list.swap(index, n - 1);
                self.high_priority_sender_list.swap(index, n - 1);
                self.thread_list.swap(index, n - 1);
                // Update the index of the swapped server
                *self
                    .map_id_index
                    .get_mut(&self.server_id_list[index])
                    .unwrap() = index;
            }

            // Remove the last server
            self.server_id_list.pop();
            self.low_priority_sender_list.pop();
            self.high_priority_sender_list.pop();
            self.thread_list.pop();
            self.map_id_index.remove(&uuid);
        }
    }

    /// Scale to the given number of servers
    pub fn scale_to(&mut self, num_servers: u32, coordinator: Arc<Mutex<CoordinatorStandard>>) {
        // Remove terminated servers
        self.update_servers();

        // We need to activate servers
        if self.no_active_servers < num_servers {
            // We activate existing servers
            while self.no_active_servers < num_servers
                && self.no_active_servers < self.server_id_list.len() as u32
            {
                // Get the channel for the server activation and activate the server
                let _ = self.high_priority_sender_list[self.no_active_servers as usize]
                    .send(HighPriorityServerRequest::Activate);

                // Notify the estimator of the server activation
                let _ = self
                    .estimator_scaling_sender
                    .send(EstimatorServerStatus::Activated {
                        server: self.server_id_list[self.no_active_servers as usize],
                        sender: self.high_priority_sender_list[self.no_active_servers as usize]
                            .clone(),
                    });

                self.no_active_servers += 1;
            }

            // We need to add more servers
            while self.no_active_servers < num_servers {
                // Create channels for the new server
                let (low_priority_sender, low_priority_receiver) = unbounded();
                let (high_priority_sender, high_priority_receiver) = unbounded();

                // Create the server
                let mut server = ServerStandard::new(
                    self.database.clone(),
                    coordinator.clone(),
                    self.reservation_timeout,
                    low_priority_receiver,
                    high_priority_receiver,
                    self.coordinator_terminated_sender.clone(),
                    self.estimator_tickets_sender.clone(),
                    self.estimator_scaling_sender.clone(),
                );
                let server_id = server.id;

                // Start the server
                self.thread_list.push(thread::spawn(move || server.run()));

                // Add everything to the lists
                self.server_id_list.push(server_id);
                self.low_priority_sender_list.push(low_priority_sender);
                self.high_priority_sender_list.push(high_priority_sender);
                self.map_id_index
                    .insert(server_id, self.no_active_servers as usize);

                // Notify the estimator of the server activation
                let _ = self
                    .estimator_scaling_sender
                    .send(EstimatorServerStatus::Activated {
                        server: self.server_id_list[self.no_active_servers as usize],
                        sender: self.high_priority_sender_list[self.no_active_servers as usize]
                            .clone(),
                    });
                self.no_active_servers += 1;
            }
        }

        // We need to deactivate servers
        if self.no_active_servers > num_servers {
            while self.no_active_servers > num_servers {
                // Get the channel for the server deactivation and deactivate the server
                let _ = self.high_priority_sender_list[(self.no_active_servers - 1) as usize]
                    .send(HighPriorityServerRequest::Deactivate);

                self.no_active_servers -= 1;
            }
        }
    }

    /// Shut down all servers
    pub fn shutdown(&mut self) {
        // Tell all servers to shut down
        for sender in self.high_priority_sender_list.iter() {
            let _ = sender.send(HighPriorityServerRequest::Shutdown);
        }
        // Wait for them to do so
        for thread in self.thread_list.drain(..) {
            thread.join().unwrap();
        }
    }
}
