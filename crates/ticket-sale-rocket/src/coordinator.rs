//! Implementation of the coordinator
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam::channel::{unbounded, Receiver, Sender};
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use ticket_sale_core::Request;
use uuid::Uuid;

use super::database::Database;
use super::server::Server;
use crate::serverrequest::HighPriorityServerRequest;
/// Coordinator orchestrating all the components of the system
pub struct Coordinator {
    /// The reservation timeout
    reservation_timeout: u32,

    /// Reference to the [`Database`]
    ///
    /// To be handed over to new servers.
    database: Arc<Mutex<Database>>,

    /// number of non-terminating servers
    pub no_active_servers: u32,

    /// map between the id of a server and its index in the lists
    pub map_id_index: HashMap<Uuid, usize>,

    /// lists containing the id, sender for low/high priority requests and thread for each
    /// server
    pub server_id_list: Vec<Uuid>,
    pub low_priority_sender_list: Vec<Sender<Request>>,
    high_priority_sender_list: Vec<Sender<HighPriorityServerRequest>>,
    thread_list: Vec<JoinHandle<()>>,

    /// channel through which servers send their id once they have fully terminated
    terminated_sender: Sender<Uuid>,
    terminated_receiver: Receiver<Uuid>,

    terminated_sender2: Sender<Uuid>,

    /// channel through which servers send their number of tickets to the estimator
    estimator_sender: Sender<u32>,
}

impl Coordinator {
    /// Create the [`Coordinator`]
    pub fn new(
        reservation_timeout: u32,
        database: Arc<Mutex<Database>>,
        estimator_sender: Sender<u32>,
        terminated_sender2: Sender<Uuid>,
    ) -> Self {
        let (terminated_sender, terminated_receiver) = unbounded();
        Self {
            reservation_timeout,
            database,
            no_active_servers: 0,
            map_id_index: HashMap::new(),
            server_id_list: Vec::new(),
            low_priority_sender_list: Vec::new(),
            high_priority_sender_list: Vec::new(),
            thread_list: Vec::new(),
            terminated_sender,
            terminated_receiver,
            estimator_sender,
            terminated_sender2,
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

    ///remove terminated servers from lists
    pub fn update_servers(&mut self) {
        // while there is a server that just terminated
        while let Ok(uuid) = self.terminated_receiver.try_recv() {
            // find its position in the lists
            let index = *self.map_id_index.get(&uuid).unwrap();
            let n = self.server_id_list.len();

            // if it is not the last one, swap it with the last one
            if index != n - 1 {
                self.server_id_list.swap(index, n - 1);
                self.low_priority_sender_list.swap(index, n - 1);
                self.high_priority_sender_list.swap(index, n - 1);
                self.thread_list.swap(index, n - 1);
                // update the index of the swapped server
                *self
                    .map_id_index
                    .get_mut(&self.server_id_list[index])
                    .unwrap() = index;
            }

            // remove the last server
            self.server_id_list.pop();
            self.low_priority_sender_list.pop();
            self.high_priority_sender_list.pop();
            self.thread_list.pop();
            self.map_id_index.remove(&uuid);
        }
    }

    /// Scale to the given number of servers
    pub fn scale_to(&mut self, num_servers: u32, coordinator: Arc<RwLock<Coordinator>>) {
        // remove terminated servers
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
                self.no_active_servers += 1;
            }

            // We need to add more servers
            while self.no_active_servers < num_servers {
                // Create channels for the new server
                let (low_priority_sender, low_priority_receiver) = unbounded();
                let (high_priority_sender, high_priority_receiver) = unbounded();

                // Create the server
                let mut server = Server::new(
                    self.database.clone(),
                    coordinator.clone(),
                    self.reservation_timeout,
                    low_priority_receiver,
                    high_priority_receiver,
                    self.terminated_sender.clone(),
                    self.terminated_sender2.clone(),
                    self.estimator_sender.clone(),
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

    /// Get estimator ids and sender channels for servers that aren't completely
    /// terminated
    pub fn get_estimator(&mut self) -> (Vec<Uuid>, Vec<Sender<HighPriorityServerRequest>>) {
        self.update_servers();
        (
            self.server_id_list.clone(),
            self.high_priority_sender_list.clone(),
        )
    }

    /// Shut down all servers
    pub fn shutdown(&mut self) {
        for sender in self.high_priority_sender_list.iter() {
            let _ = sender.send(HighPriorityServerRequest::Shutdown);
        }
        for thread in self.thread_list.drain(..) {
            thread.join().unwrap();
        }
    }
}
