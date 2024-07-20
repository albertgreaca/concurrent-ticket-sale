//! Implementation of the coordinator
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam::channel::{unbounded, Receiver, Sender};
use parking_lot::Mutex;
use rand::Rng;
use ticket_sale_core::Request;
use uuid::Uuid;

use super::database::Database;
use super::server2::Server2;
use crate::serverrequest::HighPriorityServerRequest;
/// Coordinator orchestrating all the components of the system
pub struct Coordinator2 {
    /// The reservation timeout
    reservation_timeout: u32,

    /// Reference to the [`Database`]
    ///
    /// To be handed over to new servers.
    database: Arc<Mutex<Database>>,

    /// number of non-terminating servers
    pub no_active_servers: Mutex<u32>,

    /// map between the id of a server and its index in the lists
    pub map_id_index: Mutex<HashMap<Uuid, usize>>,

    /// lists containing the id, sender for low/high priority requests and thread for each
    /// server
    pub server_id_list: Mutex<Vec<Uuid>>,
    pub low_priority_sender_list: Mutex<Vec<Sender<Request>>>,
    high_priority_sender_list: Mutex<Vec<Sender<HighPriorityServerRequest>>>,
    thread_list: Mutex<Vec<JoinHandle<()>>>,

    /// channel through which servers send their id once they have fully terminated
    terminated_sender: Sender<Uuid>,
    terminated_receiver: Receiver<Uuid>,

    /// channel through which servers send their number of tickets to the estimator
    estimator_sender: Sender<u32>,
}

impl Coordinator2 {
    /// Create the [`Coordinator`]
    pub fn new(
        reservation_timeout: u32,
        database: Arc<Mutex<Database>>,
        estimator_sender: Sender<u32>,
    ) -> Self {
        let (terminated_sender, terminated_receiver) = unbounded();
        Self {
            reservation_timeout,
            database,
            no_active_servers: Mutex::new(0),
            map_id_index: Mutex::new(HashMap::new()),
            server_id_list: Mutex::new(Vec::new()),
            low_priority_sender_list: Mutex::new(Vec::new()),
            high_priority_sender_list: Mutex::new(Vec::new()),
            thread_list: Mutex::new(Vec::new()),
            terminated_sender,
            terminated_receiver,
            estimator_sender,
        }
    }

    /// Get the number of servers that are non-terminating
    pub fn get_num_active_servers(&self) -> u32 {
        *self.no_active_servers.lock()
    }

    /// Get ids corresponding to non-terminating servers
    pub fn get_active_servers(&self) -> Vec<Uuid> {
        self.server_id_list.lock()[0..*self.no_active_servers.lock() as usize].to_vec()
    }

    /// Get the id of a random non-terminating server
    pub fn get_random_server(&self) -> Uuid {
        let mut rng = rand::thread_rng();
        self.server_id_list.lock()[rng.gen_range(0..*self.no_active_servers.lock()) as usize]
    }

    /// Get the channel for sending user requests to the server with the given id
    pub fn get_low_priority_sender(&self, id: Uuid) -> Sender<Request> {
        if self.map_id_index.lock().contains_key(&id) {
            self.low_priority_sender_list.lock()[*self.map_id_index.lock().get(&id).unwrap()]
                .clone()
        } else {
            panic!("Our panic: Low priority sender not found.");
        }
    }

    ///remove terminated servers from lists
    pub fn update_servers(&self) {
        // while there is a server that just terminated
        while let Ok(uuid) = self.terminated_receiver.try_recv() {
            // find its position in the lists
            let index = *self.map_id_index.lock().get(&uuid).unwrap();
            let n = self.server_id_list.lock().len();

            // if it is not the last one, swap it with the last one
            if index != n - 1 {
                self.server_id_list.lock().swap(index, n - 1);
                self.low_priority_sender_list.lock().swap(index, n - 1);
                self.high_priority_sender_list.lock().swap(index, n - 1);
                self.thread_list.lock().swap(index, n - 1);
                // update the index of the swapped server
                *self
                    .map_id_index
                    .lock()
                    .get_mut(&self.server_id_list.lock()[index])
                    .unwrap() = index;
            }

            // remove the last server
            self.server_id_list.lock().pop();
            self.low_priority_sender_list.lock().pop();
            self.high_priority_sender_list.lock().pop();
            self.thread_list.lock().pop();
            self.map_id_index.lock().remove(&uuid);
        }
    }

    /// Scale to the given number of servers
    pub fn scale_to(&self, num_servers: u32, coordinator: Arc<Coordinator2>) {
        // remove terminated servers
        self.update_servers();

        // We need to activate servers
        if *self.no_active_servers.lock() < num_servers {
            // We activate existing servers
            while *self.no_active_servers.lock() < num_servers
                && *self.no_active_servers.lock() < self.server_id_list.lock().len() as u32
            {
                // Get the channel for the server activation and activate the server
                let _ = self.high_priority_sender_list.lock()
                    [*self.no_active_servers.lock() as usize]
                    .send(HighPriorityServerRequest::Activate);
                *self.no_active_servers.lock() += 1;
            }

            // We need to add more servers
            while *self.no_active_servers.lock() < num_servers {
                // Create channels for the new server
                let (low_priority_sender, low_priority_receiver) = unbounded();
                let (high_priority_sender, high_priority_receiver) = unbounded();

                // Create the server
                let mut server = Server2::new(
                    self.database.clone(),
                    coordinator.clone(),
                    self.reservation_timeout,
                    low_priority_receiver,
                    high_priority_receiver,
                    self.terminated_sender.clone(),
                    self.estimator_sender.clone(),
                );
                let server_id = server.id;

                // Start the server
                self.thread_list
                    .lock()
                    .push(thread::spawn(move || server.run()));

                // Add everything to the lists
                self.server_id_list.lock().push(server_id);
                self.low_priority_sender_list
                    .lock()
                    .push(low_priority_sender);
                self.high_priority_sender_list
                    .lock()
                    .push(high_priority_sender);
                self.map_id_index
                    .lock()
                    .insert(server_id, *self.no_active_servers.lock() as usize);
                *self.no_active_servers.lock() += 1;
            }
        }

        // We need to deactivate servers
        if *self.no_active_servers.lock() > num_servers {
            while *self.no_active_servers.lock() > num_servers {
                // Get the channel for the server deactivation and deactivate the server
                let _ = self.high_priority_sender_list.lock()
                    [(*self.no_active_servers.lock() - 1) as usize]
                    .send(HighPriorityServerRequest::Deactivate);

                *self.no_active_servers.lock() -= 1;
            }
        }
    }

    /// Get estimator ids and sender channels for servers that aren't completely
    /// terminated
    pub fn get_estimator(&self) -> (Vec<Uuid>, Vec<Sender<HighPriorityServerRequest>>) {
        self.update_servers();
        (
            self.server_id_list.lock().clone(),
            self.high_priority_sender_list.lock().clone(),
        )
    }

    /// Shut down all servers
    pub fn shutdown(&self) {
        for sender in self.high_priority_sender_list.lock().iter() {
            let _ = sender.send(HighPriorityServerRequest::Shutdown);
        }
        for thread in self.thread_list.lock().drain(..) {
            thread.join().unwrap();
        }
    }
}
