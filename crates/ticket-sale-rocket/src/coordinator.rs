//! Implementation of the coordinator
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use parking_lot::Mutex;
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

    pub no_active_servers: Arc<Mutex<u32>>,
    map_id_index: HashMap<Uuid, usize>,
    server_id_list: Arc<Mutex<Vec<Uuid>>>,
    low_priority_sender_list: Vec<Sender<Request>>,
    high_priority_sender_list: Vec<Sender<HighPriorityServerRequest>>,
    thread_list: Vec<JoinHandle<()>>,

    status_sender: Sender<(Uuid, u32)>,
    status_receiver: Receiver<(Uuid, u32)>,

    estimator_sender: Sender<u32>,
}

impl Coordinator {
    /// Create the [`Coordinator`]
    pub fn new(
        reservation_timeout: u32,
        database: Arc<Mutex<Database>>,
        estimator_sender: Sender<u32>,
    ) -> Self {
        let (status_sender, status_receiver) = channel();
        Self {
            reservation_timeout,
            database,
            no_active_servers: Arc::new(Mutex::new(0)),
            map_id_index: HashMap::new(),
            server_id_list: Arc::new(Mutex::new(Vec::new())),
            low_priority_sender_list: Vec::new(),
            high_priority_sender_list: Vec::new(),
            thread_list: Vec::new(),
            status_sender,
            status_receiver,
            estimator_sender,
        }
    }

    /// Get the number of servers that are non-terminating
    pub fn get_num_active_servers(&self) -> u32 {
        *self.no_active_servers.lock()
    }

    /// Get ids corresponding to non-terminating servers
    pub fn get_active_servers(&self) -> Vec<Uuid> {
        let n = *self.no_active_servers.lock() as usize;
        self.server_id_list.lock()[0..n].to_vec()
    }

    /// Get the id of a random non-terminating server
    pub fn get_random_server(&self) -> Uuid {
        let mut rng = rand::thread_rng();
        self.server_id_list.lock()[rng.gen_range(0..*self.no_active_servers.lock()) as usize]
    }

    /// Get the channel for sending user requests to the server with the given id
    pub fn get_low_priority_sender(&self, id: Uuid) -> Sender<Request> {
        if self.map_id_index.contains_key(&id) {
            self.low_priority_sender_list[self.map_id_index[&id]].clone()
        } else {
            panic!("low priority sender not found");
        }
    }

    pub fn update_servers(&mut self) {
        loop {
            match self.status_receiver.try_recv() {
                Ok((uuid, status)) => {
                    if status == 2 {
                        let index = self.map_id_index[&uuid];

                        let mut server_id_list_guard = self.server_id_list.lock();
                        let n = server_id_list_guard.len();
                        server_id_list_guard.swap(index, n - 1);
                        server_id_list_guard.pop();

                        *self
                            .map_id_index
                            .get_mut(&server_id_list_guard[index])
                            .unwrap() = index;
                        self.map_id_index.remove(&uuid);

                        let n = self.low_priority_sender_list.len();
                        self.low_priority_sender_list.swap(index, n - 1);
                        self.low_priority_sender_list.pop();

                        let n = self.high_priority_sender_list.len();
                        self.high_priority_sender_list.swap(index, n - 1);
                        self.high_priority_sender_list.pop();

                        let n = self.thread_list.len();
                        self.thread_list.swap(index, n - 1);
                        self.thread_list.pop();
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
    }

    /// Scale to the given number of servers
    pub fn scale_to(&mut self, num_servers: u32) {
        self.update_servers();
        let mut n_guard = self.no_active_servers.lock();

        // We need to activate servers
        if *n_guard < num_servers {
            // We activate existing servers
            while *n_guard < num_servers && *n_guard < self.server_id_list.lock().len() as u32 {
                // Get the channel for the server activation and activate the server
                let _ = self.high_priority_sender_list[*n_guard as usize]
                    .send(HighPriorityServerRequest::DeActivate { activate: true });
                *n_guard += 1;
            }

            // We need to add more servers
            while *n_guard < num_servers {
                // Create channels for the new server
                let (low_priority_sender, low_priority_receiver) = channel();
                let (high_priority_sender, high_priority_receiver) = channel();

                // Create the server
                let mut server = Server::new(
                    self.database.clone(),
                    self.reservation_timeout,
                    low_priority_receiver,
                    high_priority_receiver,
                    self.status_sender.clone(),
                    self.estimator_sender.clone(),
                    self.no_active_servers.clone(),
                    self.server_id_list.clone(),
                );
                let server_id = server.id;

                // Start the server
                self.thread_list.push(thread::spawn(move || server.run()));

                // Add everything to the lists
                self.server_id_list.lock().push(server_id);
                self.low_priority_sender_list.push(low_priority_sender);
                self.high_priority_sender_list.push(high_priority_sender);
                self.map_id_index.insert(server_id, *n_guard as usize);
                *n_guard += 1;
            }
        }

        // We need to deactivate servers
        if *n_guard > num_servers {
            while *n_guard > num_servers {
                // Get the channel for the server deactivation and deactivate the server
                let _ = self.high_priority_sender_list[(*n_guard - 1) as usize]
                    .send(HighPriorityServerRequest::DeActivate { activate: false });

                *n_guard -= 1;
            }
        }
    }

    /// Get estimator ids and sender channels for servers that aren't completely
    /// terminated
    pub fn get_estimator(&mut self) -> (Vec<Uuid>, Vec<Sender<HighPriorityServerRequest>>) {
        self.update_servers();
        return (
            self.server_id_list.lock().clone(),
            self.high_priority_sender_list.clone(),
        );
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
