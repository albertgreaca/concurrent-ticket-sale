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
/// Coordinator orchestrating all the components of the system
pub struct Coordinator {
    /// The reservation timeout
    reservation_timeout: u32,

    /// Reference to the [`Database`]
    ///
    /// To be handed over to new servers.
    database: Arc<Mutex<Database>>,
    server_id_list: Vec<Uuid>,
    server_sender_req_list: Vec<Sender<Request>>,
    server_sender_est_list: Vec<Sender<u32>>,
    server_sender_de_activation_list: Vec<Sender<bool>>,
    server_sender_shutdown_list: Vec<Sender<bool>>,
    server_status_receiver_list: Vec<Receiver<u32>>,
    server_thread_list: Vec<JoinHandle<()>>,
    server_map_index: HashMap<Uuid, usize>,
    server_map_status: HashMap<Uuid, u32>,
    pub no_active_servers: u32,
    estimator_sender: Sender<u32>,
}

impl Coordinator {
    /// Create the [`Coordinator`]
    pub fn new(
        reservation_timeout: u32,
        database: Arc<Mutex<Database>>,
        estimator_sender: Sender<u32>,
    ) -> Self {
        Self {
            reservation_timeout,
            database,
            server_id_list: Vec::new(),
            server_sender_req_list: Vec::new(),
            server_sender_est_list: Vec::new(),
            server_sender_de_activation_list: Vec::new(),
            server_sender_shutdown_list: Vec::new(),
            server_status_receiver_list: Vec::new(),
            server_thread_list: Vec::new(),
            server_map_index: HashMap::new(),
            server_map_status: HashMap::new(),
            no_active_servers: 0,
            estimator_sender,
        }
    }

    /// Get the channel for sending user requests to the server with the given id
    pub fn get_balancer_server_sender(&self, id: Uuid) -> Sender<Request> {
        if self.server_map_index.contains_key(&id) {
            self.server_sender_req_list[self.server_map_index[&id]].clone()
        } else {
            // Should never happen
            self.server_sender_req_list[0].clone()
        }
    }

    /// Get the status of the server with the given id
    pub fn get_status(&mut self, id: Uuid) -> u32 {
        while let Ok(value) =
            self.server_status_receiver_list[self.server_map_index[&id]].try_recv()
        {
            if let Some(value2) = self.server_map_status.get_mut(&id) {
                *value2 = value;
            }
        }
        self.server_map_status[&id]
    }

    /// Get the number of servers that are non-terminating
    pub fn get_num_active_servers(&self) -> u32 {
        self.no_active_servers
    }

    /// Scale to the given number of servers
    pub fn scale_to(&mut self, num_servers: u32) {
        // We need to activate servers
        if self.no_active_servers < num_servers {
            // We activate existing servers
            while self.no_active_servers < num_servers
                && self.no_active_servers < self.server_id_list.len() as u32
            {
                // Get the channel for the server activation and activate the server
                let _ = self.server_sender_de_activation_list[self.no_active_servers as usize]
                    .send(true);
                self.server_thread_list[self.no_active_servers as usize]
                    .thread()
                    .unpark();
                self.no_active_servers += 1;
            }

            // We need to add more servers
            while self.no_active_servers < num_servers {
                // Create channels for the new server
                let (server_req_send, server_req_rec) = channel();
                let (server_est_send, server_est_rec) = channel();
                let (server_act_send, server_act_rec) = channel();
                let (server_shut_send, server_shut_rec) = channel();
                let (server_status_send, server_status_rec) = channel();

                // Create the server
                let mut server = Server::new(
                    self.database.clone(),
                    self.reservation_timeout,
                    server_req_rec,
                    server_est_rec,
                    server_act_rec,
                    server_shut_rec,
                    server_status_send,
                    self.estimator_sender.clone(),
                );
                let server_id = server.id;

                // Start the server
                self.server_thread_list
                    .push(thread::spawn(move || server.run()));

                // Add everything to the lists
                self.server_id_list.push(server_id);
                self.server_sender_req_list.push(server_req_send);
                self.server_sender_est_list.push(server_est_send);
                self.server_sender_de_activation_list.push(server_act_send);
                self.server_sender_shutdown_list.push(server_shut_send);
                self.server_status_receiver_list.push(server_status_rec);
                self.server_map_index
                    .insert(server_id, self.no_active_servers as usize);
                self.server_map_status.insert(server_id, 0);
                self.no_active_servers += 1;
            }
        }

        // We need to deactivate servers
        if self.no_active_servers > num_servers {
            while self.no_active_servers > num_servers {
                // Get the channel for the server deactivation and deactivate the server
                let _ = self.server_sender_de_activation_list
                    [(self.no_active_servers - 1) as usize]
                    .send(false);

                self.no_active_servers -= 1;
            }
        }
    }

    /// Get ids corresponding to non-terminating servers
    pub fn get_active_servers(&self) -> Vec<Uuid> {
        let n = self.no_active_servers as usize;
        self.server_id_list[0..n].to_vec()
    }

    /// Get estimator ids and sender channels for servers that aren't completely
    /// terminated
    pub fn get_estimator(&mut self) -> Vec<(Uuid, Sender<u32>)> {
        let mut server_sender = Vec::new();
        for i in 0..self.server_id_list.len() {
            if self.get_status(self.server_id_list[i]) != 2 {
                server_sender.push((
                    self.server_id_list[i],
                    self.server_sender_est_list[i].clone(),
                ));
            }
        }
        server_sender
    }

    /// Get the id of a random non-terminating server
    pub fn get_random_server(&self) -> Uuid {
        let mut rng = rand::thread_rng();
        self.server_id_list[rng.gen_range(0..self.no_active_servers) as usize]
    }

    /// Shut down all servers
    pub fn shutdown(&mut self) {
        for sender in self.server_sender_shutdown_list.iter() {
            let _ = sender.send(true);
        }
        for thread in self.server_thread_list.iter() {
            thread.thread().unpark();
        }
        for thread in self.server_thread_list.drain(..) {
            thread.join().unwrap();
        }
    }
}
