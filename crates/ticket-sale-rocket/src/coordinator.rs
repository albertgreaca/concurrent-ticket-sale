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
    server_id_list: Mutex<Vec<Uuid>>,
    server_sender_req_list: Mutex<Vec<Sender<Request>>>,
    server_sender_est_list: Mutex<Vec<Sender<u32>>>,
    server_sender_de_activation_list: Mutex<Vec<Sender<bool>>>,
    server_sender_shutdown_list: Mutex<Vec<Sender<bool>>>,
    server_status_request_list: Mutex<Vec<Sender<u32>>>,
    server_status_receiver_list: Mutex<Vec<Receiver<u32>>>,
    server_thread_list: Arc<Mutex<Vec<JoinHandle<()>>>>,
    server_map_index: Mutex<HashMap<Uuid, usize>>,
    pub no_active_servers: Mutex<u32>,
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
            server_id_list: Mutex::new(Vec::new()),
            server_sender_req_list: Mutex::new(Vec::new()),
            server_sender_est_list: Mutex::new(Vec::new()),
            server_sender_de_activation_list: Mutex::new(Vec::new()),
            server_sender_shutdown_list: Mutex::new(Vec::new()),
            server_status_request_list: Mutex::new(Vec::new()),
            server_status_receiver_list: Mutex::new(Vec::new()),
            server_thread_list: Arc::new(Mutex::new(Vec::new())),
            server_map_index: Mutex::new(HashMap::new()),
            no_active_servers: Mutex::new(0),
            estimator_sender,
        }
    }

    pub fn get_balancer_server_sender(&self, id: Uuid) -> Sender<Request> {
        if self.server_map_index.lock().contains_key(&id) {
            self.server_sender_req_list.lock()[self.server_map_index.lock()[&id]].clone()
        } else {
            // Should never happen
            self.server_sender_req_list.lock()[0].clone()
        }
    }

    pub fn get_status(&self, id: Uuid) -> u32 {
        if self.server_map_index.lock().contains_key(&id) {
            let _ =
                self.server_status_request_list.lock()[self.server_map_index.lock()[&id]].send(0);
        } else {
            // Should never happen
            let _ = self.server_status_request_list.lock()[0].send(0);
        }
        if self.server_map_index.lock().contains_key(&id) {
            return self.server_status_receiver_list.lock()[self.server_map_index.lock()[&id]]
                .recv()
                .unwrap();
        } else {
            // Should never happen
            return self.server_status_receiver_list.lock()[0].recv().unwrap();
        }
    }

    pub fn get_num_active_servers(&self) -> u32 {
        let id = self.no_active_servers.lock();
        *id
    }

    pub fn scale_to(&self, num_servers: u32) {
        let mut server_id_list_guard = self.server_id_list.lock();
        let mut no_active_servers_guard = self.no_active_servers.lock();
        if *no_active_servers_guard < num_servers {
            while *no_active_servers_guard < num_servers
                && *no_active_servers_guard < server_id_list_guard.len() as u32
            {
                let server_send_act = self.server_sender_de_activation_list.lock()
                    [*no_active_servers_guard as usize]
                    .clone();
                let _ = server_send_act.send(true);
                *no_active_servers_guard += 1;
            }

            while *no_active_servers_guard < num_servers {
                let (server_req_send, server_req_rec) = channel();
                let (server_est_send, server_est_rec) = channel();
                let (server_act_send, server_act_rec) = channel();
                let (server_shut_send, server_shut_rec) = channel();
                let (server_status_req_send, server_status_req_rec) = channel();
                let (server_status_send, server_status_rec) = channel();

                let mut server = Server::new(
                    self.database.clone(),
                    self.reservation_timeout,
                    server_req_rec,
                    server_est_rec,
                    server_act_rec,
                    server_shut_rec,
                    server_status_req_rec,
                    server_status_send,
                    self.estimator_sender.clone(),
                );
                let server_id = server.id;
                self.server_thread_list
                    .lock()
                    .push(thread::spawn(move || server.run()));
                server_id_list_guard.push(server_id);
                self.server_sender_est_list.lock().push(server_est_send);
                self.server_sender_req_list.lock().push(server_req_send);
                let _ = server_act_send.send(true);
                self.server_sender_de_activation_list
                    .lock()
                    .push(server_act_send);
                self.server_sender_shutdown_list
                    .lock()
                    .push(server_shut_send);
                self.server_status_request_list
                    .lock()
                    .push(server_status_req_send);
                self.server_status_receiver_list
                    .lock()
                    .push(server_status_rec);
                self.server_map_index
                    .lock()
                    .insert(server_id, *no_active_servers_guard as usize);
                *no_active_servers_guard += 1;
            }
        }

        if *no_active_servers_guard > num_servers {
            while *no_active_servers_guard > num_servers {
                let _ = self.server_sender_de_activation_list.lock()
                    [(*no_active_servers_guard - 1) as usize]
                    .send(false);
                *no_active_servers_guard -= 1;
            }
        }
    }

    pub fn get_active_servers(&self) -> Vec<Uuid> {
        let n = *self.no_active_servers.lock() as usize;
        self.server_id_list.lock()[0..n].to_vec()
    }

    pub fn get_estimator_senders(&self) -> Vec<Sender<u32>> {
        let mut senders = Vec::new();
        for (i, sender) in self.server_sender_est_list.lock().iter().enumerate() {
            if self.get_status(self.server_id_list.lock()[i]) != 2 {
                senders.push((*sender).clone());
            }
        }
        senders
    }

    pub fn get_estimator_servers(&self) -> Vec<Uuid> {
        let mut aux = self.server_id_list.lock().clone();
        aux.retain(|x| self.get_status(*x) != 2);
        aux
    }

    pub fn get_random_server(&self) -> Uuid {
        let mut rng = rand::thread_rng();
        self.server_id_list.lock()[rng.gen_range(0..*self.no_active_servers.lock()) as usize]
    }

    pub fn shutdown(&self) {
        let senders = self.server_sender_shutdown_list.lock().clone();
        for sender in senders {
            let _ = sender.send(true);
        }
        let mut threads = self.server_thread_list.lock();
        for thread in threads.drain(..) {
            thread.join().unwrap();
        }
    }
}
