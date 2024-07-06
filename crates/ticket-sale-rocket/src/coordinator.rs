//! Implementation of the coordinator
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use rand::Rng;
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
    server_list: Mutex<Vec<Arc<Mutex<Server>>>>,
    server_id_list: Mutex<Vec<Uuid>>,
    server_map_index: Mutex<HashMap<Uuid, usize>>,
    pub no_active_servers: Mutex<u32>,
}

impl Coordinator {
    /// Create the [`Coordinator`]
    pub fn new(reservation_timeout: u32, database: Arc<Mutex<Database>>) -> Self {
        Self {
            reservation_timeout,
            database,
            server_list: Mutex::new(Vec::new()),
            server_id_list: Mutex::new(Vec::new()),
            server_map_index: Mutex::new(HashMap::new()),
            no_active_servers: Mutex::new(0),
        }
    }

    pub fn get_num_active_servers(&self) -> u32 {
        let id = self.no_active_servers.lock();
        *id
    }

    pub fn scale_to(&self, num_servers: u32) {
        let mut server_list_guard = self.server_list.lock();
        let mut no_active_servers_guard = self.no_active_servers.lock();
        if *no_active_servers_guard < num_servers {
            while *no_active_servers_guard < num_servers
                && *no_active_servers_guard < server_list_guard.len() as u32
            {
                let server = &server_list_guard[*no_active_servers_guard as usize];
                server.lock().activate();
                *no_active_servers_guard += 1;
            }

            while *no_active_servers_guard < num_servers {
                let server = Server::new(self.database.clone(), self.reservation_timeout);
                let server_id = server.get_id();
                server_list_guard.push(Arc::new(Mutex::new(server)));
                self.server_id_list.lock().push(server_id);
                server_list_guard[*no_active_servers_guard as usize]
                    .lock()
                    .activate();
                self.server_map_index
                    .lock()
                    .insert(server_id, *no_active_servers_guard as usize);
                *no_active_servers_guard += 1;
            }
        }

        if *no_active_servers_guard > num_servers {
            while *no_active_servers_guard > num_servers {
                server_list_guard[(*no_active_servers_guard - 1) as usize]
                    .lock()
                    .deactivate();
                *no_active_servers_guard -= 1;
            }
        }
    }

    pub fn get_active_servers(&self) -> Vec<Uuid> {
        let n = *self.no_active_servers.lock() as usize;
        self.server_id_list.lock()[0..n].to_vec()
    }

    pub fn get_estimator_servers(&self) -> Vec<Uuid> {
        let mut aux = self.server_id_list.lock().clone();
        aux.retain(|x| self.get_server(*x).lock().get_status() != 2);
        aux
    }

    pub fn get_random_server(&self) -> Uuid {
        let mut rng = rand::thread_rng();
        self.server_id_list.lock()[rng.gen_range(0..*self.no_active_servers.lock()) as usize]
    }

    pub fn get_server(&self, id: Uuid) -> Arc<Mutex<Server>> {
        if self.server_map_index.lock().contains_key(&id) {
            self.server_list.lock()[self.server_map_index.lock()[&id]].clone()
        } else {
            self.server_list.lock()[0].clone()
        }
    }
}
