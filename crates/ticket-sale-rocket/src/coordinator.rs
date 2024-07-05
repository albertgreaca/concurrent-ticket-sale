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
    server_list: Vec<Server>,
    server_id_list: Vec<Uuid>,
    server_map_index: HashMap<Uuid, usize>,
    pub no_active_servers: u32,
}

impl Coordinator {
    /// Create the [`Coordinator`]
    pub fn new(reservation_timeout: u32, database: Arc<Mutex<Database>>) -> Self {
        Self {
            reservation_timeout,
            database,
            server_list: Vec::new(),
            server_id_list: Vec::new(),
            server_map_index: HashMap::new(),
            no_active_servers: 0,
        }
    }

    pub fn get_num_active_servers(&self) -> u32 {
        self.no_active_servers
    }

    pub fn scale_to(&mut self, num_servers: u32) {
        if self.no_active_servers < num_servers {
            while self.no_active_servers < num_servers
                && self.no_active_servers < self.server_list.len() as u32
            {
                let server = &mut self
                    .server_list
                    .get_mut(self.no_active_servers as usize)
                    .unwrap();
                server.activate();
                self.no_active_servers += 1;
            }

            while self.no_active_servers < num_servers {
                let server = Server::new(self.database.clone(), self.reservation_timeout);
                let server_id = server.get_id();
                self.server_list.push(server);
                self.server_id_list.push(server_id);
                self.server_list[self.no_active_servers as usize].activate();
                self.server_map_index
                    .insert(server_id, self.no_active_servers as usize);
                self.no_active_servers += 1;
            }
        }

        if self.no_active_servers > num_servers {
            while self.no_active_servers > num_servers {
                self.server_list[(self.no_active_servers - 1) as usize].deactivate();
                self.no_active_servers -= 1;
            }
        }
    }

    pub fn get_active_servers(&self) -> &[Uuid] {
        let n = self.no_active_servers as usize;
        &(self.server_id_list)[0..n]
    }

    pub fn get_estimator_servers(&self) -> Vec<Uuid> {
        let mut aux = self.server_id_list.clone();
        aux.retain(|x| self.get_server(*x).get_status() != 2);
        aux
    }

    pub fn get_random_server(&self) -> Uuid {
        let mut rng = rand::thread_rng();
        self.server_id_list[rng.gen_range(0..self.no_active_servers) as usize]
    }

    pub fn get_server(&self, id: Uuid) -> &Server {
        if self.server_map_index.contains_key(&id) {
            &self.server_list[self.server_map_index[&id]]
        } else {
            &self.server_list[0]
        }
    }

    pub fn get_server_mut(&mut self, id: Uuid) -> &mut Server {
        if self.server_map_index.contains_key(&id) {
            &mut self.server_list[self.server_map_index[&id]]
        } else {
            &mut self.server_list[0]
        }
    }
}
