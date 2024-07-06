//! Implementation of the estimator

use std::sync::Arc;
use std::{collections::HashMap, thread::sleep, time::Duration};

use parking_lot::Mutex;
use uuid::Uuid;

use super::coordinator::Coordinator;
use super::database::Database;

/// Estimator that estimates the number of tickets available overall
pub struct Estimator {
    coordinator: Arc<Coordinator>,
    database: Arc<Mutex<Database>>,
    roundtrip_secs: u32,
    tickets_in_server: Mutex<HashMap<Uuid, u32>>,
}

impl Estimator {
    /// The estimator's main routine.
    ///
    /// `roundtrip_secs` is the time in seconds the estimator needs to contact all
    /// servers. If there are `N` servers, then the estimator should wait
    /// `roundtrip_secs / N` between each server when collecting statistics.

    pub fn new(
        database: Arc<Mutex<Database>>,
        coordinator: Arc<Coordinator>,
        roundtrip_secs: u32,
    ) -> Self {
        Self {
            coordinator,
            database,
            roundtrip_secs,
            tickets_in_server: Mutex::new(HashMap::new()),
        }
    }

    pub fn run(&self) {
        let servers = {
            let guard = self.coordinator.clone();
            let aux = guard.get_estimator_servers().clone();
            drop(guard);
            aux
        };
        let guard = self.database.lock();
        let tickets = guard.get_num_available();
        drop(guard);
        let mut sum = 0;
        let mut tickets_in_server_guard = self.tickets_in_server.lock();
        for server in &servers {
            if tickets_in_server_guard.contains_key(server) {
                sum += tickets_in_server_guard[server];
            } else {
                tickets_in_server_guard.insert(*server, 0);
            }
        }
        for server in &servers {
            sum -= tickets_in_server_guard[server];
            *tickets_in_server_guard.get_mut(server).unwrap() = self
                .coordinator
                .get_server(*server)
                .lock()
                .send_tickets(sum + tickets);
            sum += tickets_in_server_guard[server];
            sleep(Duration::from_secs(
                (self.roundtrip_secs / servers.len() as u32) as u64,
            ));
        }
    }
}
