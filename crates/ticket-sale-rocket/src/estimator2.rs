//! Implementation of the estimator

use std::sync::Arc;
use std::{collections::HashMap, thread::sleep, time::Duration};

use parking_lot::Mutex;
use uuid::Uuid;

use super::coordinator2::Coordinator2;
use super::database::Database;

/// Estimator that estimates the number of tickets available overall
pub struct Estimator2 {
    coordinator: Arc<Coordinator2>,
    database: Arc<Mutex<Database>>,
    roundtrip_secs: u32,
    tickets_in_server: HashMap<Uuid, u32>,
}

impl Estimator2 {
    /// The estimator's main routine.
    ///
    /// `roundtrip_secs` is the time in seconds the estimator needs to contact all
    /// servers. If there are `N` servers, then the estimator should wait
    /// `roundtrip_secs / N` between each server when collecting statistics.

    pub fn new(
        database: Arc<Mutex<Database>>,
        coordinator: Arc<Coordinator2>,
        roundtrip_secs: u32,
    ) -> Self {
        Self {
            coordinator,
            database,
            roundtrip_secs,
            tickets_in_server: HashMap::new(),
        }
    }

    pub fn run(&mut self) {
        let servers = self.coordinator.get_estimator_servers();
        let guard = self.database.lock();
        let tickets = guard.get_num_available();
        drop(guard);
        let mut sum = 0;
        for server in &servers {
            if self.tickets_in_server.contains_key(server) {
                sum += self.tickets_in_server[server];
            } else {
                self.tickets_in_server.insert(*server, 0);
            }
        }
        for server in &servers {
            sum -= self.tickets_in_server[server];
            *self.tickets_in_server.get_mut(server).unwrap() = self
                .coordinator
                .get_server(*server)
                .lock()
                .send_tickets(sum + tickets);
            sum += self.tickets_in_server[server];
            sleep(Duration::from_secs(
                (self.roundtrip_secs / servers.len() as u32) as u64,
            ));
        }
    }
}
