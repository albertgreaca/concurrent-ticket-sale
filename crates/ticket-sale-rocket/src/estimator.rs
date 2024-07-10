//! Implementation of the estimator

use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::{collections::HashMap, thread::sleep, time::Duration};

use parking_lot::Mutex;
use uuid::Uuid;

use super::coordinator::Coordinator;
use super::database::Database;

/// Estimator that estimates the number of tickets available overall
pub struct Estimator {
    coordinator: Arc<Mutex<Coordinator>>,
    database: Arc<Mutex<Database>>,
    roundtrip_secs: u32,
    tickets_in_server: HashMap<Uuid, u32>,
    receive_from_server: Receiver<u32>,
}

impl Estimator {
    /// The estimator's main routine.
    ///
    /// `roundtrip_secs` is the time in seconds the estimator needs to contact all
    /// servers. If there are `N` servers, then the estimator should wait
    /// `roundtrip_secs / N` between each server when collecting statistics.

    pub fn new(
        database: Arc<Mutex<Database>>,
        coordinator: Arc<Mutex<Coordinator>>,
        roundtrip_secs: u32,
        receive_from_server: Receiver<u32>,
    ) -> Self {
        Self {
            coordinator,
            database,
            roundtrip_secs,
            tickets_in_server: HashMap::new(),
            receive_from_server,
        }
    }

    pub fn run(&mut self) {
        let guard2 = self.coordinator.lock();
        let servers = guard2.get_estimator_servers().clone();
        let senders = guard2.get_estimator_senders().clone();
        drop(guard2);
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
        for (server, sender) in servers.iter().zip(senders.iter()) {
            sum -= self.tickets_in_server[server];
            let _ = sender.send(sum + tickets);
            *self.tickets_in_server.get_mut(server).unwrap() =
                self.receive_from_server.recv().unwrap();
            sum += self.tickets_in_server[server];
            sleep(Duration::from_secs(
                (self.roundtrip_secs / servers.len() as u32) as u64,
            ));
        }
    }
}
