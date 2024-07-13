//! Implementation of the estimator
use std::sync::{mpsc, Arc};
use std::{collections::HashMap, time::Duration};

use crossbeam::channel::Receiver;
use parking_lot::Mutex;
use uuid::Uuid;

use super::coordinator::Coordinator;
use super::database::Database;
use super::serverrequest::HighPriorityServerRequest;

/// Estimator that estimates the number of tickets available overall
pub struct Estimator {
    coordinator: Arc<Mutex<Coordinator>>,
    database: Arc<Mutex<Database>>,
    roundtrip_secs: u32,
    tickets_in_server: HashMap<Uuid, u32>,
    receive_from_server: Receiver<u32>,
    estimator_shutdown: mpsc::Receiver<()>,
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
        estimator_shutdown: mpsc::Receiver<()>,
    ) -> Self {
        Self {
            coordinator,
            database,
            roundtrip_secs,
            tickets_in_server: HashMap::new(),
            receive_from_server,
            estimator_shutdown,
        }
    }

    pub fn run(&mut self) {
        let mut sum = 0;
        loop {
            let mut stop = false;
            let (servers, senders) = self.coordinator.lock().get_estimator();
            let tickets = self.database.lock().get_num_available();
            let time = (self.roundtrip_secs as f64) / (servers.len() as f64);
            let rounded_time = (time * 1000f64).floor() as u64;
            for (server, sender) in servers.iter().zip(senders.iter()) {
                if !self.tickets_in_server.contains_key(server) {
                    self.tickets_in_server.insert(*server, 0);
                }
                sum -= self.tickets_in_server[server];
                let aux = sender.send(HighPriorityServerRequest::Estimate {
                    tickets: sum + tickets,
                });
                match aux {
                    Ok(_) => {
                        *self.tickets_in_server.get_mut(server).unwrap() =
                            self.receive_from_server.recv().unwrap();
                    }
                    Err(_) => {
                        *self.tickets_in_server.get_mut(server).unwrap() = 0;
                    }
                }
                sum += self.tickets_in_server[server];
                if self
                    .estimator_shutdown
                    .recv_timeout(Duration::from_millis(rounded_time))
                    .is_ok()
                {
                    stop = true;
                    break;
                }
            }
            if stop {
                break;
            }
        }
    }
}