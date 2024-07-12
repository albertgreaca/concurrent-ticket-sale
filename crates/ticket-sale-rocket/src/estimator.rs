//! Implementation of the estimator
use std::sync::Arc;
use std::{collections::HashMap, thread::sleep, time::Duration};

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

    pub fn run(&mut self, mut sum: u32) -> u32 {
        let mut guard2 = self.coordinator.lock();
        let (servers, senders) = guard2.get_estimator();
        drop(guard2);
        let guard = self.database.lock();
        let tickets = guard.get_num_available();
        drop(guard);
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
            sleep(Duration::from_secs(
                (self.roundtrip_secs / servers.len() as u32) as u64,
            ));
        }
        sum
    }
}
