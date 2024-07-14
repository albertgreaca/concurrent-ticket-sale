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

    /// number of tickets known to be in each server
    tickets_in_server: HashMap<Uuid, u32>,

    /// channel through which the estimator receives the number of tickets from each
    /// server
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
        loop {
            let mut stop = false; // becomes true when the estimator needs to shut down

            // get non-terminated servers and the senders for high priority requests
            let (servers, senders) = self.coordinator.lock().get_estimator();

            // get number of tickets in the database
            let tickets = self.database.lock().get_num_available();

            // calculate the sleep time between servers
            let time_seconds = (self.roundtrip_secs as f64) / (servers.len() as f64);
            let time_miliseconds = (time_seconds * 1000f64).floor() as u64;

            // calculate the total number of tickets known to be in the servers from previous
            // iterations
            let mut sum = 0;
            for server in servers.clone() {
                self.tickets_in_server.entry(server).or_insert(0);
                sum += self.tickets_in_server[&server];
            }

            // main estimator loop
            for (server, sender) in servers.iter().zip(senders.iter()) {
                // make sum the number of tickets known to be in the other servers
                sum -= self.tickets_in_server[server];

                // send the number of tickets in the other servers + the database
                let aux = sender.send(HighPriorityServerRequest::Estimate {
                    tickets: sum + tickets,
                });
                match aux {
                    Ok(_) => {
                        // message was sent => server not terminated => wait for response
                        *self.tickets_in_server.get_mut(server).unwrap() =
                            self.receive_from_server.recv().unwrap();
                    }
                    Err(_) => {
                        // message not sent => server terminated mid loop =>
                        // it should've cleared all tickets so it has 0 left
                        *self.tickets_in_server.get_mut(server).unwrap() = 0;
                    }
                }

                // make sum the number of tickets known to be in all servers again
                sum += self.tickets_in_server[server];

                // wait for time_miliseconds miliseconds, but break the for loop if shutdown signal
                // is received
                if self
                    .estimator_shutdown
                    .recv_timeout(Duration::from_millis(time_miliseconds))
                    .is_ok()
                {
                    stop = true;
                    break;
                }
            }
            // if shutdown signal was received, break the main loop
            if stop {
                break;
            }
        }
    }
}
