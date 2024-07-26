//! Implementation of the standard estimator

use std::sync::{mpsc, Arc};
use std::{collections::HashMap, time::Duration};

use crossbeam::channel::{Receiver, Sender};
use parking_lot::Mutex;
use uuid::Uuid;

use super::database::Database;
use super::serverrequest::HighPriorityServerRequest;
use crate::serverstatus::EstimatorServerStatus;

/// Estimator that estimates the number of tickets available overall
pub struct EstimatorStandard {
    database: Arc<Mutex<Database>>,

    /// Number of seconds each loop should take
    roundtrip_secs: u32,

    /// Number of tickets known to be in each server
    server_tickets: HashMap<Uuid, u32>,

    /// High priority senders for each server
    server_senders: HashMap<Uuid, Sender<HighPriorityServerRequest>>,

    /// Receiver for receiving the number of tickets from each server
    estimator_tickets_receiver: Receiver<u32>,

    /// Receiver for being notified of each server's activation/termination
    estimator_scaling_receiver: Receiver<EstimatorServerStatus>,

    /// Receiver for being told to shut down
    estimator_shutdown_receiver: mpsc::Receiver<()>,
}

impl EstimatorStandard {
    /// Create a new [`EstimatorStandard`]
    pub fn new(
        database: Arc<Mutex<Database>>,
        roundtrip_secs: u32,
        estimator_tickets_receiver: Receiver<u32>,
        estimator_scaling_receiver: Receiver<EstimatorServerStatus>,
        estimator_shutdown_receiver: mpsc::Receiver<()>,
    ) -> Self {
        Self {
            database,
            roundtrip_secs,
            server_tickets: HashMap::new(),
            server_senders: HashMap::new(),
            estimator_tickets_receiver,
            estimator_scaling_receiver,
            estimator_shutdown_receiver,
        }
    }

    /// Main estimator loop
    pub fn run(&mut self) {
        loop {
            let mut stop = false; // Becomes true when the estimator needs to shut down

            // While there are servers that activated/terminated
            while let Ok(msg) = self.estimator_scaling_receiver.try_recv() {
                match msg {
                    EstimatorServerStatus::Activated { server, sender } => {
                        // Add the newly activated server
                        self.server_senders.insert(server, sender);
                        self.server_tickets.insert(server, 0);
                    }
                    EstimatorServerStatus::Deactivated { server } => {
                        // Remove the newly terminated server
                        self.server_senders.remove(&server);
                        self.server_tickets.remove(&server);
                    }
                }
            }

            // Get the number of tickets in the database
            let tickets = self.database.lock().get_num_available();

            // Calculate the sleep time between servers
            let time_seconds = (self.roundtrip_secs as f64) / (self.server_senders.len() as f64);
            let time_miliseconds = (time_seconds * 1000f64).floor() as u64;

            // Calculate the total number of tickets known to be in the servers from previous
            // iterations
            let mut sum = 0;
            for (_, tickets) in &self.server_tickets {
                sum += tickets;
            }

            // Current iteration loop
            for (server, sender) in &self.server_senders {
                // Make sum the number of tickets known to be in the other servers
                sum -= self.server_tickets[server];

                // Send the number of tickets in the other servers + the database
                let aux = sender.send(HighPriorityServerRequest::Estimate {
                    tickets: sum + tickets,
                });
                match aux {
                    Ok(_) => {
                        // Message was sent => server not terminated => wait for response
                        *self.server_tickets.get_mut(server).unwrap() =
                            self.estimator_tickets_receiver.recv().unwrap();
                    }
                    Err(_) => {
                        // Message not sent => server terminated mid loop =>
                        // it should've cleared all tickets so it has 0 left
                        *self.server_tickets.get_mut(server).unwrap() = 0;
                    }
                }

                // Make sum the number of tickets known to be in all servers again
                sum += self.server_tickets[server];

                // Wait for time_miliseconds miliseconds, but break the for loop if shutdown signal
                // is received
                if self
                    .estimator_shutdown_receiver
                    .recv_timeout(Duration::from_millis(time_miliseconds))
                    .is_ok()
                {
                    stop = true;
                    break;
                }
            }
            // If shutdown signal was received, break the main loop
            if stop {
                break;
            }
        }
    }
}
