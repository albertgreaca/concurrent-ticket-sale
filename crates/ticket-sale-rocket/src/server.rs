//! Implementation of the server
#![allow(clippy::too_many_arguments)]
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, sync::mpsc::Receiver};

use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;

/// A server in the ticket sales system
pub struct Server {
    /// The server's ID
    pub id: Uuid,
    estimate: u32,
    /// The database
    database: Arc<Mutex<Database>>,
    pub status: u32,
    tickets: Vec<u32>,
    reserved: HashMap<Uuid, (u32, Instant)>,
    timeout: u32,
    balancer_receiver: Receiver<Request>,
    estimator_receiver: Receiver<u32>,
    de_activate_receiver: Receiver<bool>,
    shutdown_receiver: Receiver<bool>,
    status_req_receiver: Receiver<u32>,
    status_sender: Sender<u32>,
    estimator_sender: Sender<u32>,
}

impl Server {
    /// Create a new [`Server`]
    pub fn new(
        database: Arc<Mutex<Database>>,
        timeout: u32,
        balancer_receiver: Receiver<Request>,
        estimator_receiver: Receiver<u32>,
        de_activate_receiver: Receiver<bool>,
        shutdown_receiver: Receiver<bool>,
        status_req_receiver: Receiver<u32>,
        status_sender: Sender<u32>,
        estimator_sender: Sender<u32>,
    ) -> Server {
        let id = Uuid::new_v4();
        let num_tickets = (database.lock().get_num_available() as f64).sqrt() as u32;
        let tickets = database.lock().allocate(num_tickets);
        Self {
            id,
            estimate: 0,
            database,
            status: 0,
            tickets,
            reserved: HashMap::new(),
            timeout,
            balancer_receiver,
            estimator_receiver,
            de_activate_receiver,
            shutdown_receiver,
            status_req_receiver,
            status_sender,
            estimator_sender,
        }
    }

    /// Handle a [`Request`]
    pub fn cycle(&mut self) {
        if self.status_req_receiver.try_recv().is_ok() {
            let _ = self.status_sender.send(self.status);
            return;
        }
        match self.estimator_receiver.try_recv() {
            Ok(value) => {
                self.send_tickets(value);
            }
            Err(_) => {
                if let Ok(rq) = self.balancer_receiver.try_recv() {
                    self.handle_request(rq);
                }
            }
        }
    }

    pub fn handle_request(&mut self, mut rq: Request) {
        self.reserved.retain(|_, (ticket, time)| {
            if time.elapsed().as_secs() > self.timeout as u64 {
                if self.status == 0 {
                    self.tickets.push(*ticket);
                } else {
                    let x = *ticket;
                    self.database.lock().deallocate(&[x]);
                }
                false
            } else {
                true
            }
        });
        if self.reserved.is_empty() && self.status == 1 {
            self.database.lock().deallocate(self.tickets.as_slice());
            self.tickets.clear();
            self.status = 2;
        }
        match rq.kind() {
            RequestKind::NumAvailableTickets => {
                rq.respond_with_int(self.get_available_tickets());
            }
            RequestKind::ReserveTicket => {
                rq.set_server_id(self.id);
                let bloke = rq.customer_id();
                if self.reserved.contains_key(&bloke) {
                    rq.respond_with_err("one reservation already present");
                    return;
                }
                /*if self.get_available_tickets() == 0 {
                    rq.respond_with_sold_out();
                    return;
                }*/
                if self.tickets.is_empty() {
                    if self.database.lock().get_num_available() == 0 {
                        rq.respond_with_sold_out();
                        return;
                    }
                    let num_tickets =
                        (self.database.lock().get_num_available() as f64).sqrt() as u32;
                    self.tickets
                        .extend(self.database.lock().allocate(num_tickets));
                }
                let ticket = self.tickets.pop().unwrap();
                self.reserved.insert(bloke, (ticket, Instant::now()));
                rq.respond_with_int(ticket);
            }
            RequestKind::BuyTicket => {
                rq.set_server_id(self.id);
                let ticket_option = rq.read_u32();
                if ticket_option.is_none() {
                    rq.respond_with_err("no ticket");
                } else {
                    let ticket = ticket_option.unwrap();
                    let bloke = rq.customer_id();
                    if self.reserved.contains_key(&bloke) {
                        if self.reserved[&bloke].0 == ticket {
                            self.reserved.remove(&bloke);
                            if self.reserved.is_empty() && self.status == 1 {
                                self.database.lock().deallocate(self.tickets.as_slice());
                                self.tickets.clear();
                                self.status = 2;
                            }
                            rq.respond_with_int(ticket);
                        } else {
                            rq.respond_with_err("you ain't got that shit")
                        }
                    } else {
                        rq.respond_with_err("you ain't got that shit")
                    }
                }
            }
            RequestKind::AbortPurchase => {
                rq.set_server_id(self.id);
                let ticket_option = rq.read_u32();
                if ticket_option.is_none() {
                    rq.respond_with_err("no ticket");
                } else {
                    let ticket = ticket_option.unwrap();
                    let bloke = rq.customer_id();
                    if self.reserved.contains_key(&bloke) {
                        if self.reserved[&bloke].0 == ticket {
                            self.reserved.remove(&bloke);
                            if self.status == 0 {
                                self.tickets.push(ticket);
                            } else {
                                self.database.lock().deallocate(&[ticket]);
                            }
                            if self.reserved.is_empty() && self.status == 1 {
                                self.database.lock().deallocate(self.tickets.as_slice());
                                self.tickets.clear();
                                self.status = 2;
                            }
                            rq.respond_with_int(ticket);
                        } else {
                            rq.respond_with_err("you ain't got that shit")
                        }
                    } else {
                        rq.respond_with_err("you ain't got that shit")
                    }
                }
            }
            _ => {
                rq.respond_with_err("fucking hell");
            }
        }
    }

    pub fn send_tickets(&mut self, tickets: u32) {
        self.reserved.retain(|_, (ticket, time)| {
            if time.elapsed().as_secs() > self.timeout as u64 {
                if self.status == 0 {
                    self.tickets.push(*ticket);
                } else {
                    let x = *ticket;
                    self.database.lock().deallocate(&[x]);
                }
                false
            } else {
                true
            }
        });
        self.estimate = tickets;
        let _ = self.estimator_sender.send(self.tickets.len() as u32);
    }

    pub fn get_available_tickets(&self) -> u32 {
        self.tickets.len() as u32 + self.estimate
    }

    pub fn run(&mut self) {
        loop {
            self.cycle();
            if self.shutdown_receiver.try_recv().is_ok() {
                break;
            }
            if let Ok(value) = self.de_activate_receiver.try_recv() {
                if value {
                    self.activate()
                } else {
                    self.deactivate()
                }
            }
        }
    }

    pub fn activate(&mut self) {
        self.status = 0;
    }

    pub fn deactivate(&mut self) {
        self.status = 1;
        self.database.lock().deallocate(self.tickets.as_slice());
        self.tickets.clear();
        if self.reserved.is_empty() {
            self.status = 2;
        }
    }
}
