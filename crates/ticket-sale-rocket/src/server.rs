//! Implementation of the server
#![allow(clippy::too_many_arguments)]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crossbeam::channel::{Receiver, Sender};
use parking_lot::Mutex;
use rand::Rng;
use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;
use super::serverrequest::HighPriorityServerRequest;

/// A server in the ticket sales system
pub struct Server {
    /// The server's ID
    pub id: Uuid,
    estimate: u32,
    /// The database
    database: Arc<Mutex<Database>>,
    status: u32,
    tickets: Vec<u32>,
    reserved: HashMap<Uuid, (u32, Instant)>,
    timeout: u32,
    low_priority: Option<Receiver<Request>>,
    high_priority: Option<Receiver<HighPriorityServerRequest>>,
    terminated_sender: Sender<Uuid>,
    estimator_sender: Sender<u32>,
    no_active_servers: Arc<Mutex<u32>>,
    server_id_list: Arc<Mutex<Vec<Uuid>>>,
}

impl Server {
    /// Create a new [`Server`]
    pub fn new(
        database: Arc<Mutex<Database>>,
        timeout: u32,
        low_priority: Receiver<Request>,
        high_priority: Receiver<HighPriorityServerRequest>,
        terminated_sender: Sender<Uuid>,
        estimator_sender: Sender<u32>,
        no_active_servers: Arc<Mutex<u32>>,
        server_id_list: Arc<Mutex<Vec<Uuid>>>,
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
            low_priority: Some(low_priority),
            high_priority: Some(high_priority),
            terminated_sender,
            estimator_sender,
            server_id_list,
            no_active_servers,
        }
    }

    pub fn get_low_priority_receiver(&mut self) -> &Receiver<Request> {
        match &self.low_priority {
            Some(value) => value,
            None => {
                panic!("fucking hell");
            }
        }
    }

    pub fn get_high_priority_receiver(&mut self) -> &Receiver<HighPriorityServerRequest> {
        match &self.high_priority {
            Some(value) => value,
            None => {
                panic!("fucking hell");
            }
        }
    }

    pub fn run(&mut self) {
        loop {
            self.cycle();
            if self.status == 2 {
                let high_priority_channel = self.get_high_priority_receiver();
                if let Ok(value) = high_priority_channel.try_recv() {
                    match value {
                        HighPriorityServerRequest::DeActivate { activate } => {
                            if activate {
                                self.activate()
                            } else {
                                self.deactivate()
                            }
                        }
                        HighPriorityServerRequest::Shutdown => self.status = 3,
                        HighPriorityServerRequest::Estimate { tickets } => {
                            self.send_tickets(tickets);
                        }
                    }
                }
                if self.status == 2 {
                    let high_priority_channel = self.high_priority.take().unwrap();
                    drop(high_priority_channel);

                    let low_priority_channel = self.low_priority.take().unwrap();
                    while let Ok(mut rq) = low_priority_channel.try_recv() {
                        let mut rng = rand::thread_rng();
                        let guard = self.no_active_servers.lock();
                        let new_serv =
                            self.server_id_list.lock()[rng.gen_range(0..*guard) as usize];
                        drop(guard);
                        rq.set_server_id(new_serv);
                        rq.respond_with_err("No Ticket Reservation allowed anymore on this server");
                    }
                    drop(low_priority_channel);

                    let _ = self.terminated_sender.send(self.id);
                    break;
                }
            }
            if self.status == 3 {
                break;
            }
        }
    }

    pub fn cycle(&mut self) {
        let high_priority_channel = self.get_high_priority_receiver();
        match high_priority_channel.try_recv() {
            Ok(value) => {
                match value {
                    HighPriorityServerRequest::DeActivate { activate } => {
                        if activate {
                            self.activate()
                        } else {
                            self.deactivate()
                        }
                    }
                    HighPriorityServerRequest::Shutdown => self.status = 3,
                    HighPriorityServerRequest::Estimate { tickets } => {
                        self.send_tickets(tickets);
                    }
                }
            }
            Err(_) => {
                let low_priority_channel = self.get_low_priority_receiver();
                if let Ok(value) = low_priority_channel.try_recv() {
                    self.handle_request(value);
                }
            }
        }
    }

    pub fn activate(&mut self) {
        self.status = 0;
    }

    pub fn deactivate(&mut self) {
        self.status = 1;
        if !self.tickets.is_empty() {
            self.database.lock().deallocate(self.tickets.as_slice());
        }
        self.tickets.clear();
        if self.reserved.is_empty() {
            self.status = 2;
        }
    }

    pub fn get_available_tickets(&self) -> u32 {
        self.tickets.len() as u32 + self.estimate
    }

    pub fn send_tickets(&mut self, tickets: u32) {
        let mut database_guard = self.database.lock();
        self.reserved.retain(|_, (ticket, time)| {
            if time.elapsed().as_secs() > self.timeout as u64 {
                if self.status == 0 {
                    self.tickets.push(*ticket);
                } else {
                    let x = *ticket;
                    database_guard.deallocate(&[x]);
                }
                false
            } else {
                true
            }
        });
        drop(database_guard);
        if self.reserved.is_empty() && self.status == 1 {
            if !self.tickets.is_empty() {
                self.database.lock().deallocate(self.tickets.as_slice());
            }
            self.tickets.clear();
            self.status = 2;
        }
        self.estimate = tickets;
        let _ = self.estimator_sender.send(self.tickets.len() as u32);
    }

    /// Handle a [`Request`]
    pub fn handle_request(&mut self, mut rq: Request) {
        let mut database_guard = self.database.lock();
        self.reserved.retain(|_, (ticket, time)| {
            if time.elapsed().as_secs() > self.timeout as u64 {
                if self.status == 0 {
                    self.tickets.push(*ticket);
                } else {
                    let x = *ticket;
                    database_guard.deallocate(&[x]);
                }
                false
            } else {
                true
            }
        });
        drop(database_guard);
        if self.reserved.is_empty() && self.status == 1 {
            if !self.tickets.is_empty() {
                self.database.lock().deallocate(self.tickets.as_slice());
            }
            self.tickets.clear();
            self.status = 2;
        }
        match rq.kind() {
            RequestKind::NumAvailableTickets => {
                rq.respond_with_int(self.get_available_tickets());
            }
            RequestKind::ReserveTicket => {
                let bloke = rq.customer_id();
                if self.reserved.contains_key(&bloke) {
                    rq.respond_with_err("one reservation already present");
                    return;
                }
                if self.status == 1 {
                    let mut rng = rand::thread_rng();
                    let guard = self.no_active_servers.lock();
                    let new_serv = self.server_id_list.lock()[rng.gen_range(0..*guard) as usize];
                    drop(guard);
                    rq.set_server_id(new_serv);
                    rq.respond_with_err("No Ticket Reservation allowed anymore on this server");
                    return;
                }
                if self.tickets.is_empty() {
                    let mut database_guard = self.database.lock();
                    if database_guard.get_num_available() == 0 {
                        rq.respond_with_sold_out();
                        return;
                    }
                    let num_tickets = (database_guard.get_num_available() as f64).sqrt() as u32;
                    self.tickets.extend(database_guard.allocate(num_tickets));
                }
                let ticket = self.tickets.pop().unwrap();
                self.reserved.insert(bloke, (ticket, Instant::now()));
                rq.respond_with_int(ticket);
            }
            RequestKind::BuyTicket => {
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
                                if !self.tickets.is_empty() {
                                    self.database.lock().deallocate(self.tickets.as_slice());
                                }
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
                                if !self.tickets.is_empty() {
                                    self.database.lock().deallocate(self.tickets.as_slice());
                                }
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
}
