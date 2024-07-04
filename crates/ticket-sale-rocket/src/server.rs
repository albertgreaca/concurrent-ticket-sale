//! Implementation of the server

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;

/// A server in the ticket sales system
pub struct Server {
    /// The server's ID
    id: Uuid,
    estimate: u32,
    /// The database
    database: Arc<Mutex<Database>>,
    status: u32,
    tickets: Vec<u32>,
    reserved: HashMap<Uuid, (u32, Instant)>,
    bought: HashMap<Uuid, u32>,
    timeout: u32,
}

impl Server {
    /// Create a new [`Server`]
    pub fn new(database: Arc<Mutex<Database>>, timeout: u32) -> Server {
        let id = Uuid::new_v4();
        let num_tickets = (database.lock().unwrap().get_num_available() as f64)
            .sqrt()
            .ceil() as u32;
        let tickets = database.lock().unwrap().allocate(num_tickets);
        Self {
            id,
            estimate: 0,
            database,
            status: 0,
            tickets,
            reserved: HashMap::new(),
            bought: HashMap::new(),
            timeout,
        }
    }

    /// Handle a [`Request`]
    pub fn handle_request(&mut self, mut rq: Request) {
        self.reserved.retain(|_, (ticket, time)| {
            if time.elapsed().as_secs() > self.timeout as u64 {
                self.tickets.push(*ticket);
                false
            } else {
                true
            }
        });
        if self.reserved.is_empty() && self.status == 1 {
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
                    if self.database.lock().unwrap().get_num_available() == 0 {
                        rq.respond_with_sold_out();
                        return;
                    }
                    let num_tickets = (self.database.lock().unwrap().get_num_available() as f64)
                        .sqrt()
                        .ceil() as u32;
                    self.tickets
                        .extend(self.database.lock().unwrap().allocate(num_tickets));
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
                            self.bought.insert(bloke, ticket);
                            rq.respond_with_int(ticket);
                            if self.reserved.is_empty() && self.status == 1 {
                                self.status = 2;
                            }
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
                                self.database.lock().unwrap().deallocate(&[ticket]);
                            }
                            rq.respond_with_int(ticket);
                            if self.reserved.is_empty() && self.status == 1 {
                                self.status = 2;
                            }
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

    pub fn send_tickets(&mut self, tickets: u32) -> u32 {
        self.estimate = tickets;
        self.tickets.len() as u32
    }

    pub fn get_available_tickets(&self) -> u32 {
        self.tickets.len() as u32 + self.estimate
    }

    pub fn get_id(&self) -> Uuid {
        self.id
    }

    pub fn get_status(&self) -> u32 {
        self.status
    }

    pub fn activate(&mut self) {
        self.status = 0;
    }

    pub fn deactivate(&mut self) {
        self.status = 1;
        self.database
            .lock()
            .unwrap()
            .deallocate(self.tickets.as_slice());
        self.tickets.clear();
        if self.reserved.is_empty() {
            self.status = 2;
        }
    }
}
