//! Implementation of the server

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;

/// A server in the ticket sales system
pub struct Server2 {
    /// The server's ID
    id: Uuid,
    estimate: u32,
    /// The database
    database: Arc<Mutex<Database>>,
    status: u32,
    tickets: Vec<u32>,
    reserved: HashMap<Uuid, (u32, Instant)>,
    timeout_queue: VecDeque<(Uuid, Instant)>,
    timeout: u32,
}

impl Server2 {
    /// Create a new [`Server`]
    pub fn new(database: Arc<Mutex<Database>>, timeout: u32) -> Server2 {
        let id = Uuid::new_v4();
        let num_tickets = (database.lock().get_num_available() as f64).sqrt().ceil() as u32;
        let tickets = database.lock().allocate(num_tickets);
        Self {
            id,
            estimate: 0,
            database,
            status: 0,
            tickets,
            reserved: HashMap::new(),
            timeout_queue: VecDeque::new(),
            timeout,
        }
    }

    /// Handle a [`Request`]
    pub fn handle_request(&mut self, mut rq: Request) {
        self.remove_timeouted_reservations();
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
                let time = Instant::now();
                self.timeout_queue.push_back((bloke, time));
                self.reserved.insert(bloke, (ticket, time));
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

    pub fn send_tickets(&mut self, tickets: u32) -> u32 {
        self.remove_timeouted_reservations();
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
        self.database.lock().deallocate(self.tickets.as_slice());
        self.tickets.clear();
        if self.reserved.is_empty() {
            self.status = 2;
        }
    }

    pub fn remove_timeouted_reservations(&mut self) {
        let mut database_guard = self.database.lock();

        // while we have reservations
        while !self.timeout_queue.is_empty() {
            if self.timeout_queue.front().unwrap().1.elapsed().as_secs() <= self.timeout as u64 {
                // no more timeouted reservations
                break;
            }
            // get customer and time of reservation
            let customer = self.timeout_queue.front().unwrap().0;
            let time = self.timeout_queue.front().unwrap().1;
            self.timeout_queue.pop_front();

            // if reservation still exists
            if self.reserved.contains_key(&customer) && self.reserved[&customer].1 == time {
                let ticket = self.reserved[&customer].0;
                // if the server is active
                if self.status == 0 {
                    // return the ticket to the list
                    self.tickets.push(ticket);
                } else {
                    // otherwise, return it to the database
                    database_guard.deallocate(&[ticket]);
                }
                // remove reservation
                self.reserved.remove(&customer);
            }
        }
        drop(database_guard);

        // if no reservations are left and the server is terminating
        if self.reserved.is_empty() && self.status == 1 {
            // mark server as terminated
            self.status = 2;
        }
    }
}
