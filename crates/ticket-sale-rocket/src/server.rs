//! Implementation of the server
#![allow(clippy::too_many_arguments)]
use std::cmp::min;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use crossbeam::channel::{Receiver, Sender};
use crossbeam::select;
use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::database::Database;
use super::serverrequest::HighPriorityServerRequest;
use crate::coordinator::Coordinator;
use crate::serverstatus::ServerStatus;

/// A server in the ticket sales system
pub struct Server {
    /// The server's ID
    pub id: Uuid,
    estimate: u32,
    /// The database
    database: Arc<Mutex<Database>>,
    coordinator: Arc<Mutex<Coordinator>>,

    /// current server status
    status: ServerStatus,

    /// list of non-reserved tickets
    tickets: Vec<u32>,

    /// map from customer id to ticket id and time it was reserved
    reserved: HashMap<Uuid, (u32, Instant)>,
    timeout_queue: VecDeque<(Uuid, Instant)>,
    timeout: u32,

    /// channels for receiving requests
    low_priority: Option<Receiver<Request>>,
    high_priority: Option<Receiver<HighPriorityServerRequest>>,

    /// server sends its id through this channel once it terminated
    terminated_sender: Sender<Uuid>,
    /// channel through which it sends its number of tickets to the estimator
    estimator_sender: Sender<u32>,
}

impl Server {
    /// Create a new [`Server`]
    pub fn new(
        database: Arc<Mutex<Database>>,
        coordinator: Arc<Mutex<Coordinator>>,
        timeout: u32,
        low_priority: Receiver<Request>,
        high_priority: Receiver<HighPriorityServerRequest>,
        terminated_sender: Sender<Uuid>,
        estimator_sender: Sender<u32>,
    ) -> Server {
        let id = Uuid::new_v4();
        let database_tickets = database.lock().get_num_available();

        let num_tickets = min(
            ((database_tickets as f64).sqrt() as u32) * 2,
            database_tickets,
        );

        let tickets = database.lock().allocate(num_tickets);
        Self {
            id,
            estimate: 0,
            database,
            coordinator,
            status: ServerStatus::Active,
            tickets,
            reserved: HashMap::new(),
            timeout_queue: VecDeque::new(),
            timeout,
            low_priority: Some(low_priority),
            high_priority: Some(high_priority),
            terminated_sender,
            estimator_sender,
        }
    }

    /// get the receiver for receiving low priority requests
    pub fn get_low_priority_receiver(&self) -> &Receiver<Request> {
        match &self.low_priority {
            Some(value) => value,
            None => {
                panic!("Our panic: couldn't get low priority receiver");
            }
        }
    }

    /// get the receiver for receiving high priority requests
    pub fn get_high_priority_receiver(&self) -> &Receiver<HighPriorityServerRequest> {
        match &self.high_priority {
            Some(value) => value,
            None => {
                panic!("Our panic: couldn't get high priority receiver");
            }
        }
    }

    /// main server loop
    pub fn run(&mut self) {
        loop {
            // process the next request
            self.process_request();

            // if the server is supposedly terminated after that request
            if self.status == ServerStatus::Terminated {
                // respond to all high priority requests to make sure
                // it is still supposed to terminate
                // i.e. no requests to activate enqueued
                while self.try_process_high_priority() {}

                // if the server is still terminated
                if self.status == ServerStatus::Terminated {
                    // drop the high priority receiver to prevent
                    // further requests from being sent
                    let high_priority_channel = self.high_priority.take().unwrap();
                    drop(high_priority_channel);

                    // assign a new server to all low priority requests
                    let low_priority_channel = self.low_priority.take().unwrap();
                    while let Ok(mut rq) = low_priority_channel.try_recv() {
                        let coordinator_guard = self.coordinator.lock();
                        let (x, _) = coordinator_guard.get_random_server_sender();
                        rq.set_server_id(x);
                        rq.respond_with_err("Our error: Server no longer exists.");
                    }

                    // drop the low priority receiver to prevent
                    // further reuqests from begin sent
                    drop(low_priority_channel);

                    // send the server id to signal that the server terminated
                    let _ = self.terminated_sender.send(self.id);

                    // terminate the server
                    break;
                }
            }

            // if the server needs to shut down after that request
            if self.status == ServerStatus::Shutdown {
                // terminate the server
                break;
            }
        }
    }

    /// processes the next request
    /// giving priority to high priority requests
    pub fn process_request(&mut self) {
        let high_priority_channel = self.get_high_priority_receiver();
        match high_priority_channel.try_recv() {
            Ok(rq) => {
                self.process_high_priority(rq);
            }
            Err(_) => {
                let low_priority_channel = self.get_low_priority_receiver();
                match low_priority_channel.try_recv() {
                    Ok(rq) => {
                        self.process_low_priority(rq);
                    }
                    Err(_) => {
                        self.wait_for_requests();
                    }
                }
            }
        }
    }

    /// if no requests are available, wait for one, then process it
    pub fn wait_for_requests(&mut self) {
        let high_priority_channel = self.get_high_priority_receiver();
        let low_priority_channel = self.get_low_priority_receiver();
        select! {
            recv(high_priority_channel) -> msg => {
                match msg {
                    Ok(rq) => {
                        self.process_high_priority(rq);
                    }
                    Err(_) => {
                        panic!("Our panic: Select recv gave Err on high priority.");
                    }
                }
            }
            recv(low_priority_channel) -> msg => {
                match msg {
                    Ok(rq) => {
                        self.process_low_priority(rq);
                    }
                    Err(_) => {
                        panic!("Our panic: Select recv gave Err on low priority.");
                    }
                }
            }
        }
    }

    /// processes a high priority request
    /// returns true if there is one, false otherwise
    pub fn try_process_high_priority(&mut self) -> bool {
        let high_priority_channel = self.get_high_priority_receiver();
        if let Ok(rq) = high_priority_channel.try_recv() {
            self.process_high_priority(rq);
            true
        } else {
            false
        }
    }

    /// processes a given high priority request
    pub fn process_high_priority(&mut self, rq: HighPriorityServerRequest) {
        match rq {
            HighPriorityServerRequest::Activate => self.activate(),
            HighPriorityServerRequest::Deactivate => self.deactivate(),
            HighPriorityServerRequest::Shutdown => self.status = ServerStatus::Shutdown,
            HighPriorityServerRequest::Estimate { tickets } => {
                self.send_tickets(tickets);
            }
        }
    }

    /// activate the server
    pub fn activate(&mut self) {
        // if the server is supposed to shut down do not interfere
        if self.status == ServerStatus::Shutdown {
            return;
        }
        self.status = ServerStatus::Active;
    }

    /// deactivate the server
    pub fn deactivate(&mut self) {
        // if the server is supposed to shut down do not interfere
        if self.status == ServerStatus::Shutdown {
            return;
        }
        self.status = ServerStatus::Terminating;

        // clear all non-reserved tickets
        if !self.tickets.is_empty() {
            self.database.lock().deallocate(self.tickets.as_slice());
            self.tickets.clear();
        }

        // if there are no reservations left, mark it as terminated
        if self.reserved.is_empty() {
            self.status = ServerStatus::Terminated;
        }
    }

    /// removes reservations that have timed out
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
                if self.status == ServerStatus::Active {
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
        if self.reserved.is_empty() && self.status == ServerStatus::Terminating {
            // mark server as terminated
            self.status = ServerStatus::Terminated;
        }
    }

    /// stores estimate and sends its number of tickets to the estimator
    pub fn send_tickets(&mut self, tickets: u32) {
        self.remove_timeouted_reservations();
        self.estimate = tickets;
        let _ = self.estimator_sender.send(self.tickets.len() as u32);
    }

    /// processes a given low priority request.
    pub fn process_low_priority(&mut self, rq: Request) {
        self.remove_timeouted_reservations();
        match rq.kind() {
            RequestKind::NumAvailableTickets => {
                rq.respond_with_int(self.get_available_tickets());
            }
            RequestKind::ReserveTicket => {
                self.process_reservation(rq);
            }
            RequestKind::BuyTicket => {
                self.process_buy(rq);
            }
            RequestKind::AbortPurchase => {
                self.process_cancel(rq);
            }
            _ => {
                rq.respond_with_err("Our error: RequestKind not found.");
            }
        }
    }

    /// get number of available tickets
    pub fn get_available_tickets(&self) -> u32 {
        self.tickets.len() as u32 + self.estimate
    }

    /// process a reservation
    pub fn process_reservation(&mut self, mut rq: Request) {
        // get the customer and check if he already has a reservation
        let customer = rq.customer_id();
        if self.reserved.contains_key(&customer) {
            rq.respond_with_err("Our error: One reservation already present.");
            return;
        }

        // if the server is terminating
        if self.status == ServerStatus::Terminating {
            // assign a new server and respond with error
            let coordinator_guard = self.coordinator.lock();
            let (x, _) = coordinator_guard.get_random_server_sender();
            rq.set_server_id(x);
            rq.respond_with_err("Our error: Ticket reservations no longer allowed on this server");
            return;
        }

        // if server doesn't have any tickets
        if self.tickets.is_empty() {
            let mut database_guard = self.database.lock();

            if database_guard.get_num_available() == 0 {
                rq.respond_with_sold_out();
                return;
            }

            // get tickets from database
            let database_tickets = database_guard.get_num_available();

            let num_tickets = min(
                ((database_tickets as f64).sqrt() as u32) * 2,
                database_tickets,
            );

            self.tickets.extend(database_guard.allocate(num_tickets));
        }

        // reserve the last ticket
        let ticket = self.tickets.pop().unwrap();
        let time = Instant::now();
        self.reserved.insert(customer, (ticket, time));
        self.timeout_queue.push_back((customer, time));
        rq.respond_with_int(ticket);
    }

    /// process a buy request
    pub fn process_buy(&mut self, mut rq: Request) {
        // make sure the request has a ticket id
        let ticket_option = rq.read_u32();
        if ticket_option.is_none() {
            rq.respond_with_err("Our error: No ticket id given.");
        } else {
            // get ticket id and customer id
            let ticket = ticket_option.unwrap();
            let customer = rq.customer_id();

            // make sure the customer has a reservation
            if self.reserved.contains_key(&customer) {
                // and that it reserved that specific ticket
                if self.reserved[&customer].0 == ticket {
                    // remove the reservation
                    self.reserved.remove(&customer);
                    // terminate server if this was the last reservation and server was terminating
                    if self.reserved.is_empty() && self.status == ServerStatus::Terminating {
                        self.status = ServerStatus::Terminated;
                    }
                    rq.respond_with_int(ticket);
                } else {
                    rq.respond_with_err(
                        "Our error: Reservation not made for that ticket for buy request.",
                    )
                }
            } else {
                rq.respond_with_err("Our error: No reservation for buy request.")
            }
        }
    }

    pub fn process_cancel(&mut self, mut rq: Request) {
        // make sure the request has a ticket id
        let ticket_option = rq.read_u32();
        if ticket_option.is_none() {
            rq.respond_with_err("Our error: No ticket id given.");
        } else {
            // get ticket id and customer id
            let ticket = ticket_option.unwrap();
            let customer = rq.customer_id();

            // make sure the customer has a reservation
            if self.reserved.contains_key(&customer) {
                // and that it reserved that specific ticket
                if self.reserved[&customer].0 == ticket {
                    // remove the reservation
                    self.reserved.remove(&customer);

                    // return ticket to non-reserved list or database
                    if self.status == ServerStatus::Active {
                        self.tickets.push(ticket);
                    } else {
                        self.database.lock().deallocate(&[ticket]);
                    }

                    // terminate server if this was the last reservation and server was terminating
                    if self.reserved.is_empty() && self.status == ServerStatus::Terminating {
                        self.status = ServerStatus::Terminated;
                    }
                    rq.respond_with_int(ticket);
                } else {
                    rq.respond_with_err(
                        "Our error: Reservation not made for that ticket for cancel request.",
                    )
                }
            } else {
                rq.respond_with_err("Our error: No reservation for cancel request.")
            }
        }
    }
}
