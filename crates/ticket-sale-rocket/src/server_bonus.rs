//! Implementation of the bonus server

#![allow(clippy::too_many_arguments)]
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use crossbeam::select;
use parking_lot::Mutex;
use ticket_sale_core::{Request, RequestKind};
use uuid::Uuid;

use super::coordinator_bonus::CoordinatorBonus;
use super::database::Database;
use super::enums::EstimatorServerStatus;
use super::enums::HighPriorityServerRequest;
use super::enums::ServerStatus;
use super::enums::UserSessionStatus;

pub struct ServerBonus {
    /// The server's ID
    pub id: Uuid,

    database: Arc<Mutex<Database>>,
    coordinator: Arc<Mutex<CoordinatorBonus>>,

    /// Current server status
    status: ServerStatus,

    /// List of non-reserved tickets
    tickets: Vec<u32>,

    /// Estimate of tickets in other servers
    estimate: u32,

    /// Map from customer id to ticket id and time it was reserved
    reserved: HashMap<Uuid, (u32, Instant)>,

    /// Queue of reservations as (customer id, time of reservation)
    timeout_queue: VecDeque<(Uuid, Instant)>,

    /// The reservation timeout
    reservation_timeout: u32,

    /// Receivers for receiving requests
    low_priority: Option<Receiver<Request>>,
    high_priority: Option<Receiver<HighPriorityServerRequest>>,

    /// Sender for notifying the coordinator of the server's termination
    coordinator_terminated_sender: mpsc::Sender<Uuid>,

    /// Sender for sending the server's number of tickets to the estimator
    estimator_tickets_sender: mpsc::Sender<u32>,

    /// Sender for notifying the estimator of the server's termination
    estimator_scaling_sender: mpsc::Sender<EstimatorServerStatus>,

    user_session_sender: Sender<UserSessionStatus>,
}

impl ServerBonus {
    /// Create a new [`ServerBonus`]
    pub fn new(
        database: Arc<Mutex<Database>>,
        coordinator: Arc<Mutex<CoordinatorBonus>>,
        reservation_timeout: u32,
        low_priority: Receiver<Request>,
        high_priority: Receiver<HighPriorityServerRequest>,
        coordinator_terminated_sender: mpsc::Sender<Uuid>,
        estimator_tickets_sender: mpsc::Sender<u32>,
        estimator_scaling_sender: mpsc::Sender<EstimatorServerStatus>,
        user_session_sender: Sender<UserSessionStatus>,
    ) -> Self {
        let id = Uuid::new_v4();
        Self {
            id,
            database,
            coordinator,
            status: ServerStatus::Active,
            tickets: Vec::new(),
            estimate: 0,
            reserved: HashMap::new(),
            timeout_queue: VecDeque::new(),
            reservation_timeout,
            low_priority: Some(low_priority),
            high_priority: Some(high_priority),
            coordinator_terminated_sender,
            estimator_tickets_sender,
            estimator_scaling_sender,
            user_session_sender,
        }
    }

    /// Get the receiver for low priority requests
    pub fn get_low_priority_receiver(&self) -> &Receiver<Request> {
        match &self.low_priority {
            Some(value) => value,
            None => {
                panic!("Our panic: couldn't get low priority receiver");
            }
        }
    }

    /// Get the receiver for high priority requests
    pub fn get_high_priority_receiver(&self) -> &Receiver<HighPriorityServerRequest> {
        match &self.high_priority {
            Some(value) => value,
            None => {
                panic!("Our panic: couldn't get high priority receiver");
            }
        }
    }

    /// Main server loop
    pub fn run(&mut self) {
        loop {
            // Process the next request
            self.process_request();

            // If the server is supposedly terminated after that request
            if self.status == ServerStatus::Terminated {
                // Respond to all high priority requests to make sure
                // it is still supposed to terminate
                // i.e. no requests to activate enqueued
                while self.try_process_high_priority() {}

                // If the server is still terminated
                if self.status == ServerStatus::Terminated {
                    // Notify the estimator of the server termination
                    let _ = self
                        .estimator_scaling_sender
                        .send(EstimatorServerStatus::Deactivated { server: self.id });

                    // Notify the coordinator of the server termination
                    let _ = self.coordinator_terminated_sender.send(self.id);

                    // Drop the high priority receiver to prevent
                    // further requests from being sent
                    let high_priority_receiver = self.high_priority.take().unwrap();
                    drop(high_priority_receiver);

                    // Assign a new server to all low priority requests
                    let low_priority_receiver = self.low_priority.take().unwrap();
                    while let Ok(mut rq) = low_priority_receiver.try_recv() {
                        let coordinator_guard = self.coordinator.lock();
                        let (x, _) = coordinator_guard.get_random_server_sender();
                        rq.set_server_id(x);
                        rq.respond_with_err("Our error: Server no longer exists.");
                    }

                    // Drop the low priority receiver to prevent
                    // further reuqests from begin sent
                    drop(low_priority_receiver);

                    // Terminate the server
                    break;
                }
            }

            // If the server needs to shut down after that request
            if self.status == ServerStatus::Shutdown {
                // Terminate the server
                break;
            }
        }
    }

    /// Processes the next request
    /// giving priority to high priority requests
    pub fn process_request(&mut self) {
        let high_priority_receiver = self.get_high_priority_receiver();
        match high_priority_receiver.try_recv() {
            Ok(rq) => {
                self.process_high_priority(rq);
            }
            Err(_) => {
                let low_priority_receiver = self.get_low_priority_receiver();
                match low_priority_receiver.try_recv() {
                    Ok(rq) => {
                        self.process_low_priority(rq);
                    }
                    Err(_) => {
                        // Avoid busy wait
                        self.wait_for_requests();
                    }
                }
            }
        }
    }

    /// Waits for any request, then processes it
    pub fn wait_for_requests(&mut self) {
        let high_priority_receiver = self.get_high_priority_receiver();
        let low_priority_receiver = self.get_low_priority_receiver();

        select! {
            recv(high_priority_receiver) -> msg => {
                match msg {
                    Ok(rq) => {
                        self.process_high_priority(rq);
                    }
                    Err(_) => {
                        panic!("Our panic: Select recv gave Err on high priority.");
                    }
                }
            }
            recv(low_priority_receiver) -> msg => {
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

    /// Tries to process a high priority request
    /// returns true if there is one, false otherwise
    pub fn try_process_high_priority(&mut self) -> bool {
        let high_priority_receiver = self.get_high_priority_receiver();
        if let Ok(rq) = high_priority_receiver.try_recv() {
            self.process_high_priority(rq);
            true
        } else {
            false
        }
    }

    /// Processes a given high priority request
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

    /// Activate the server
    pub fn activate(&mut self) {
        // If the server is supposed to shut down, do not interfere
        if self.status == ServerStatus::Shutdown {
            return;
        }
        self.status = ServerStatus::Active;
    }

    /// Deactivate the server
    pub fn deactivate(&mut self) {
        // If the server is supposed to shut down, do not interfere
        if self.status == ServerStatus::Shutdown {
            return;
        }
        self.status = ServerStatus::Terminating;

        // Clear all non-reserved tickets
        if !self.tickets.is_empty() {
            self.database.lock().deallocate(self.tickets.as_slice());
            self.tickets.clear();
        }

        // If there are no reservations left, mark it as terminated
        if self.reserved.is_empty() {
            self.status = ServerStatus::Terminated;
        }
    }

    /// Removes reservations that have timed out
    pub fn remove_timeouted_reservations(&mut self) {
        let mut database_guard = self.database.lock();

        // While we have reservations
        while !self.timeout_queue.is_empty() {
            if self.timeout_queue.front().unwrap().1.elapsed().as_secs()
                <= self.reservation_timeout as u64
            {
                // No more timeouted reservations
                break;
            }
            // Get customer and time of reservation
            let customer = self.timeout_queue.front().unwrap().0;
            let time = self.timeout_queue.front().unwrap().1;
            self.timeout_queue.pop_front();

            // If reservation still exists
            if self.reserved.contains_key(&customer) && self.reserved[&customer].1 == time {
                let ticket = self.reserved[&customer].0;
                // If the server is active
                if self.status == ServerStatus::Active {
                    // Return the ticket to the list
                    self.tickets.push(ticket);
                } else {
                    // Otherwise, return it to the database
                    database_guard.deallocate(&[ticket]);
                }
                // Remove reservation
                self.reserved.remove(&customer);

                // Notify the balancer of the finished user session
                let _ = self
                    .user_session_sender
                    .send(UserSessionStatus::Deactivated { user: customer });
            }
        }
        drop(database_guard);

        // If no reservations are left and the server is terminating
        if self.reserved.is_empty() && self.status == ServerStatus::Terminating {
            // Mark server as terminated
            self.status = ServerStatus::Terminated;
        }
    }

    /// Stores estimate and sends its number of tickets to the estimator
    pub fn send_tickets(&mut self, tickets: u32) {
        // Remove reservations that have timed out
        self.remove_timeouted_reservations();

        self.estimate = tickets;
        let _ = self
            .estimator_tickets_sender
            .send(self.tickets.len() as u32);
    }

    /// Processes a given low priority request
    pub fn process_low_priority(&mut self, rq: Request) {
        // Remove reservations that have timed out
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

    /// Get number of available tickets
    pub fn get_available_tickets(&self) -> u32 {
        self.tickets.len() as u32 + self.estimate
    }

    /// Process a reservation request
    pub fn process_reservation(&mut self, mut rq: Request) {
        // Get the customer id and check if he already has a reservation
        let customer = rq.customer_id();
        if self.reserved.contains_key(&customer) {
            rq.respond_with_err("Our error: One reservation already present.");
            return;
        }

        // If the server is terminating
        if self.status == ServerStatus::Terminating {
            // Assign a new server and respond with error
            let coordinator_guard = self.coordinator.lock();
            let (x, _) = coordinator_guard.get_random_server_sender();
            rq.set_server_id(x);
            rq.respond_with_err("Our error: Ticket reservations no longer allowed on this server");
            return;
        }

        // If server doesn't have any tickets
        if self.tickets.is_empty() {
            let mut database_guard = self.database.lock();

            // If the database also doesn't have tickets => sold out
            if database_guard.get_num_available() == 0 {
                rq.respond_with_sold_out();
                return;
            }

            // Get the number of tickets in the database
            let database_tickets = database_guard.get_num_available();

            // Determine number of tickets to allocate
            let num_tickets = (database_tickets as f64).sqrt() as u32;

            // Allocate the tickets
            self.tickets.extend(database_guard.allocate(num_tickets));
        }

        // Reserve the last ticket
        let ticket = self.tickets.pop().unwrap();
        let time = Instant::now();
        self.reserved.insert(customer, (ticket, time));
        self.timeout_queue.push_back((customer, time));

        // Notify the balancer of the started user session
        let _ = self
            .user_session_sender
            .send(UserSessionStatus::Activated { user: customer });

        rq.respond_with_int(ticket);
    }

    /// Process a buy request
    pub fn process_buy(&mut self, mut rq: Request) {
        // Make sure the request has a ticket id
        let ticket_option = rq.read_u32();
        if ticket_option.is_none() {
            rq.respond_with_err("Our error: No ticket id given.");
        } else {
            // Get ticket id and customer id
            let ticket = ticket_option.unwrap();
            let customer = rq.customer_id();

            // Attempt to remove a reservation for this customer
            let reservation = self.reserved.remove(&customer);

            match reservation {
                // Make sure the customer has a reservation
                Some((reservation_ticket, time)) => {
                    // And that it reserved that specific ticket
                    if reservation_ticket == ticket {
                        // Terminate server if this was the last reservation and server was
                        // terminating
                        if self.reserved.is_empty() && self.status == ServerStatus::Terminating {
                            self.status = ServerStatus::Terminated;
                        }

                        // Notify the balancer of the finished user session
                        let _ = self
                            .user_session_sender
                            .send(UserSessionStatus::Deactivated { user: customer });

                        rq.respond_with_int(ticket);
                    } else {
                        // Insert the reservation back so it can still be bought later
                        self.reserved.insert(customer, (reservation_ticket, time));
                        rq.respond_with_err(
                            "Our error: Reservation not made for that ticket for buy request.",
                        )
                    }
                }
                None => rq.respond_with_err("Our error: No reservation for buy request."),
            }
        }
    }

    /// Process a cancel request
    pub fn process_cancel(&mut self, mut rq: Request) {
        // Make sure the request has a ticket id
        let ticket_option = rq.read_u32();
        if ticket_option.is_none() {
            rq.respond_with_err("Our error: No ticket id given.");
        } else {
            // Get ticket id and customer id
            let ticket = ticket_option.unwrap();
            let customer = rq.customer_id();

            // Attempt to remove a reservation for this customer
            let reservation = self.reserved.remove(&customer);

            match reservation {
                // Make sure the customer has a reservation
                Some((reservation_ticket, time)) => {
                    // And that it reserved that specific ticket
                    if reservation_ticket == ticket {
                        // Return ticket to non-reserved list or database
                        if self.status == ServerStatus::Active {
                            self.tickets.push(ticket);
                        } else {
                            self.database.lock().deallocate(&[ticket]);
                        }

                        // Terminate server if this was the last reservation and server was
                        // terminating
                        if self.reserved.is_empty() && self.status == ServerStatus::Terminating {
                            self.status = ServerStatus::Terminated;
                        }

                        // Notify the balancer of the finished user session
                        let _ = self
                            .user_session_sender
                            .send(UserSessionStatus::Deactivated { user: customer });

                        rq.respond_with_int(ticket);
                    } else {
                        // Insert the reservation back so it can still be cancelled later
                        self.reserved.insert(customer, (reservation_ticket, time));
                        rq.respond_with_err(
                            "Our error: Reservation not made for that ticket for buy request.",
                        )
                    }
                }
                None => rq.respond_with_err("Our error: No reservation for cancel request."),
            }
        }
    }
}
