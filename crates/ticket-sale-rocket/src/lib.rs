//! :rocket: Your implementation of the ticket sales system must go here.
//!
//! We already provide you with a skeleton of classes for the components of the
//! system: The [database], [load balancer][balancer], [coordinator],
//! [estimator], and [server].
//!
//! While you are free to modify anything in this module, your implementation
//! must adhere to the project description regarding the components and their
//! communication.

#![allow(rustdoc::private_intra_doc_links)]
use std::sync::{mpsc, Arc};
use std::thread;

use crossbeam::channel::unbounded;
use estimator::Estimator;
use parking_lot::Mutex;
use ticket_sale_core::Config;

mod balancer;
mod coordinator;
mod database;
mod estimator;
mod server;
mod serverrequest;
mod serverstatus;

pub use balancer::Balancer;
use coordinator::Coordinator;
use database::Database;

/// Entrypoint of your implementation
///
/// :pushpin: Hint: The function must construct a balancer which is served requests by the
/// surrounding infrastructure.
///
/// :warning: This functions must not be renamed and its signature must not be changed.
pub fn launch(config: &Config) -> Balancer {
    if config.bonus {
        todo!("Bonus not implemented!")
    }

    let database = Arc::new(Mutex::new(Database::new(config.tickets)));

    let (est_send, est_rec) = unbounded();
    let (terminated_sender, terminated_receiver) = unbounded();
    let coordinator = Arc::new(Mutex::new(Coordinator::new(
        config.timeout,
        database.clone(),
        est_send,
        terminated_sender,
    )));
    coordinator
        .lock()
        .scale_to(config.initial_servers, coordinator.clone());

    let (estimator_shutdown_sender, estimator_shutdown_receiver) = mpsc::channel();

    let mut estimator = Estimator::new(
        database.clone(),
        coordinator.clone(),
        config.estimator_roundtrip_time,
        est_rec,
        estimator_shutdown_receiver,
    );
    let other_thread = thread::spawn(move || {
        estimator.run();
    });

    let no_active_servers = coordinator.lock().no_active_servers;
    let map_id_index = coordinator.lock().map_id_index.clone();
    let server_id_list = coordinator.lock().server_id_list.clone();
    let low_priority_sender_list = coordinator.lock().low_priority_sender_list.clone();
    Balancer::new(
        coordinator.clone(),
        estimator_shutdown_sender,
        other_thread,
        Mutex::new(no_active_servers),
        Mutex::new(map_id_index),
        Mutex::new(server_id_list),
        Mutex::new(low_priority_sender_list),
        terminated_receiver,
    )
}
