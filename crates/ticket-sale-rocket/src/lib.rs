//! 🚀 Your implementation of the ticket sales system must go here.
//!
//! We already provide you with a skeleton of classes for the components of the
//! system: The [database], [load balancer][balancer], [coordinator],
//! [estimator], and [server].
//!
//! While you are free to modify anything in this module, your implementation
//! must adhere to the project description regarding the components and their
//! communication.

#![allow(rustdoc::private_intra_doc_links)]

use std::sync::Arc;
use std::thread;

use estimator::Estimator;
use parking_lot::Mutex;
use ticket_sale_core::Config;

mod balancer;
mod coordinator;
mod database;
mod estimator;
mod server;

pub use balancer::Balancer;
use coordinator::Coordinator;
use database::Database;

/// Entrypoint of your implementation
///
/// 📌 Hint: The function must construct a balancer which is served requests by the
/// surrounding infrastructure.
///
/// ⚠️ This functions must not be renamed and its signature must not be changed.
pub fn launch(config: &Config) -> Balancer {
    if config.bonus {
        todo!("Bonus not implemented!")
    }

    let database = Arc::new(Mutex::new(Database::new(config.tickets)));
    let coordinator = Arc::new(Coordinator::new(config.timeout, database.clone()));
    coordinator.scale_to(config.initial_servers);
    let estimator = Arc::new(Mutex::new(Estimator::new(
        database.clone(),
        coordinator.clone(),
        config.estimator_roundtrip_time,
    )));
    let estimator2 = estimator.clone();
    let other_thread = thread::spawn(move || {
        while Arc::strong_count(&estimator2) > 1 {
            estimator2.lock().run();
        }
    });
    Balancer::new(coordinator.clone(), estimator, other_thread)
}
