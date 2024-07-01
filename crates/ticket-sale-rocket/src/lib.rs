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

use estimator::Estimator;
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

    let database = Database::new(config.tickets);
    let balancer = Balancer::new();
    let coordinator = Coordinator::new(config.timeout, database);
    let estimator = Estimator::new(todo!(), config.estimator_roundtrip_time);

    todo!("Launch coordinator and estimator.");

    balancer
}
