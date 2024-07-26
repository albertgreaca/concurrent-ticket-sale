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

use balancer_bonus::BalancerBonus;
use balancer_standard::BalancerStandard;
use crossbeam::channel::unbounded;
use estimator_bonus::EstimatorBonus;
use estimator_standard::EstimatorStandard;
use parking_lot::Mutex;
use ticket_sale_core::Config;

mod balancer;
mod balancer_bonus;
mod balancer_standard;
mod coordinator_bonus;
mod coordinator_standard;
mod database;
mod estimator_bonus;
mod estimator_standard;
mod server_bonus;
mod server_standard;
mod serverrequest;
mod serverstatus;

pub use balancer::Balancer;
use coordinator_bonus::CoordinatorBonus;
use coordinator_standard::CoordinatorStandard;
use database::Database;

/// Entrypoint of your implementation
///
/// :pushpin: Hint: The function must construct a balancer which is served requests by the
/// surrounding infrastructure.
///
/// :warning: This functions must not be renamed and its signature must not be changed.
pub fn launch(config: &Config) -> Balancer {
    if !config.bonus {
        let database = Arc::new(Mutex::new(Database::new(config.tickets)));

        let (estimator_sender, estimator_receiver) = unbounded();
        let (scaling_sender, scaling_receiver) = unbounded();
        let coordinator = Arc::new(Mutex::new(CoordinatorStandard::new(
            config.timeout,
            database.clone(),
            estimator_sender,
            scaling_sender,
        )));
        coordinator
            .lock()
            .scale_to(config.initial_servers, coordinator.clone());

        let (estimator_shutdown_sender, estimator_shutdown_receiver) = mpsc::channel();

        let mut estimator = EstimatorStandard::new(
            database.clone(),
            config.estimator_roundtrip_time,
            estimator_receiver,
            scaling_receiver,
            estimator_shutdown_receiver,
        );
        let other_thread = thread::spawn(move || {
            estimator.run();
        });
        let balancer_standard =
            BalancerStandard::new(coordinator, estimator_shutdown_sender, other_thread);

        Balancer::new(Some(balancer_standard), None, false)
    } else {
        let database = Arc::new(Mutex::new(Database::new(config.tickets)));

        let (estimator_sender, estimator_receiver) = unbounded();
        let (scaling_sender, scaling_receiver) = unbounded();
        let coordinator = Arc::new(Mutex::new(CoordinatorBonus::new(
            config.timeout,
            database.clone(),
            estimator_sender,
            scaling_sender,
        )));
        coordinator
            .lock()
            .scale_to(config.initial_servers, coordinator.clone());

        let (estimator_shutdown_sender, estimator_shutdown_receiver) = mpsc::channel();

        let mut estimator = EstimatorBonus::new(
            database.clone(),
            config.estimator_roundtrip_time,
            estimator_receiver,
            scaling_receiver,
            estimator_shutdown_receiver,
        );
        let other_thread = thread::spawn(move || {
            estimator.run();
        });
        let balancer_bonus =
            BalancerBonus::new(coordinator, estimator_shutdown_sender, other_thread);

        Balancer::new(None, Some(balancer_bonus), true)
    }
}
