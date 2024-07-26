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
    // Create the database
    let database = Arc::new(Mutex::new(Database::new(config.tickets)));

    // Create estimator channels
    let (estimator_tickets_sender, estimator_tickets_receiver) = unbounded();
    let (estimator_scaling_sender, estimator_scaling_receiver) = unbounded();
    let (estimator_shutdown_sender, estimator_shutdown_receiver) = mpsc::channel();

    if !config.bonus {
        let (estimator_tickets_sender, estimator_tickets_receiver) = mpsc::channel();
        let (estimator_scaling_sender, estimator_scaling_receiver) = mpsc::channel();
        // Create the coordinator and scale to initial number of servers
        let coordinator = Arc::new(Mutex::new(CoordinatorStandard::new(
            database.clone(),
            config.timeout,
            estimator_tickets_sender,
            estimator_scaling_sender,
        )));
        coordinator
            .lock()
            .scale_to(config.initial_servers, coordinator.clone());

        // Create the estimator and start it
        let mut estimator = EstimatorStandard::new(
            database.clone(),
            config.estimator_roundtrip_time,
            estimator_tickets_receiver,
            estimator_scaling_receiver,
            estimator_shutdown_receiver,
        );
        let estimator_thread = thread::spawn(move || {
            estimator.run();
        });

        // Create the standard balancer
        let balancer_standard =
            BalancerStandard::new(coordinator, estimator_shutdown_sender, estimator_thread);

        // Create the balancer
        Balancer::new(Some(balancer_standard), None, false)
    } else {
        let (user_session_sender, user_session_receiver) = unbounded();
        // Create the coordinator and scale to initial number of servers
        let coordinator = Arc::new(Mutex::new(CoordinatorBonus::new(
            database.clone(),
            config.timeout,
            estimator_tickets_sender,
            estimator_scaling_sender,
            user_session_sender,
        )));
        coordinator
            .lock()
            .scale_to(config.initial_servers, coordinator.clone());

        // Create the estimator and start it
        let mut estimator = EstimatorBonus::new(
            database.clone(),
            config.estimator_roundtrip_time,
            estimator_tickets_receiver,
            estimator_scaling_receiver,
            estimator_shutdown_receiver,
        );
        let estimator_thread = thread::spawn(move || {
            estimator.run();
        });

        // Create the bonus balancer
        let balancer_bonus = BalancerBonus::new(
            coordinator,
            estimator_shutdown_sender,
            estimator_thread,
            user_session_receiver,
        );

        // Create the balancer
        Balancer::new(None, Some(balancer_bonus), true)
    }
}
