//! Implementation of the load balancer
use ticket_sale_core::{Request, RequestHandler};

use crate::balancer_bonus::BalancerBonus;
use crate::balancer_standard::BalancerStandard;

/// Implementation of the load balancer
///
/// ⚠️ This struct must implement the [`RequestHandler`] trait, and it must be
/// exposed from the crate root (to be used from the tester as
/// `ticket_sale_rocket::Balancer`).
pub struct Balancer {
    balancer_standard: Option<BalancerStandard>,
    balancer_bonus: Option<BalancerBonus>,
    bonus: bool,
}

impl Balancer {
    /// Create a new [`Balancer`]
    pub fn new(
        balancer_standard: Option<BalancerStandard>,
        balancer_bonus: Option<BalancerBonus>,
        bonus: bool,
    ) -> Self {
        Self {
            balancer_standard,
            balancer_bonus,
            bonus,
        }
    }
}

impl RequestHandler for Balancer {
    // 📌 Hint: Look into the `RequestHandler` trait definition for specification
    // docstrings of `handle()` and `shutdown()`.

    fn handle(&self, rq: Request) {
        if !self.bonus {
            let balancer = match &self.balancer_standard {
                Some(v) => v,
                None => panic!("Our panic: Standard balancer not found in request."),
            };
            balancer.handle(rq);
        } else {
            let balancer = match &self.balancer_bonus {
                Some(v) => v,
                None => panic!("Our panic: Bonus balancer not found in request."),
            };
            balancer.handle(rq);
        }
    }

    fn shutdown(self) {
        if !self.bonus {
            match self.balancer_standard {
                Some(balancer) => balancer.shutdown(),
                None => panic!("Our panic: Standard balancer not found in shutdown."),
            }
        } else {
            match self.balancer_bonus {
                Some(balancer) => balancer.shutdown(),
                None => panic!("Our panic: Bonus balancer not found in shutdown."),
            }
        }
    }
}
