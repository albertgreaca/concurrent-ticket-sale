//! Implementation of the balancer
use ticket_sale_core::{Request, RequestHandler};

use crate::balancer_bonus::BalancerBonus;
use crate::balancer_standard::BalancerStandard;

pub struct Balancer {
    // May contain the regular balancer or the one used for the bonus
    balancer_standard: Option<BalancerStandard>,
    balancer_bonus: Option<BalancerBonus>,

    // If we are in the bonus or not
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
    /// Handle a given request
    fn handle(&self, rq: Request) {
        // Forward the request to the appropriate balancer
        if self.bonus {
            match &self.balancer_standard {
                Some(balancer) => balancer.handle(rq),
                None => panic!("Our panic: Standard balancer not found in request."),
            };
        } else {
            match &self.balancer_bonus {
                Some(balancer) => balancer.handle(rq),
                None => panic!("Our panic: Bonus balancer not found in request."),
            };
        }
    }

    /// Shutdown the system
    fn shutdown(self) {
        // Forward to the appropriate balancer
        if self.bonus {
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
