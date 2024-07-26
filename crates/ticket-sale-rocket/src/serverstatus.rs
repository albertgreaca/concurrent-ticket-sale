use crossbeam::channel::Sender;
use uuid::Uuid;

use crate::serverrequest::HighPriorityServerRequest;

#[derive(PartialEq)]
pub enum ServerStatus {
    Active,
    Terminating,
    Terminated,
    Shutdown,
}

pub enum EstimatorServerStatus {
    Activated {
        server: Uuid,
        sender: Sender<HighPriorityServerRequest>,
    },
    Deactivated {
        server: Uuid,
    },
}

pub enum UserSessionStatus {
    Activated { user: Uuid },
    Deactivated { user: Uuid },
}