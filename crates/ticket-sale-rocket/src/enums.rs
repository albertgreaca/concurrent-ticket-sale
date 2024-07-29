use crossbeam::channel::Sender;
use uuid::Uuid;

pub enum EstimatorServerStatus {
    Activated {
        server: Uuid,
        sender: Sender<HighPriorityServerRequest>,
    },
    Deactivated {
        server: Uuid,
    },
}

pub enum HighPriorityServerRequest {
    Activate,
    Deactivate,
    Shutdown,
    Estimate { tickets: u32 },
}

#[derive(PartialEq)]
pub enum ServerStatus {
    Active,
    Terminating,
    Terminated,
    Shutdown,
}
