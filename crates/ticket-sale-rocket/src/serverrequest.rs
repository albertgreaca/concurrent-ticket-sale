#[derive(Clone)]

pub enum ServerRequest {
    DeActivate { activate: bool },
    Shutdown,
    Estimate { tickets: u32 },
}