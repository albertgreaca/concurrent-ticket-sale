pub enum HighPriorityServerRequest {
    DeActivate { activate: bool },
    Shutdown,
    Estimate { tickets: u32 },
}
