pub enum HighPriorityServerRequest {
    Activate,
    Deactivate,
    Shutdown,
    Estimate { tickets: u32 },
}
