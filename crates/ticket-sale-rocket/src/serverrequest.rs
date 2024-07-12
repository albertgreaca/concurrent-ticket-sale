enum ServerRequest {
    De_Activate,
    Shutdown,
    Estimate { tickets: u32 }
}