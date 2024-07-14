#[derive(PartialEq)]
pub enum ServerStatus {
    Active,
    Terminating,
    Terminated,
    Shutdown,
}
