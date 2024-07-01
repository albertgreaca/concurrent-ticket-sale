//! Implementation of the estimator

use super::database::Database;

/// Estimator that estimates the number of tickets available overall
pub struct Estimator {}

impl Estimator {
    /// The estimator's main routine.
    ///
    /// `roundtrip_secs` is the time in seconds the estimator needs to contact all
    /// servers. If there are `N` servers, then the estimator should wait
    /// `roundtrip_secs / N` between each server when collecting statistics.
    pub fn new(database: Database, roundtrip_secs: u32) -> Self {
        todo!()
    }
}
