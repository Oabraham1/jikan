// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! A liveness monitor for the pipeline.
//!
//! The `PipelineWatchdog` tracks the last time each pipeline stage reported
//! progress. If any stage stalls for longer than the configured deadline,
//! the watchdog fires and the pipeline is considered unhealthy.
//!
//! This is analogous to the heartbeat mechanism described in Chandy &
//! Lamport (1985) for detecting when a channel has gone quiet.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// A named stage in the pipeline whose liveness is tracked.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StageId(pub String);

impl std::fmt::Display for StageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Tracks the last-heartbeat time for each registered pipeline stage.
///
/// Shared via `Arc<Mutex<...>>` so that pipeline stages can call `heartbeat`
/// from async tasks without holding a reference to the watchdog across
/// await points.
#[derive(Clone)]
pub struct PipelineWatchdog {
    inner: Arc<Mutex<WatchdogInner>>,
    deadline: Duration,
}

struct WatchdogInner {
    heartbeats: HashMap<StageId, Instant>,
}

impl PipelineWatchdog {
    /// Creates a new watchdog with the given liveness deadline.
    ///
    /// A stage that has not called `heartbeat` within `deadline` is
    /// considered stalled.
    pub fn new(deadline: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WatchdogInner {
                heartbeats: HashMap::new(),
            })),
            deadline,
        }
    }

    /// Registers a stage and records its initial heartbeat.
    pub fn register(&self, stage: StageId) {
        let mut inner = self.inner.lock().expect("watchdog mutex poisoned");
        inner.heartbeats.insert(stage, Instant::now());
    }

    /// Records a heartbeat for the given stage, resetting its liveness timer.
    pub fn heartbeat(&self, stage: &StageId) {
        let mut inner = self.inner.lock().expect("watchdog mutex poisoned");
        if let Some(ts) = inner.heartbeats.get_mut(stage) {
            *ts = Instant::now();
        }
    }

    /// Returns the names of all stages that have exceeded the liveness deadline.
    pub fn stalled_stages(&self) -> Vec<StageId> {
        let inner = self.inner.lock().expect("watchdog mutex poisoned");
        let now = Instant::now();
        inner
            .heartbeats
            .iter()
            .filter(|(_, last)| now.duration_since(**last) > self.deadline)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Logs a warning for each stalled stage and returns whether any stage is stalled.
    pub fn check(&self) -> bool {
        let stalled = self.stalled_stages();
        for stage in &stalled {
            warn!(stage = %stage, deadline_secs = self.deadline.as_secs(), "pipeline stage stalled");
        }
        if stalled.is_empty() {
            info!("watchdog check passed: all stages healthy");
        }
        !stalled.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn fresh_stage_is_not_stalled() {
        let wd = PipelineWatchdog::new(Duration::from_secs(60));
        let stage = StageId("stream".into());
        wd.register(stage.clone());
        assert!(!wd.check());
    }

    #[test]
    fn stale_stage_is_detected() {
        let wd = PipelineWatchdog::new(Duration::from_millis(1));
        let stage = StageId("stream".into());
        wd.register(stage);
        std::thread::sleep(Duration::from_millis(10));
        assert!(wd.check());
    }

    #[test]
    fn heartbeat_resets_timer() {
        let wd = PipelineWatchdog::new(Duration::from_millis(5));
        let stage = StageId("stream".into());
        wd.register(stage.clone());
        std::thread::sleep(Duration::from_millis(10));
        wd.heartbeat(&stage);
        assert!(!wd.check());
    }
}
