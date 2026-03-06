// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! A sink that writes events to stdout as newline-delimited JSON.
//!
//! Intended for development, debugging, and CI tests where no external
//! broker is available. At-least-once semantics only.

use crate::{
    error::JikanError,
    event::ChangeEvent,
    position::Position,
    sink::{DeliveryGuarantee, Sink},
};
use async_trait::async_trait;

/// A sink that writes each event as a JSON line to stdout.
pub struct StdoutSink {
    pending: Vec<ChangeEvent>,
}

impl StdoutSink {
    /// Creates a new `StdoutSink`.
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
        }
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for StdoutSink {
    fn delivery_guarantee(&self) -> DeliveryGuarantee {
        DeliveryGuarantee::AtLeastOnce
    }

    async fn begin(&mut self) -> Result<(), JikanError> {
        self.pending.clear();
        Ok(())
    }

    async fn send(&mut self, event: ChangeEvent) -> Result<(), JikanError> {
        self.pending.push(event);
        Ok(())
    }

    async fn commit(&mut self, _up_to_position: &Position) -> Result<(), JikanError> {
        for event in self.pending.drain(..) {
            let line = serde_json::to_string(&event)
                .map_err(|e| JikanError::SinkDelivery(format!("serialise: {e}")))?;
            println!("{line}");
        }
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), JikanError> {
        self.pending.clear();
        Ok(())
    }
}
