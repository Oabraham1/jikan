// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! The `Sink` trait: the contract every event destination must satisfy.
//!
//! A sink receives decoded `ChangeEvent`s and delivers them to the downstream
//! system. Exactly-once delivery requires the sink to participate in a two-phase
//! commit protocol, as described in Carbone et al. (2015). Sinks that cannot
//! implement 2PC (e.g. the webhook sink) provide at-least-once semantics.

use crate::error::JikanError;
use crate::event::ChangeEvent;
use crate::position::Position;
use async_trait::async_trait;

/// The delivery guarantee offered by a sink implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// Each event is delivered at least once. Duplicate delivery is possible
    /// on restart. Callers that require exactly-once must implement idempotency
    /// on the receiving end.
    AtLeastOnce,
    /// Each event is delivered exactly once, using two-phase commit.
    ///
    /// Carbone et al. (2015): exactly-once across a restart boundary requires
    /// the sink to durably record which events have been committed. The pipeline
    /// checkpoints only after the sink acknowledges via `commit`.
    ExactlyOnce,
}

/// The contract every event sink must implement.
#[async_trait]
pub trait Sink: Send + Sync {
    /// The delivery guarantee this sink provides.
    fn delivery_guarantee(&self) -> DeliveryGuarantee;

    /// Begins a logical transaction covering the events that follow.
    ///
    /// For Kafka this opens a producer transaction. For at-least-once sinks
    /// this is a no-op.
    async fn begin(&mut self) -> Result<(), JikanError>;

    /// Sends a single event to the sink.
    ///
    /// The event is buffered but not yet committed. `commit` must be called
    /// to make the events durable. Between `begin` and `commit`, a crash
    /// leaves the events invisible to consumers.
    async fn send(&mut self, event: ChangeEvent) -> Result<(), JikanError>;

    /// Commits all events sent since the last `begin`.
    ///
    /// `up_to_position` is the position of the last event in this batch.
    /// After `commit` returns `Ok`, the pipeline may checkpoint this position.
    /// The ordering — commit sink, then checkpoint — is the invariant from
    /// Carbone et al. (2015) that makes crash recovery correct.
    async fn commit(&mut self, up_to_position: &Position) -> Result<(), JikanError>;

    /// Rolls back all events sent since the last `begin`.
    ///
    /// Called on error. After rollback the pipeline may retry the batch or
    /// halt, depending on the error kind.
    async fn rollback(&mut self) -> Result<(), JikanError>;
}
