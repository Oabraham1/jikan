// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! The unified error type for the Jikan pipeline.
//!
//! Every crate in the workspace converts its local errors into `JikanError`
//! before returning them across a crate boundary. This keeps error handling
//! uniform at the pipeline level while allowing each connector to express
//! connector-specific failure modes.

use thiserror::Error;

/// A failure that can occur anywhere in the Jikan pipeline.
#[derive(Debug, Error)]
pub enum JikanError {
    /// The source database refused or dropped the connection.
    #[error("source connection failed: {0}")]
    SourceConnection(String),

    /// The replication slot or change stream returned a malformed event.
    #[error("replication protocol error: {0}")]
    ReplicationProtocol(String),

    /// A snapshot operation failed, either during setup or while reading a chunk.
    #[error("snapshot error: {0}")]
    Snapshot(String),

    /// The sink rejected an event or failed to acknowledge delivery.
    #[error("sink delivery failed: {0}")]
    SinkDelivery(String),

    /// A checkpoint could not be read from or written to durable storage.
    #[error("checkpoint I/O error: {0}")]
    CheckpointIo(String),

    /// The pipeline detected a violation of one of its core correctness invariants.
    ///
    /// This error is non-recoverable. The pipeline must halt and the operator
    /// must investigate before resuming, because the state on disk may be
    /// inconsistent.
    #[error("invariant violation: {0}")]
    InvariantViolation(String),

    /// An operation was cancelled, typically because the pipeline is shutting down.
    #[error("operation cancelled")]
    Cancelled,
}
