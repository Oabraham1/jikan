// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Change events flowing through the Jikan pipeline.
//!
//! A `RawEvent` is the uninterpreted bytes from the replication protocol.
//! A `ChangeEvent` is the decoded, structured form ready for delivery to a sink.

use crate::position::Position;
use crate::table::{ColumnValue, PrimaryKey, TableId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The kind of change represented by an event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    /// A row was inserted.
    Insert,
    /// A row was updated. The event carries both the old and new images when
    /// the source provides them (e.g. PostgreSQL with `REPLICA IDENTITY FULL`).
    Update,
    /// A row was deleted.
    Delete,
    /// A DDL statement altered the schema of a table.
    ///
    /// The pipeline pauses and checkpoints when it receives this event, then
    /// re-reads the schema before continuing.
    Ddl,
}

/// Raw bytes received from a replication protocol before decoding.
///
/// Carrying the raw form allows the snapshot engine to buffer events without
/// paying the decoding cost until the events are needed during the merge phase.
#[derive(Debug, Clone)]
pub struct RawEvent {
    /// The source-assigned position of this event.
    pub position: Position,
    /// The opaque payload from the replication protocol.
    pub payload: bytes::Bytes,
}

/// A decoded change event ready for delivery to a sink.
///
/// This is the canonical event type that flows from source connectors through
/// the snapshot merger to sinks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// The position at which this event occurred in the source.
    ///
    /// Lamport (1978): this value is the event's logical timestamp. Within
    /// a source, events are totally ordered by this field.
    pub position: Position,
    /// The table affected by this event.
    pub table: TableId,
    /// The kind of change.
    pub kind: EventKind,
    /// The primary key of the affected row.
    ///
    /// Present for all event kinds except `Ddl`.
    pub primary_key: Option<PrimaryKey>,
    /// Column values after the change (for `Insert` and `Update`).
    pub after: Option<BTreeMap<String, ColumnValue>>,
    /// Column values before the change (for `Update` and `Delete`).
    ///
    /// Only populated when the source is configured to provide the before-image
    /// (e.g. PostgreSQL `REPLICA IDENTITY FULL`, MySQL `binlog_row_image=FULL`).
    pub before: Option<BTreeMap<String, ColumnValue>>,
    /// The wall-clock time at which the source database committed this change,
    /// as reported by the replication protocol.
    ///
    /// This value is informational only. Ordering invariants use `position`,
    /// not wall-clock time, because clocks are not monotonic across nodes.
    pub committed_at: Option<chrono::DateTime<chrono::Utc>>,
}
