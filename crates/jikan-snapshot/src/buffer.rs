// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! In-memory stream buffer: the "channel state" in the Chandy-Lamport model.
//!
//! Chandy & Lamport (1985): when the global-state recording begins, the channel
//! state (messages in transit) must be captured. Here, the channel is the
//! replication stream. The buffer holds events received after `snapshot_position`
//! in arrival order. It is drained once all tables have been snapshotted.

use jikan_core::{error::JikanError, event::ChangeEvent, position::Position, table::PrimaryKey};
use std::collections::HashMap;

/// A bounded, ordered buffer of stream events received during the snapshot phase.
///
/// Events are keyed by `(TableId, PrimaryKey)` so that the merger can quickly
/// determine whether a stream event supersedes a snapshot row.
pub struct StreamBuffer {
    /// Events stored in the order they arrived.
    events: Vec<ChangeEvent>,
    /// Index from `(table, primary_key serialised as string)` to the position
    /// of the most recent event in `events` for that key.
    index: HashMap<String, usize>,
    /// The Chandy-Lamport marker anchoring this buffer.
    snapshot_position: Position,
}

impl StreamBuffer {
    /// Creates an empty buffer anchored at `snapshot_position`.
    pub fn new(snapshot_position: Position) -> Self {
        Self {
            events: Vec::new(),
            index: HashMap::new(),
            snapshot_position,
        }
    }

    /// Buffers a stream event, verifying it is after the snapshot boundary.
    ///
    /// Returns `InvariantViolation` if the event's position is at or before
    /// `snapshot_position` — that would mean the stream rewound past the
    /// Chandy-Lamport marker (invariant 6).
    pub fn push(&mut self, event: ChangeEvent) -> Result<(), JikanError> {
        if !event.position.is_after(&self.snapshot_position) {
            return Err(JikanError::InvariantViolation(format!(
                "stream event at position {:?} is not after snapshot_position {:?}; \
                 merge boundary violated (invariant 6)",
                event.position, self.snapshot_position
            )));
        }

        if let Some(pk) = &event.primary_key {
            let key = format!("{}.{}:{}", event.table.schema, event.table.name, pk_key(pk));
            self.index.insert(key, self.events.len());
        }

        self.events.push(event);
        Ok(())
    }

    /// Returns the most recent buffered event for the given table and primary key,
    /// if one exists and is at a position after `snapshot_position`.
    ///
    /// A snapshot row for key K is superseded if the buffer contains a stream
    /// event for K at any position > `snapshot_position`.
    pub fn supersedes(
        &self,
        table: &jikan_core::table::TableId,
        pk: &PrimaryKey,
    ) -> Option<&ChangeEvent> {
        let key = format!("{}.{}:{}", table.schema, table.name, pk_key(pk));
        self.index.get(&key).map(|&idx| &self.events[idx])
    }

    /// Drains all buffered events in arrival order, consuming the buffer.
    pub fn drain(self) -> Vec<ChangeEvent> {
        self.events
    }

    /// Returns the number of events currently buffered.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Serialises a primary key into a deterministic string for use as a hash key.
fn pk_key(pk: &PrimaryKey) -> String {
    pk.0.iter()
        .map(|(k, v)| format!("{k}={v:?}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use jikan_core::{
        event::EventKind,
        position::{Lsn, Position},
        table::{ColumnValue, PrimaryKey, TableId},
    };

    fn pos(n: u64) -> Position {
        Position::Lsn(Lsn(n))
    }

    fn event(pk_val: i64, lsn: u64) -> ChangeEvent {
        ChangeEvent {
            position: pos(lsn),
            table: TableId::new("public", "users"),
            kind: EventKind::Insert,
            primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(pk_val))),
            after: None,
            before: None,
            committed_at: None,
        }
    }

    #[test]
    fn event_after_snapshot_is_accepted() {
        let mut buf = StreamBuffer::new(pos(100));
        assert!(buf.push(event(1, 101)).is_ok());
    }

    #[test]
    fn event_at_snapshot_boundary_is_rejected() {
        let mut buf = StreamBuffer::new(pos(100));
        assert!(buf.push(event(1, 100)).is_err());
    }

    #[test]
    fn event_before_snapshot_is_rejected() {
        let mut buf = StreamBuffer::new(pos(100));
        assert!(buf.push(event(1, 99)).is_err());
    }

    #[test]
    fn supersedes_finds_buffered_event() {
        let mut buf = StreamBuffer::new(pos(100));
        buf.push(event(42, 105)).unwrap();
        let table = TableId::new("public", "users");
        let pk = PrimaryKey::single("id", ColumnValue::Int(42));
        assert!(buf.supersedes(&table, &pk).is_some());
    }

    #[test]
    fn supersedes_returns_none_for_unknown_key() {
        let buf = StreamBuffer::new(pos(100));
        let table = TableId::new("public", "users");
        let pk = PrimaryKey::single("id", ColumnValue::Int(99));
        assert!(buf.supersedes(&table, &pk).is_none());
    }
}
