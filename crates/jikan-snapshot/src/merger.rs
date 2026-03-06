// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Merge boundary enforcement and per-chunk merge logic.
//!
//! Adya et al. (2000): delivering a snapshot row after a stream event for the
//! same key at a higher position would create an anti-dependency cycle. The
//! `StreamMerger` prevents this by checking the stream buffer before delivering
//! each snapshot row.

use jikan_core::{error::JikanError, event::ChangeEvent};

use crate::buffer::StreamBuffer;

/// Merges a snapshot chunk against the stream buffer.
///
/// For each snapshot row:
/// - If the buffer contains a stream event for the same key at a position
///   after `snapshot_position`, the stream event replaces the snapshot row.
/// - Otherwise the snapshot row is delivered as-is.
///
/// This implements step 5 of the snapshot algorithm.
pub struct StreamMerger<'a> {
    buffer: &'a StreamBuffer,
}

impl<'a> StreamMerger<'a> {
    /// Creates a merger that uses `buffer` to detect superseded rows.
    pub fn new(buffer: &'a StreamBuffer) -> Self {
        Self { buffer }
    }

    /// Merges a single snapshot row against the stream buffer.
    ///
    /// Returns the event to deliver: either the original snapshot row or
    /// the superseding stream event.
    ///
    /// Returns `None` if the stream event is a delete — delivering a snapshot
    /// row for a key that has been deleted in the stream would be incorrect.
    pub fn merge_row(&self, snapshot_row: ChangeEvent) -> Result<Option<ChangeEvent>, JikanError> {
        let pk = match &snapshot_row.primary_key {
            Some(pk) => pk.clone(),
            None => return Ok(Some(snapshot_row)),
        };

        match self.buffer.supersedes(&snapshot_row.table, &pk) {
            None => Ok(Some(snapshot_row)),
            Some(stream_event) => {
                use jikan_core::event::EventKind;
                match stream_event.kind {
                    EventKind::Delete => {
                        // The row was deleted after the snapshot was taken.
                        // Do not deliver the snapshot row.
                        Ok(None)
                    }
                    EventKind::Insert | EventKind::Update => {
                        // The row was modified after the snapshot was taken.
                        // Deliver the stream event, which carries the newer state.
                        Ok(Some(stream_event.clone()))
                    }
                    EventKind::Ddl => Ok(Some(snapshot_row)),
                }
            }
        }
    }

    /// Merges an entire chunk of snapshot rows.
    ///
    /// Rows that are superseded by stream deletes are dropped.
    pub fn merge_chunk(&self, rows: Vec<ChangeEvent>) -> Result<Vec<ChangeEvent>, JikanError> {
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(event) = self.merge_row(row)? {
                out.push(event);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StreamBuffer;
    use jikan_core::{
        event::EventKind,
        position::{Lsn, Position},
        table::{ColumnValue, PrimaryKey, TableId},
    };

    fn pos(n: u64) -> Position {
        Position::Lsn(Lsn(n))
    }

    fn snapshot_row(pk_val: i64) -> ChangeEvent {
        ChangeEvent {
            position: pos(100),
            table: TableId::new("public", "users"),
            kind: EventKind::Insert,
            primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(pk_val))),
            after: None,
            before: None,
            committed_at: None,
        }
    }

    fn stream_event(pk_val: i64, kind: EventKind, lsn: u64) -> ChangeEvent {
        ChangeEvent {
            position: pos(lsn),
            table: TableId::new("public", "users"),
            kind,
            primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(pk_val))),
            after: None,
            before: None,
            committed_at: None,
        }
    }

    #[test]
    fn no_stream_event_passes_snapshot_row_through() {
        let buf = StreamBuffer::new(pos(100));
        let merger = StreamMerger::new(&buf);
        let result = merger.merge_row(snapshot_row(1)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn delete_stream_event_drops_snapshot_row() {
        let mut buf = StreamBuffer::new(pos(100));
        buf.push(stream_event(1, EventKind::Delete, 150)).unwrap();
        let merger = StreamMerger::new(&buf);
        let result = merger.merge_row(snapshot_row(1)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn update_stream_event_replaces_snapshot_row() {
        let mut buf = StreamBuffer::new(pos(100));
        buf.push(stream_event(1, EventKind::Update, 150)).unwrap();
        let merger = StreamMerger::new(&buf);
        let result = merger.merge_row(snapshot_row(1)).unwrap().unwrap();
        assert_eq!(result.kind, EventKind::Update);
        assert_eq!(result.position, pos(150));
    }
}
