// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Durable pipeline state: checkpoints and chunk cursors.
//!
//! A `Checkpoint` is the complete resumable state of the pipeline at a
//! given moment. Writing it to disk before advancing the position satisfies
//! the "persist before deliver" requirement from Carbone et al. (2015),
//! which is the basis for exactly-once delivery on resume.

use crate::position::Position;
use crate::table::TableId;
use serde::{Deserialize, Serialize};

/// The current phase of the pipeline, stored inside a `Checkpoint`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelinePhase {
    /// The pipeline is performing the initial table snapshot.
    ///
    /// The `cursor` tracks progress through the current table. `pending`
    /// holds tables not yet started; `completed` holds tables fully delivered.
    Snapshotting {
        /// Progress through the table currently being snapshotted.
        cursor: ChunkCursor,
        /// Tables that have not yet been started.
        pending: Vec<TableId>,
        /// Tables that have been fully snapshotted and delivered.
        completed: Vec<TableId>,
    },
    /// The snapshot is complete; the pipeline is in steady-state streaming.
    Streaming {
        /// The last stream position acknowledged by the sink.
        position: Position,
    },
}

/// Progress through one table during the snapshot phase.
///
/// Chandy & Lamport (1985): `snapshot_position` is the global-state marker.
/// It is set once when snapshotting begins and never changed. All chunk reads
/// happen under this anchor. Carbone et al. (2015): the cursor is the barrier
/// state — it must be persisted after each chunk and before delivering the
/// chunk's rows to the sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkCursor {
    /// The table being snapshotted.
    pub table: TableId,
    /// The primary key of the last row delivered in the previous chunk.
    ///
    /// `None` means no chunk has been delivered yet (i.e. this is the first
    /// chunk of the table).
    pub last_pk: Option<crate::table::PrimaryKey>,
    /// The stream position at which snapshotting began.
    ///
    /// This is the Chandy-Lamport marker. It is written to disk before any
    /// snapshot rows are read, satisfying invariant 5 (channel state
    /// consistency). Rows from the stream at positions ≤ this value are
    /// superseded by the snapshot; rows at positions > this value supersede
    /// the snapshot (merge boundary integrity, invariant 6).
    pub snapshot_position: Position,
    /// The number of chunks delivered so far for this table.
    ///
    /// Monotonically increasing; used to detect stale checkpoints on resume.
    pub chunk_index: u64,
    /// The total number of rows delivered from this table so far.
    pub rows_processed: u64,
}

impl ChunkCursor {
    /// Constructs the initial cursor for a table, anchored at `snapshot_position`.
    ///
    /// The caller must have already persisted `snapshot_position` to disk
    /// before calling this constructor. Constructing a cursor without a
    /// persisted anchor violates the Chandy-Lamport invariant.
    pub fn new(table: TableId, snapshot_position: Position) -> Self {
        Self {
            table,
            last_pk: None,
            snapshot_position,
            chunk_index: 0,
            rows_processed: 0,
        }
    }

    /// Returns a new cursor that has advanced past the given chunk.
    ///
    /// `rows_in_chunk` is added to `rows_processed`. `last_pk` is updated
    /// to the last primary key seen in the chunk. `chunk_index` increments
    /// by one. The `snapshot_position` is unchanged — it is the fixed marker.
    pub fn advance(&self, last_pk: crate::table::PrimaryKey, rows_in_chunk: u64) -> Self {
        Self {
            table: self.table.clone(),
            last_pk: Some(last_pk),
            snapshot_position: self.snapshot_position.clone(),
            chunk_index: self.chunk_index.saturating_add(1),
            rows_processed: self.rows_processed.saturating_add(rows_in_chunk),
        }
    }
}

/// The complete resumable state of the pipeline at a given moment.
///
/// Carbone et al. (2015): exactly-once delivery requires that the checkpoint
/// be durable before any events covered by it are delivered to the sink.
/// On crash and resume, the pipeline rewinds to the last committed checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Schema version for the checkpoint format.
    ///
    /// Increment this when the serialised format changes in a backward-
    /// incompatible way so that older checkpoints can be detected and rejected.
    pub version: u32,
    /// The source instance this checkpoint belongs to.
    pub source_id: crate::position::SourceId,
    /// The phase and position at the time this checkpoint was written.
    pub phase: PipelinePhase,
}

impl Checkpoint {
    /// The current checkpoint format version.
    pub const CURRENT_VERSION: u32 = 1;

    /// Constructs a new checkpoint in the `Snapshotting` phase.
    pub fn new_snapshot(
        source_id: crate::position::SourceId,
        cursor: ChunkCursor,
        pending: Vec<TableId>,
    ) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            source_id,
            phase: PipelinePhase::Snapshotting {
                cursor,
                pending,
                completed: Vec::new(),
            },
        }
    }

    /// Constructs a new checkpoint in the `Streaming` phase.
    pub fn new_streaming(source_id: crate::position::SourceId, position: Position) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            source_id,
            phase: PipelinePhase::Streaming { position },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::{Lsn, Position, SourceId};
    use crate::table::{ColumnValue, PrimaryKey, TableId};

    fn table() -> TableId {
        TableId::new("public", "users")
    }

    fn pos(n: u64) -> Position {
        Position::Lsn(Lsn(n))
    }

    #[test]
    fn cursor_advance_increments_chunk_index() {
        let cursor = ChunkCursor::new(table(), pos(100));
        let pk = PrimaryKey::single("id", ColumnValue::Int(42));
        let next = cursor.advance(pk, 500);
        assert_eq!(next.chunk_index, 1);
        assert_eq!(next.rows_processed, 500);
        // snapshot_position must not change — it is the Chandy-Lamport marker.
        assert_eq!(next.snapshot_position, pos(100));
    }

    #[test]
    fn checkpoint_version_is_current() {
        let cursor = ChunkCursor::new(table(), pos(1));
        let cp = Checkpoint::new_snapshot(SourceId("pg-primary".into()), cursor, vec![]);
        assert_eq!(cp.version, Checkpoint::CURRENT_VERSION);
    }
}
