// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! The `Source` trait: the contract every database connector must satisfy.
//!
//! A source provides two capabilities: streaming events in position order,
//! and reading snapshot chunks under a stable transaction. Both are needed
//! to implement the Chandy-Lamport snapshot algorithm (1985).

use crate::checkpoint::ChunkCursor;
use crate::error::JikanError;
use crate::event::{ChangeEvent, RawEvent};
use crate::position::Position;
use crate::table::TableSchema;
use async_trait::async_trait;

/// A stream of raw replication events from a source database.
///
/// The stream yields events in position order. It is the "channel" in the
/// Chandy-Lamport model — it must be opened before the snapshot begins so
/// that in-flight events at snapshot time are captured (invariant 5).
pub type EventStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<RawEvent, JikanError>> + Send>>;

/// A stream of decoded snapshot rows for a single chunk.
pub type SnapshotChunkStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<ChangeEvent, JikanError>> + Send>>;

/// The contract every database source connector must implement.
///
/// Implementors are responsible for:
/// - Maintaining the replication connection and reconnecting on transient failures.
/// - Ensuring that `open_stream` returns events in monotonically increasing
///   position order (invariant 1).
/// - Ensuring that `snapshot_chunk` reads under `REPEATABLE READ` or stronger
///   isolation, as required by Berenson et al. (1995) and Adya et al. (2000).
#[async_trait]
pub trait Source: Send + Sync {
    /// Returns the stable identifier of this source instance.
    fn source_id(&self) -> &crate::position::SourceId;

    /// Opens a stream of replication events starting at `start_position`.
    ///
    /// Chandy & Lamport (1985): this must be called before reading any
    /// snapshot rows. Opening the stream after beginning the snapshot would
    /// lose the in-flight events that occurred between snapshot start and
    /// stream open (invariant 5, channel state consistency).
    ///
    /// The stream yields events at positions strictly greater than
    /// `start_position`. The first event in the stream is the next undelivered
    /// event after the checkpoint.
    async fn open_stream(&self, start_position: Position) -> Result<EventStream, JikanError>;

    /// Decodes a `RawEvent` into a `ChangeEvent`.
    ///
    /// Decoding is separate from streaming so the snapshot engine can buffer
    /// raw events cheaply and decode only those that survive the merge.
    fn decode(&self, raw: RawEvent) -> Result<ChangeEvent, JikanError>;

    /// Returns the current replication position of the source.
    ///
    /// This is used at pipeline startup to determine the `snapshot_position`
    /// when no existing checkpoint is present.
    async fn current_position(&self) -> Result<Position, JikanError>;

    /// Returns the schemas of all tables selected for capture.
    async fn table_schemas(&self) -> Result<Vec<TableSchema>, JikanError>;

    /// Reads one chunk of snapshot rows from `table` starting after `cursor`.
    ///
    /// Berenson et al. (1995): the snapshot transaction must use at minimum
    /// `REPEATABLE READ` isolation to prevent read skew across chunks. Using
    /// `READ COMMITTED` would allow a concurrent `UPDATE` to be invisible in
    /// one chunk and visible in the next, violating invariant 5.
    ///
    /// The chunk contains at most `chunk_size` rows. If the returned stream
    /// yields fewer rows than `chunk_size`, the table is exhausted.
    async fn snapshot_chunk(
        &self,
        cursor: &ChunkCursor,
        chunk_size: u32,
    ) -> Result<SnapshotChunkStream, JikanError>;

    /// Acknowledges that all events up to and including `position` have been
    /// processed.
    ///
    /// For PostgreSQL this sends a standby status update, which advances the
    /// replication slot's confirmed flush LSN and allows WAL segments to be
    /// recycled.
    async fn acknowledge(&self, position: &Position) -> Result<(), JikanError>;
}
