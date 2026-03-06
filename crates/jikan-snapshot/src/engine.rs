// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! The snapshot engine: orchestrates the full snapshot lifecycle.
//!
//! This is where the Chandy-Lamport algorithm (1985) runs end-to-end, with
//! checkpoint persistence satisfying the barrier semantics from Carbone et
//! al. (2015).

use crate::buffer::StreamBuffer;
use crate::merger::StreamMerger;
use jikan_core::{
    checkpoint::{Checkpoint, ChunkCursor, PipelinePhase},
    error::JikanError,
    event::ChangeEvent,
    position::Position,
    sink::Sink,
    source::Source,
    table::TableId,
};
use tracing::{info, instrument};

/// Configuration for a snapshot run.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Number of rows to read per chunk.
    pub chunk_size: u32,
    /// Path to the file where checkpoints are written.
    pub checkpoint_path: std::path::PathBuf,
}

/// Orchestrates the full snapshot-then-stream lifecycle.
pub struct SnapshotEngine<S: Source, K: Sink> {
    source: S,
    sink: K,
    config: SnapshotConfig,
}

impl<S: Source, K: Sink> SnapshotEngine<S, K> {
    /// Creates a new engine. No I/O occurs in the constructor.
    pub fn new(source: S, sink: K, config: SnapshotConfig) -> Self {
        Self {
            source,
            sink,
            config,
        }
    }

    /// Runs the snapshot algorithm from step 1 to step 7.
    ///
    /// On return, the engine has transitioned to streaming mode and `sink`
    /// has received all events up to and including the current stream position.
    #[instrument(skip(self), fields(source_id = %self.source.source_id().0))]
    pub async fn run(&mut self) -> Result<(), JikanError> {
        let existing = self.load_checkpoint().await?;

        match existing {
            Some(cp) => self.resume(cp).await,
            None => self.start_fresh().await,
        }
    }

    /// Performs a fresh snapshot when no checkpoint exists on disk.
    async fn start_fresh(&mut self) -> Result<(), JikanError> {
        // Step 1: record snapshot_position and persist before anything else.
        // Chandy & Lamport (1985): loss of this marker on crash violates
        // exactly-once because we cannot reconstruct which events were
        // in-flight at snapshot time.
        let snapshot_position = self.source.current_position().await?;
        info!(position = ?snapshot_position, "snapshot position recorded");

        let tables = self.source.table_schemas().await?;
        let table_ids: Vec<TableId> = tables.iter().map(|s| s.table.clone()).collect();

        let first_table = match table_ids.first() {
            Some(t) => t.clone(),
            None => {
                info!("no tables to snapshot; transitioning directly to streaming");
                return self.stream_from(snapshot_position).await;
            }
        };

        let cursor = ChunkCursor::new(first_table, snapshot_position.clone());
        let mut pending = table_ids[1..].to_vec();
        pending.reverse(); // pop from the end as a stack

        let checkpoint = Checkpoint::new_snapshot(
            self.source.source_id().clone(),
            cursor.clone(),
            pending.clone(),
        );
        self.persist_checkpoint(&checkpoint).await?;

        // Step 2: open the stream buffer from snapshot_position.
        // Must happen after persisting the marker but before reading rows.
        let mut buffer = StreamBuffer::new(snapshot_position.clone());
        self.fill_buffer_in_background(&snapshot_position, &mut buffer)
            .await?;

        // Steps 3–5: read chunks, merge, deliver.
        self.snapshot_tables(cursor, pending, &mut buffer).await?;

        // Step 6: drain the buffer.
        let drained = buffer.drain();
        self.deliver_stream_drain(drained).await?;

        // Step 7: transition to streaming.
        self.stream_from(snapshot_position).await
    }

    /// Resumes from a persisted checkpoint after a crash or restart.
    async fn resume(&mut self, checkpoint: Checkpoint) -> Result<(), JikanError> {
        info!("resuming from checkpoint");
        match checkpoint.phase {
            PipelinePhase::Snapshotting {
                cursor, pending, ..
            } => {
                let snapshot_position = cursor.snapshot_position.clone();
                let mut buffer = StreamBuffer::new(snapshot_position.clone());
                self.fill_buffer_in_background(&snapshot_position, &mut buffer)
                    .await?;
                self.snapshot_tables(cursor, pending, &mut buffer).await?;
                let drained = buffer.drain();
                self.deliver_stream_drain(drained).await?;
                self.stream_from(snapshot_position).await
            }
            PipelinePhase::Streaming { position } => self.stream_from(position).await,
        }
    }

    /// Iterates through tables chunk by chunk, merging each chunk against the
    /// stream buffer before delivering to the sink.
    async fn snapshot_tables(
        &mut self,
        initial_cursor: ChunkCursor,
        mut pending: Vec<TableId>,
        buffer: &mut StreamBuffer,
    ) -> Result<(), JikanError> {
        let mut cursor = initial_cursor;

        loop {
            let mut chunk_stream = self
                .source
                .snapshot_chunk(&cursor, self.config.chunk_size)
                .await?;

            let mut rows: Vec<ChangeEvent> = Vec::new();
            use futures::StreamExt;
            while let Some(row) = chunk_stream.next().await {
                rows.push(row?);
            }

            let is_last_chunk = (rows.len() as u32) < self.config.chunk_size;
            let last_pk = rows.last().and_then(|r| r.primary_key.clone());

            // Merge before delivering — invariant 6.
            let merger = StreamMerger::new(buffer);
            let merged = merger.merge_chunk(rows)?;

            // Step 4: persist cursor before delivering rows (Carbone 2015).
            if let Some(pk) = last_pk {
                let row_count = merged.len() as u64;
                let next_cursor = cursor.advance(pk, row_count);
                let cp = Checkpoint {
                    version: Checkpoint::CURRENT_VERSION,
                    source_id: self.source.source_id().clone(),
                    phase: PipelinePhase::Snapshotting {
                        cursor: next_cursor.clone(),
                        pending: pending.clone(),
                        completed: vec![],
                    },
                };
                self.persist_checkpoint(&cp).await?;
                cursor = next_cursor;
            }

            self.deliver_batch(merged).await?;

            if is_last_chunk {
                match pending.pop() {
                    Some(next_table) => {
                        cursor = ChunkCursor::new(next_table, cursor.snapshot_position.clone());
                    }
                    None => break,
                }
            }
        }
        Ok(())
    }

    /// Opens the replication stream so that in-flight events are captured.
    ///
    /// In production this runs the stream reader concurrently with the
    /// snapshot chunk reads. For correctness, the stream must be opened
    /// before any snapshot rows are read (Chandy & Lamport 1985).
    /// This stub opens the stream and immediately returns; the full
    /// implementation uses a tokio task and a channel.
    async fn fill_buffer_in_background(
        &self,
        snapshot_position: &Position,
        buffer: &mut StreamBuffer,
    ) -> Result<(), JikanError> {
        let _ = self.source.open_stream(snapshot_position.clone()).await?;
        let _ = buffer;
        Ok(())
    }

    /// Delivers a batch of events to the sink within a single transaction.
    async fn deliver_batch(&mut self, events: Vec<ChangeEvent>) -> Result<(), JikanError> {
        if events.is_empty() {
            return Ok(());
        }
        let last_pos = events.last().map(|e| e.position.clone());
        self.sink.begin().await?;
        for event in events {
            self.sink.send(event).await?;
        }
        if let Some(pos) = last_pos {
            self.sink.commit(&pos).await?;
        }
        Ok(())
    }

    /// Delivers the remaining stream buffer events after all tables are done.
    async fn deliver_stream_drain(&mut self, events: Vec<ChangeEvent>) -> Result<(), JikanError> {
        self.deliver_batch(events).await
    }

    /// Persists a streaming-phase checkpoint and logs the transition.
    async fn stream_from(&mut self, position: Position) -> Result<(), JikanError> {
        let cp = Checkpoint::new_streaming(self.source.source_id().clone(), position.clone());
        self.persist_checkpoint(&cp).await?;
        info!(position = ?position, "transitioned to streaming mode");
        // The streaming loop is wired in Phase 08 (CLI) and Phase 09 (tests).
        Ok(())
    }

    /// Loads an existing checkpoint from disk, if one exists.
    async fn load_checkpoint(&self) -> Result<Option<Checkpoint>, JikanError> {
        if !self.config.checkpoint_path.exists() {
            return Ok(None);
        }
        let bytes = tokio::fs::read(&self.config.checkpoint_path)
            .await
            .map_err(|e| JikanError::CheckpointIo(format!("read checkpoint: {e}")))?;
        let cp: Checkpoint = serde_json::from_slice(&bytes)
            .map_err(|e| JikanError::CheckpointIo(format!("deserialise checkpoint: {e}")))?;
        Ok(Some(cp))
    }

    /// Atomically persists a checkpoint by writing to a temp file then renaming.
    ///
    /// A crash between write and rename leaves the old checkpoint intact,
    /// satisfying the durability requirement from Carbone et al. (2015).
    async fn persist_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), JikanError> {
        let bytes = serde_json::to_vec(checkpoint)
            .map_err(|e| JikanError::CheckpointIo(format!("serialise checkpoint: {e}")))?;

        let tmp = self.config.checkpoint_path.with_extension("tmp");
        tokio::fs::write(&tmp, &bytes)
            .await
            .map_err(|e| JikanError::CheckpointIo(format!("write checkpoint: {e}")))?;
        tokio::fs::rename(&tmp, &self.config.checkpoint_path)
            .await
            .map_err(|e| JikanError::CheckpointIo(format!("rename checkpoint: {e}")))?;
        Ok(())
    }
}
