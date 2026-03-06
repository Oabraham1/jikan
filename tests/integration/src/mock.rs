// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! In-memory mock source and sink for integration tests.
//!
//! The mock source emits a fixed set of events in order. The mock sink
//! records every event it receives so tests can assert on the delivered
//! sequence.

use async_trait::async_trait;
use jikan_core::{
    checkpoint::ChunkCursor,
    error::JikanError,
    event::{ChangeEvent, EventKind, RawEvent},
    position::{Lsn, Position, SourceId},
    sink::{DeliveryGuarantee, Sink},
    source::{EventStream, SnapshotChunkStream, Source},
    table::{ColumnValue, PrimaryKey, TableId, TableSchema},
};
use std::sync::{Arc, Mutex};

/// Constructs a `Position::Lsn` from a raw u64.
pub fn pos(n: u64) -> Position {
    Position::Lsn(Lsn(n))
}

/// Constructs an insert event with the given primary key and LSN.
pub fn insert_event(pk: i64, lsn: u64) -> ChangeEvent {
    ChangeEvent {
        position: pos(lsn),
        table: TableId::new("public", "items"),
        kind: EventKind::Insert,
        primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(pk))),
        after: None,
        before: None,
        committed_at: None,
    }
}

/// A mock source backed by pre-loaded snapshot rows and stream events.
pub struct MockSource {
    source_id: SourceId,
    snapshot_rows: Vec<ChangeEvent>,
    current_lsn: u64,
    chunk_size_for_test: u32,
}

impl MockSource {
    /// Creates a new mock source with the given snapshot rows.
    pub fn new(
        source_id: &str,
        snapshot_rows: Vec<ChangeEvent>,
        snapshot_lsn: u64,
        chunk_size_for_test: u32,
    ) -> Self {
        Self {
            source_id: SourceId(source_id.into()),
            snapshot_rows,
            current_lsn: snapshot_lsn,
            chunk_size_for_test,
        }
    }
}

#[async_trait]
impl Source for MockSource {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    async fn open_stream(&self, _start_position: Position) -> Result<EventStream, JikanError> {
        let stream = futures::stream::empty::<Result<RawEvent, JikanError>>();
        Ok(Box::pin(stream))
    }

    fn decode(&self, _raw: RawEvent) -> Result<ChangeEvent, JikanError> {
        Err(JikanError::ReplicationProtocol(
            "mock source does not decode raw events".into(),
        ))
    }

    async fn current_position(&self) -> Result<Position, JikanError> {
        Ok(pos(self.current_lsn))
    }

    async fn table_schemas(&self) -> Result<Vec<TableSchema>, JikanError> {
        Ok(vec![TableSchema {
            table: TableId::new("public", "items"),
            columns: vec!["id".into()],
            primary_key_columns: vec!["id".into()],
        }])
    }

    async fn snapshot_chunk(
        &self,
        cursor: &ChunkCursor,
        chunk_size: u32,
    ) -> Result<SnapshotChunkStream, JikanError> {
        let start_pk = cursor
            .last_pk
            .as_ref()
            .and_then(|pk| {
                pk.0.get("id").and_then(|v| match v {
                    ColumnValue::Int(n) => Some(*n),
                    _ => None,
                })
            })
            .unwrap_or(i64::MIN);

        let limit = chunk_size.min(self.chunk_size_for_test) as usize;

        let rows: Vec<Result<ChangeEvent, JikanError>> = self
            .snapshot_rows
            .iter()
            .filter(|e| {
                e.primary_key
                    .as_ref()
                    .and_then(|pk| pk.0.get("id"))
                    .map(|v| match v {
                        ColumnValue::Int(n) => *n > start_pk,
                        _ => false,
                    })
                    .unwrap_or(false)
            })
            .take(limit)
            .map(|e| Ok(e.clone()))
            .collect();

        Ok(Box::pin(futures::stream::iter(rows)))
    }

    async fn acknowledge(&self, _position: &Position) -> Result<(), JikanError> {
        Ok(())
    }
}

/// A mock sink that records every event it receives.
#[derive(Clone)]
pub struct RecordingSink {
    inner: Arc<Mutex<RecordingSinkInner>>,
}

struct RecordingSinkInner {
    delivered: Vec<ChangeEvent>,
    commit_count: usize,
}

impl RecordingSink {
    /// Creates an empty recording sink.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RecordingSinkInner {
                delivered: Vec::new(),
                commit_count: 0,
            })),
        }
    }

    /// Returns a clone of all delivered events.
    pub fn delivered(&self) -> Vec<ChangeEvent> {
        self.inner.lock().unwrap().delivered.clone()
    }

    /// Returns the number of commits performed.
    pub fn commit_count(&self) -> usize {
        self.inner.lock().unwrap().commit_count
    }
}

#[async_trait]
impl Sink for RecordingSink {
    fn delivery_guarantee(&self) -> DeliveryGuarantee {
        DeliveryGuarantee::AtLeastOnce
    }

    async fn begin(&mut self) -> Result<(), JikanError> {
        Ok(())
    }

    async fn send(&mut self, event: ChangeEvent) -> Result<(), JikanError> {
        self.inner.lock().unwrap().delivered.push(event);
        Ok(())
    }

    async fn commit(&mut self, _up_to_position: &Position) -> Result<(), JikanError> {
        self.inner.lock().unwrap().commit_count += 1;
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), JikanError> {
        Ok(())
    }
}
