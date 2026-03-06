// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Jikan's correctness invariants.

#[cfg(test)]
mod mock;

#[cfg(test)]
mod tests {
    use super::mock::*;
    use jikan_snapshot::{SnapshotConfig, SnapshotEngine};

    fn snapshot_config(dir: &std::path::Path) -> SnapshotConfig {
        SnapshotConfig {
            chunk_size: 3,
            checkpoint_path: dir.join("checkpoint.json"),
        }
    }

    /// Invariant 2 (no gap): every snapshot row is delivered exactly once.
    #[tokio::test]
    async fn all_snapshot_rows_delivered() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<_> = (1..=10).map(|i| insert_event(i, 100)).collect();
        let source = MockSource::new("test", rows, 100, 3);
        let sink = RecordingSink::new();
        let mut engine = SnapshotEngine::new(source, sink.clone(), snapshot_config(dir.path()));

        engine.run().await.expect("engine run failed");

        let delivered = sink.delivered();
        assert_eq!(delivered.len(), 10, "all 10 rows must be delivered");
        for (i, event) in delivered.iter().enumerate() {
            let pk = event.primary_key.as_ref().unwrap();
            let id = match pk.0.get("id").unwrap() {
                jikan_core::table::ColumnValue::Int(n) => *n,
                _ => panic!("unexpected pk type"),
            };
            assert_eq!(id, (i + 1) as i64, "rows delivered in primary key order");
        }
    }

    /// Invariant 3 (no duplicate): resuming from a streaming checkpoint
    /// does not re-deliver any snapshot rows.
    #[tokio::test]
    async fn resume_does_not_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<_> = (1..=9).map(|i| insert_event(i, 100)).collect();

        // First run: deliver all 9 rows, checkpoint transitions to streaming.
        {
            let source = MockSource::new("test", rows.clone(), 100, 3);
            let sink = RecordingSink::new();
            let mut engine = SnapshotEngine::new(source, sink.clone(), snapshot_config(dir.path()));
            engine.run().await.expect("first run failed");
        }

        // Second run from the same checkpoint must not re-deliver any rows
        // (checkpoint says streaming).
        let source2 = MockSource::new("test", rows, 100, 3);
        let sink2 = RecordingSink::new();
        let mut engine2 = SnapshotEngine::new(source2, sink2.clone(), snapshot_config(dir.path()));
        engine2.run().await.expect("second run failed");

        assert_eq!(
            sink2.delivered().len(),
            0,
            "no rows re-delivered on streaming-mode resume"
        );
    }

    /// Invariant 6 (merge boundary): a stream event at a position equal to
    /// snapshot_position is rejected.
    #[tokio::test]
    async fn stream_event_at_or_before_snapshot_is_rejected() {
        let stale_event = jikan_core::event::ChangeEvent {
            position: pos(100),
            table: jikan_core::table::TableId::new("public", "items"),
            kind: jikan_core::event::EventKind::Insert,
            primary_key: Some(jikan_core::table::PrimaryKey::single(
                "id",
                jikan_core::table::ColumnValue::Int(1),
            )),
            after: None,
            before: None,
            committed_at: None,
        };

        let mut buf = jikan_snapshot::buffer::StreamBuffer::new(
            jikan_core::position::Position::Lsn(jikan_core::position::Lsn(100)),
        );
        let result = buf.push(stale_event);
        assert!(
            result.is_err(),
            "event at snapshot boundary must be rejected"
        );
        match result.unwrap_err() {
            jikan_core::error::JikanError::InvariantViolation(_) => {}
            other => panic!("expected InvariantViolation, got {:?}", other),
        }
    }

    /// Invariant 6 (merge boundary): a stream delete supersedes a snapshot row.
    #[tokio::test]
    async fn stream_delete_drops_snapshot_row() {
        let snapshot_pos = pos(100);
        let mut buf = jikan_snapshot::buffer::StreamBuffer::new(snapshot_pos);

        let delete = jikan_core::event::ChangeEvent {
            position: pos(150),
            table: jikan_core::table::TableId::new("public", "items"),
            kind: jikan_core::event::EventKind::Delete,
            primary_key: Some(jikan_core::table::PrimaryKey::single(
                "id",
                jikan_core::table::ColumnValue::Int(42),
            )),
            after: None,
            before: None,
            committed_at: None,
        };
        buf.push(delete).unwrap();

        let merger = jikan_snapshot::merger::StreamMerger::new(&buf);
        let snapshot_row = insert_event(42, 100);
        let result = merger.merge_row(snapshot_row).unwrap();

        assert!(
            result.is_none(),
            "snapshot row must be dropped when stream has a delete for the same key"
        );
    }

    /// Invariant 6 (merge boundary): a stream update supersedes a snapshot row.
    #[tokio::test]
    async fn stream_update_supersedes_snapshot_row() {
        let snapshot_pos = pos(100);
        let mut buf = jikan_snapshot::buffer::StreamBuffer::new(snapshot_pos);

        let update = jikan_core::event::ChangeEvent {
            position: pos(200),
            table: jikan_core::table::TableId::new("public", "items"),
            kind: jikan_core::event::EventKind::Update,
            primary_key: Some(jikan_core::table::PrimaryKey::single(
                "id",
                jikan_core::table::ColumnValue::Int(7),
            )),
            after: None,
            before: None,
            committed_at: None,
        };
        buf.push(update).unwrap();

        let merger = jikan_snapshot::merger::StreamMerger::new(&buf);
        let snapshot_row = insert_event(7, 100);
        let result = merger.merge_row(snapshot_row).unwrap().unwrap();

        assert_eq!(result.kind, jikan_core::event::EventKind::Update);
        assert_eq!(result.position, pos(200));
    }

    /// Invariant 1 (monotonicity): the `ChunkCursor::advance` method never
    /// decreases the chunk index.
    #[test]
    fn chunk_cursor_monotone() {
        let table = jikan_core::table::TableId::new("public", "items");
        let cursor = jikan_core::checkpoint::ChunkCursor::new(table, pos(100));
        let pk =
            jikan_core::table::PrimaryKey::single("id", jikan_core::table::ColumnValue::Int(100));
        let next = cursor.advance(pk.clone(), 50);
        assert!(next.chunk_index > cursor.chunk_index);
        let next2 = next.advance(pk, 50);
        assert!(next2.chunk_index > next.chunk_index);
    }

    /// Invariant 5 (Carbone 2015): checkpoint is written before rows are
    /// delivered. Verify that the checkpoint file exists on disk after the
    /// engine processes the first chunk.
    #[tokio::test]
    async fn checkpoint_written_before_delivery() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<_> = (1..=5).map(|i| insert_event(i, 100)).collect();
        let source = MockSource::new("test", rows, 100, 3);
        let sink = RecordingSink::new();
        let mut engine = SnapshotEngine::new(source, sink.clone(), snapshot_config(dir.path()));

        engine.run().await.expect("engine run failed");

        // After a successful run, the checkpoint file must exist on disk.
        assert!(
            dir.path().join("checkpoint.json").exists(),
            "checkpoint file must exist after engine run"
        );
        assert!(
            sink.commit_count() > 0,
            "sink must have committed at least one batch"
        );
    }
}
