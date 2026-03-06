// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for the snapshot engine and stream merger.
//!
//! Run with: `cargo bench -p jikan-snapshot`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use jikan_core::{
    checkpoint::{Checkpoint, ChunkCursor, PipelinePhase},
    event::{ChangeEvent, EventKind},
    position::{Lsn, Position, SourceId},
    table::{ColumnValue, PrimaryKey, TableId},
};
use jikan_snapshot::buffer::StreamBuffer;
use jikan_snapshot::merger::StreamMerger;

fn pos(n: u64) -> Position {
    Position::Lsn(Lsn(n))
}

fn insert_event(pk: i64, lsn: u64) -> ChangeEvent {
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

/// Measures the cost of pushing one event into the stream buffer.
///
/// The buffer's `push` is on the hot path during snapshotting: every stream
/// event received while reading snapshot chunks goes through here.
fn bench_buffer_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_push");

    for buffer_size in [0u64, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(buffer_size),
            &buffer_size,
            |b, &n| {
                b.iter(|| {
                    let mut buf = StreamBuffer::new(pos(100));
                    for i in 0..n {
                        buf.push(insert_event(i as i64, 101 + i)).unwrap();
                    }
                    // Push one more event to measure marginal cost at this buffer size.
                    buf.push(black_box(insert_event(n as i64, 101 + n)))
                        .unwrap();
                    black_box(buf.len())
                });
            },
        );
    }
    group.finish();
}

/// Measures throughput of `StreamMerger::merge_chunk` with no superseding events.
///
/// The common case during snapshotting: the stream buffer is empty or the
/// chunk's keys do not overlap with any buffered stream events.
fn bench_merge_chunk_no_supersede(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_chunk_no_supersede");

    for chunk_size in [100u32, 1_000, 10_000] {
        let rows: Vec<ChangeEvent> = (0..chunk_size)
            .map(|i| insert_event(i as i64, 100))
            .collect();

        group.throughput(Throughput::Elements(u64::from(chunk_size)));
        group.bench_with_input(BenchmarkId::from_parameter(chunk_size), &rows, |b, rows| {
            b.iter(|| {
                let buf = StreamBuffer::new(pos(100));
                let merger = StreamMerger::new(&buf);
                let result = merger.merge_chunk(black_box(rows.clone())).unwrap();
                black_box(result.len())
            });
        });
    }
    group.finish();
}

/// Measures throughput of `StreamMerger::merge_chunk` when every row in the
/// chunk is superseded by a stream update.
///
/// This is the worst case for the merge: every snapshot row requires a buffer
/// lookup and the snapshot row is replaced.
fn bench_merge_chunk_all_superseded(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_chunk_all_superseded");

    for chunk_size in [100u32, 1_000] {
        let rows: Vec<ChangeEvent> = (0..chunk_size)
            .map(|i| insert_event(i as i64, 100))
            .collect();

        group.throughput(Throughput::Elements(u64::from(chunk_size)));
        group.bench_with_input(BenchmarkId::from_parameter(chunk_size), &rows, |b, rows| {
            b.iter(|| {
                let mut buf = StreamBuffer::new(pos(100));
                for i in 0..chunk_size {
                    buf.push(insert_event(i as i64, 200)).unwrap();
                }
                let merger = StreamMerger::new(&buf);
                let result = merger.merge_chunk(black_box(rows.clone())).unwrap();
                black_box(result.len())
            });
        });
    }
    group.finish();
}

/// Measures the cost of checkpoint serialisation and write.
fn bench_checkpoint_write(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint.json");

    let cursor = ChunkCursor::new(TableId::new("public", "items"), pos(100));
    let checkpoint = Checkpoint {
        version: Checkpoint::CURRENT_VERSION,
        source_id: SourceId("bench-source".into()),
        phase: PipelinePhase::Snapshotting {
            cursor,
            pending: (0..10)
                .map(|i| TableId::new("public", format!("table_{i}")))
                .collect(),
            completed: vec![],
        },
    };

    c.bench_function("checkpoint_write", |b| {
        b.iter(|| {
            let bytes = serde_json::to_vec(black_box(&checkpoint)).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            black_box(bytes.len())
        });
    });
}

criterion_group!(
    benches,
    bench_buffer_push,
    bench_merge_chunk_no_supersede,
    bench_merge_chunk_all_superseded,
    bench_checkpoint_write,
);
criterion_main!(benches);
