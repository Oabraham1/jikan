// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for the pgoutput decoder.
//!
//! Run with: `cargo bench -p jikan-postgres --features bench`

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use jikan_core::{
    event::RawEvent,
    position::{Lsn, Position},
};
use jikan_postgres::codec::RelationCache;
use jikan_postgres::codec_bench::make_insert_payload;

/// Measures the time to decode one pgoutput INSERT event.
fn bench_decode_insert(c: &mut Criterion) {
    let (relation_payload, insert_payload) = make_insert_payload();
    let cache = RelationCache::new();

    // Populate the relation cache before benchmarking inserts.
    let rel_raw = RawEvent {
        position: Position::Lsn(Lsn(0)),
        payload: relation_payload,
    };
    jikan_postgres::codec::decode_pgoutput(rel_raw, &cache).unwrap();

    let mut group = c.benchmark_group("pgoutput_decode");
    group.throughput(Throughput::Elements(1));

    group.bench_function("insert", |b| {
        b.iter(|| {
            let raw = RawEvent {
                position: Position::Lsn(Lsn(12345)),
                payload: black_box(insert_payload.clone()),
            };
            let result = jikan_postgres::codec::decode_pgoutput(raw, &cache).unwrap();
            black_box(result)
        });
    });
    group.finish();
}

criterion_group!(benches, bench_decode_insert);
criterion_main!(benches);
