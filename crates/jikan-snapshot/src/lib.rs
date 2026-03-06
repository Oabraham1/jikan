// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Snapshot engine and stream merger for Jikan.
//!
//! Implements the Chandy-Lamport snapshot algorithm (1985) and enforces the
//! six correctness invariants. The merge boundary is the central construct:
//! positions ≤ snapshot_position belong to the snapshot; positions above it
//! belong to the stream. Violating this boundary is a non-recoverable error.

pub mod buffer;
pub mod engine;
pub mod merger;

pub use engine::{SnapshotConfig, SnapshotEngine};
pub use merger::StreamMerger;
