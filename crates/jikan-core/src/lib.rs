// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Core types, traits, and error definitions shared across all Jikan crates.
//!
//! This crate contains no I/O. It is the vocabulary every other crate speaks.

pub mod checkpoint;
pub mod error;
pub mod event;
pub mod position;
pub mod table;

pub use checkpoint::{Checkpoint, ChunkCursor, PipelinePhase};
pub use error::JikanError;
pub use event::{ChangeEvent, EventKind, RawEvent};
pub use position::{GtidSet, Lsn, OplogTimestamp, Position, SourceId};
pub use table::{ColumnValue, PrimaryKey, TableId, TableSchema};
