// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Core types, traits, and error definitions shared across all Jikan crates.

pub mod checkpoint;
pub mod error;
pub mod event;
pub mod position;
pub mod sink;
pub mod source;
pub mod table;
pub mod watchdog;

pub use checkpoint::{Checkpoint, ChunkCursor, PipelinePhase};
pub use error::JikanError;
pub use event::{ChangeEvent, EventKind, RawEvent};
pub use position::{GtidSet, Lsn, OplogTimestamp, Position, SourceId};
pub use sink::Sink;
pub use source::Source;
pub use table::{ColumnValue, PrimaryKey, TableId, TableSchema};
pub use watchdog::PipelineWatchdog;
