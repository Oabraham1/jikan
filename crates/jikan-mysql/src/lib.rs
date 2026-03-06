// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! MySQL source connector for Jikan.
//!
//! Reads the binary log using GTID-based positioning and takes consistent
//! snapshots using `START TRANSACTION WITH CONSISTENT SNAPSHOT` under
//! REPEATABLE READ isolation (Berenson et al. 1995).

mod binlog;
mod codec;
mod connector;
mod snapshot;

pub use binlog::make_raw_event;
pub use connector::{MySqlConfig, MySqlSource};
