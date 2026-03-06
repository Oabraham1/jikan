// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL source connector for Jikan.
//!
//! Decodes the pgoutput logical replication protocol and implements the
//! Chandy-Lamport snapshot algorithm (1985) under REPEATABLE READ isolation
//! (Berenson et al. 1995).

mod codec;
mod connector;
mod slot;
mod snapshot;

pub use connector::PostgresConfig;
pub use connector::PostgresSource;
