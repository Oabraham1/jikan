// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL source connector for Jikan.
//!
//! Decodes the pgoutput logical replication protocol and implements the
//! snapshot algorithm described in Chandy & Lamport (1985) using
//! `REPEATABLE READ` snapshot isolation (Berenson et al. 1995).
