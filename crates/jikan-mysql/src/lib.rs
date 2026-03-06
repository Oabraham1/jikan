// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! MySQL source connector for Jikan.
//!
//! Reads the binary log using GTID-based positioning and takes consistent
//! snapshots using `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
