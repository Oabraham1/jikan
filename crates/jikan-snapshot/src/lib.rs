// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Snapshot engine and stream merger for Jikan.
//!
//! Implements the Chandy-Lamport snapshot algorithm (1985) and the
//! merge boundary invariant: positions ≤ snapshot_lsn are owned by
//! the snapshot; positions > snapshot_lsn belong to the stream.
//! Violation of this boundary is detected and treated as a fatal error.
