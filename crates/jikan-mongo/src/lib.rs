// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! MongoDB source connector for Jikan.
//!
//! Consumes change streams and takes point-in-time snapshots using
//! `atClusterTime` to anchor the Chandy-Lamport marker.
