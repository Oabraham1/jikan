// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! HTTP webhook sink for Jikan.
//!
//! Delivers events with at-least-once semantics. Callers that require
//! exactly-once must implement idempotency on the receiving end.
