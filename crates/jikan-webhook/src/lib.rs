// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! HTTP webhook sink for Jikan.
//!
//! Delivers events with at-least-once semantics. Receivers must implement
//! idempotency if exactly-once processing is required.

mod sink;

pub use sink::{WebhookConfig, WebhookSink};
