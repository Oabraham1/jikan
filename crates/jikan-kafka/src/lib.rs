// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Kafka sink for Jikan.
//!
//! Delivers events with exactly-once semantics via Kafka producer transactions,
//! satisfying the 2PC sink participation requirement from Carbone et al. (2015).

mod sink;

pub use sink::{KafkaConfig, KafkaSink};
