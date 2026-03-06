// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Kafka sink for Jikan.
//!
//! Delivers events with exactly-once semantics via Kafka transactions
//! (two-phase commit), satisfying the sink participation requirement
//! from Carbone et al. (2015).
