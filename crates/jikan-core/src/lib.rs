// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Core types, traits, and error definitions shared across all Jikan crates.
//!
//! This crate contains no I/O. It is the vocabulary every other crate speaks.
//! The type system here enforces the invariants from the theoretical foundation:
//! [`Position`] deliberately omits `PartialOrd` so cross-source comparison is
//! a compile-time error (Lamport 1978).
