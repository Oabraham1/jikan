// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Table and schema representations used across source connectors.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A fully-qualified table identifier: schema (or database) plus table name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId {
    /// The schema name (PostgreSQL/MySQL) or database name (MongoDB).
    pub schema: String,
    /// The table or collection name.
    pub name: String,
}

impl TableId {
    /// Constructs a new `TableId`.
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

/// A column value decoded from a replication event or snapshot row.
///
/// The variants cover the types Jikan needs to represent across all three
/// source databases. More specific type information (e.g. PostgreSQL OIDs)
/// lives in connector-specific types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnValue {
    /// A null value for any column type.
    Null,
    /// A boolean.
    Bool(bool),
    /// A 64-bit signed integer, covering all integer column types.
    Int(i64),
    /// A 64-bit float.
    Float(f64),
    /// A UTF-8 string.
    Text(String),
    /// Raw bytes, used for binary columns and JSONB.
    Bytes(Vec<u8>),
}

/// The primary key of a row, as an ordered map from column name to value.
///
/// `BTreeMap` preserves column order, which matters for deterministic
/// serialisation and merge comparisons.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrimaryKey(pub BTreeMap<String, ColumnValue>);

impl PrimaryKey {
    /// Constructs a single-column primary key.
    pub fn single(column: impl Into<String>, value: ColumnValue) -> Self {
        let mut m = BTreeMap::new();
        m.insert(column.into(), value);
        Self(m)
    }
}

/// The schema of a table at a given point in time.
///
/// Captured at snapshot time so that DDL events during streaming can be
/// detected and the pipeline can react (or halt, depending on configuration).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// The table this schema describes.
    pub table: TableId,
    /// Ordered list of column names.
    pub columns: Vec<String>,
    /// Names of the columns that form the primary key, in key order.
    pub primary_key_columns: Vec<String>,
}
