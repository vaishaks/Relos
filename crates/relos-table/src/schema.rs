use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ColumnType {
    Int64,
    Utf8,
    Bytes,
    Bool,
    Float64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<String>, // column names that form the primary key
}

/// A value in a row
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Int64(i64),
    Utf8(String),
    Bytes(Vec<u8>),
    Bool(bool),
    Float64(f64),
}

/// A row is an ordered list of values (matching column order in schema)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}
