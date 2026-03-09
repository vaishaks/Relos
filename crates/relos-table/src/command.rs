use serde::{Deserialize, Serialize};

use crate::schema::{Row, TableSchema, Value};

/// Commands that get serialized into Entry payloads
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TableCommand {
    CreateTable(TableSchema),
    DropTable { table: String },
    Put { table: String, row: Row },
    Delete { table: String, key: Vec<Value> },
}

/// Response from applying a command
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TableResponse {
    Ok,
    Error(String),
}

impl TableCommand {
    pub fn to_bytes(&self) -> relos_core::Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| relos_core::RelosError::Serialization(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> relos_core::Result<Self> {
        bincode::deserialize(bytes)
            .map_err(|e| relos_core::RelosError::Serialization(e.to_string()))
    }
}

impl TableResponse {
    pub fn to_bytes(&self) -> relos_core::Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| relos_core::RelosError::Serialization(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> relos_core::Result<Self> {
        bincode::deserialize(bytes)
            .map_err(|e| relos_core::RelosError::Serialization(e.to_string()))
    }
}
