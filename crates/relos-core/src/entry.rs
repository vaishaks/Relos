use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Entry {
    /// Engine headers: engine_name -> serialized header bytes
    pub headers: HashMap<String, Vec<u8>>,
    /// Application payload
    pub payload: Vec<u8>,
}

impl Entry {
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            headers: HashMap::new(),
            payload,
        }
    }

    pub fn with_header(mut self, engine: impl Into<String>, header: Vec<u8>) -> Self {
        self.headers.insert(engine.into(), header);
        self
    }

    pub fn get_header(&self, engine: &str) -> Option<&[u8]> {
        self.headers.get(engine).map(|v| v.as_slice())
    }

    /// Serialize this entry to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::RelosError> {
        bincode::serialize(self).map_err(|e| crate::RelosError::Serialization(e.to_string()))
    }

    /// Deserialize an entry from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::RelosError> {
        bincode::deserialize(bytes).map_err(|e| crate::RelosError::Serialization(e.to_string()))
    }
}
