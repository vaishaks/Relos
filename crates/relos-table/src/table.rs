use std::sync::Arc;

use relos_core::{Entry, RelosError, Result};
use relos_engine::{Engine, ReturnValue};
use relos_store::LocalStore;

use crate::command::{TableCommand, TableResponse};
use crate::schema::{Row, TableSchema, Value};

/// DelosTable — a replicated table store built on top of the Delos engine stack.
///
/// Write operations (create_table, drop_table, put, delete) are proposed to the
/// shared log via the engine and applied by the `TableApplicator`.
///
/// Read operations (get, scan) sync the engine first to ensure we have caught up
/// with the log, then read directly from the local store.
pub struct DelosTable {
    engine: Arc<dyn Engine>,
    store: Arc<dyn LocalStore>,
}

impl DelosTable {
    pub fn new(engine: Arc<dyn Engine>, store: Arc<dyn LocalStore>) -> Self {
        Self { engine, store }
    }

    /// Create a new table.
    pub async fn create_table(&self, schema: TableSchema) -> Result<()> {
        let cmd = TableCommand::CreateTable(schema);
        self.propose_and_check(cmd).await
    }

    /// Drop a table.
    pub async fn drop_table(&self, table: &str) -> Result<()> {
        let cmd = TableCommand::DropTable {
            table: table.to_string(),
        };
        self.propose_and_check(cmd).await
    }

    /// Put a row (upsert).
    pub async fn put(&self, table: &str, row: Row) -> Result<()> {
        let cmd = TableCommand::Put {
            table: table.to_string(),
            row,
        };
        self.propose_and_check(cmd).await
    }

    /// Delete a row by primary key.
    pub async fn delete(&self, table: &str, key: Vec<Value>) -> Result<()> {
        let cmd = TableCommand::Delete {
            table: table.to_string(),
            key,
        };
        self.propose_and_check(cmd).await
    }

    /// Get a row by primary key (read-only — uses sync + local read).
    pub async fn get(&self, table: &str, key: &[Value]) -> Result<Option<Row>> {
        // Sync to ensure we have the latest state
        self.engine.sync().await?;

        let txn = self.store.begin_read_txn()?;
        let row_key = Self::row_key(table, key);
        match txn.get(&row_key)? {
            Some(bytes) => {
                let row: Row = bincode::deserialize(&bytes)
                    .map_err(|e| RelosError::Serialization(e.to_string()))?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    /// Scan all rows in a table (read-only).
    pub async fn scan(&self, table: &str) -> Result<Vec<Row>> {
        // Sync to ensure we have the latest state
        self.engine.sync().await?;

        let txn = self.store.begin_read_txn()?;
        let prefix = format!("data/{}/", table).into_bytes();
        let entries = txn.prefix_scan(&prefix)?;

        let mut rows = Vec::with_capacity(entries.len());
        for (_key, value) in entries {
            let row: Row = bincode::deserialize(&value)
                .map_err(|e| RelosError::Serialization(e.to_string()))?;
            rows.push(row);
        }
        Ok(rows)
    }

    // ---- internal helpers ----

    /// Propose a command to the engine and check the response.
    async fn propose_and_check(&self, cmd: TableCommand) -> Result<()> {
        let payload = cmd.to_bytes()?;
        let entry = Entry::new(payload);
        let ret = self.engine.propose(entry).await?;

        match ret {
            ReturnValue::Data(bytes) => {
                let resp = TableResponse::from_bytes(&bytes)?;
                match resp {
                    TableResponse::Ok => Ok(()),
                    TableResponse::Error(msg) => Err(RelosError::InvalidOperation(msg)),
                }
            }
            ReturnValue::Success => Ok(()),
        }
    }

    /// Build the store key for a row (mirrors applicator logic).
    fn row_key(table: &str, pk_values: &[Value]) -> Vec<u8> {
        let pk_encoded = encode_primary_key(pk_values);
        format!("data/{}/{}", table, pk_encoded).into_bytes()
    }
}

/// Encode primary key values — must match the applicator's encoding.
fn encode_primary_key(values: &[Value]) -> String {
    values
        .iter()
        .map(|v| {
            let bytes =
                bincode::serialize(v).expect("primary key value serialization should not fail");
            hex::encode(bytes)
        })
        .collect::<Vec<_>>()
        .join("\x00")
}
