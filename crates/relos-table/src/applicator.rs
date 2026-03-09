use relos_core::{Entry, LogPos, RelosError, Result};
use relos_engine::{Applicator, ReturnValue};
use relos_store::WriteTransaction;
use tracing::debug;

use crate::command::{TableCommand, TableResponse};
use crate::schema::Value;

/// The DelosTable applicator — applies table commands to the LocalStore.
///
/// Key layout in the LocalStore:
/// - Table schemas: key = `schema/{table_name}`, value = serialized TableSchema
/// - Row data: key = `data/{table_name}/{primary_key_encoded}`, value = serialized Row
/// - Primary key encoding: each Value serialized with bincode, hex-encoded, joined with `\x00`
pub struct TableApplicator;

impl TableApplicator {
    pub fn new() -> Self {
        Self
    }

    /// Build the store key for a table schema.
    fn schema_key(table_name: &str) -> Vec<u8> {
        format!("schema/{}", table_name).into_bytes()
    }

    /// Build the store key for a row.
    fn row_key(table_name: &str, pk_values: &[Value]) -> Vec<u8> {
        let pk_encoded = encode_primary_key(pk_values);
        format!("data/{}/{}", table_name, pk_encoded).into_bytes()
    }

    /// Build the prefix for all rows of a table.
    fn row_prefix(table_name: &str) -> Vec<u8> {
        format!("data/{}/", table_name).into_bytes()
    }

    /// Load a table schema from the store.
    fn load_schema(
        txn: &dyn WriteTransaction,
        table_name: &str,
    ) -> Result<Option<crate::schema::TableSchema>> {
        let key = Self::schema_key(table_name);
        match txn.get(&key)? {
            Some(bytes) => {
                let schema: crate::schema::TableSchema = bincode::deserialize(&bytes)
                    .map_err(|e| RelosError::Serialization(e.to_string()))?;
                Ok(Some(schema))
            }
            None => Ok(None),
        }
    }

    /// Extract primary key values from a row given the schema.
    fn extract_pk(
        schema: &crate::schema::TableSchema,
        row: &crate::schema::Row,
    ) -> Result<Vec<Value>> {
        let mut pk_values = Vec::new();
        for pk_col_name in &schema.primary_key {
            let idx = schema
                .columns
                .iter()
                .position(|c| &c.name == pk_col_name)
                .ok_or_else(|| {
                    RelosError::InvalidOperation(format!(
                        "primary key column '{}' not found in schema",
                        pk_col_name
                    ))
                })?;
            if idx >= row.values.len() {
                return Err(RelosError::InvalidOperation(format!(
                    "row has {} values but primary key column '{}' is at index {}",
                    row.values.len(),
                    pk_col_name,
                    idx
                )));
            }
            pk_values.push(row.values[idx].clone());
        }
        Ok(pk_values)
    }

    fn make_response(resp: TableResponse) -> Result<ReturnValue> {
        let bytes = resp.to_bytes()?;
        Ok(ReturnValue::Data(bytes))
    }

    fn apply_create_table(
        txn: &mut dyn WriteTransaction,
        schema: crate::schema::TableSchema,
    ) -> Result<ReturnValue> {
        let key = Self::schema_key(&schema.name);
        if txn.get(&key)?.is_some() {
            return Self::make_response(TableResponse::Error(format!(
                "table '{}' already exists",
                schema.name
            )));
        }
        let value =
            bincode::serialize(&schema).map_err(|e| RelosError::Serialization(e.to_string()))?;
        txn.put(&key, &value)?;
        debug!(table = %schema.name, "created table");
        Self::make_response(TableResponse::Ok)
    }

    fn apply_drop_table(txn: &mut dyn WriteTransaction, table: &str) -> Result<ReturnValue> {
        let schema_key = Self::schema_key(table);
        if txn.get(&schema_key)?.is_none() {
            return Self::make_response(TableResponse::Error(format!(
                "table '{}' does not exist",
                table
            )));
        }
        // Delete schema
        txn.delete(&schema_key)?;
        // Delete all rows with prefix data/{table}/
        let prefix = Self::row_prefix(table);
        let rows = txn.prefix_scan(&prefix)?;
        for (key, _) in rows {
            txn.delete(&key)?;
        }
        debug!(table = %table, "dropped table");
        Self::make_response(TableResponse::Ok)
    }

    fn apply_put(
        txn: &mut dyn WriteTransaction,
        table: &str,
        row: crate::schema::Row,
    ) -> Result<ReturnValue> {
        let schema = match Self::load_schema(txn, table)? {
            Some(s) => s,
            None => {
                return Self::make_response(TableResponse::Error(format!(
                    "table '{}' does not exist",
                    table
                )));
            }
        };

        // Validate row length matches schema
        if row.values.len() != schema.columns.len() {
            return Self::make_response(TableResponse::Error(format!(
                "row has {} values but schema has {} columns",
                row.values.len(),
                schema.columns.len()
            )));
        }

        let pk_values = Self::extract_pk(&schema, &row)?;
        let key = Self::row_key(table, &pk_values);
        let value =
            bincode::serialize(&row).map_err(|e| RelosError::Serialization(e.to_string()))?;
        txn.put(&key, &value)?;
        debug!(table = %table, "put row");
        Self::make_response(TableResponse::Ok)
    }

    fn apply_delete(
        txn: &mut dyn WriteTransaction,
        table: &str,
        key_values: &[Value],
    ) -> Result<ReturnValue> {
        if Self::load_schema(txn, table)?.is_none() {
            return Self::make_response(TableResponse::Error(format!(
                "table '{}' does not exist",
                table
            )));
        }
        let key = Self::row_key(table, key_values);
        txn.delete(&key)?;
        debug!(table = %table, "deleted row");
        Self::make_response(TableResponse::Ok)
    }
}

impl Default for TableApplicator {
    fn default() -> Self {
        Self::new()
    }
}

impl Applicator for TableApplicator {
    fn apply(
        &self,
        txn: &mut dyn WriteTransaction,
        entry: &Entry,
        pos: LogPos,
    ) -> Result<ReturnValue> {
        let cmd = TableCommand::from_bytes(&entry.payload)?;
        debug!(?cmd, pos, "applying table command");

        match cmd {
            TableCommand::CreateTable(schema) => Self::apply_create_table(txn, schema),
            TableCommand::DropTable { table } => Self::apply_drop_table(txn, &table),
            TableCommand::Put { table, row } => Self::apply_put(txn, &table, row),
            TableCommand::Delete { table, key } => Self::apply_delete(txn, &table, &key),
        }
    }
}

/// Encode primary key values into a string suitable for use in a store key.
/// Each Value is serialized with bincode and hex-encoded, joined by `\x00`.
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
