use std::sync::Arc;

use tonic::{Request, Response, Status};

use relos_table::DelosTable;

use crate::proto;
use crate::proto::relos_table_server::RelosTable;

// ---------------------------------------------------------------------------
// Type conversion helpers: proto <-> relos-table types
// ---------------------------------------------------------------------------

fn proto_column_type_to_relos(ct: i32) -> Result<relos_table::ColumnType, Status> {
    match proto::ColumnType::try_from(ct) {
        Ok(proto::ColumnType::Int64) => Ok(relos_table::ColumnType::Int64),
        Ok(proto::ColumnType::Utf8) => Ok(relos_table::ColumnType::Utf8),
        Ok(proto::ColumnType::Bytes) => Ok(relos_table::ColumnType::Bytes),
        Ok(proto::ColumnType::Bool) => Ok(relos_table::ColumnType::Bool),
        Ok(proto::ColumnType::Float64) => Ok(relos_table::ColumnType::Float64),
        Ok(proto::ColumnType::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("unspecified or unknown column type"))
        }
    }
}

fn relos_column_type_to_proto(ct: &relos_table::ColumnType) -> proto::ColumnType {
    match ct {
        relos_table::ColumnType::Int64 => proto::ColumnType::Int64,
        relos_table::ColumnType::Utf8 => proto::ColumnType::Utf8,
        relos_table::ColumnType::Bytes => proto::ColumnType::Bytes,
        relos_table::ColumnType::Bool => proto::ColumnType::Bool,
        relos_table::ColumnType::Float64 => proto::ColumnType::Float64,
    }
}

fn proto_value_to_relos(v: &proto::Value) -> relos_table::Value {
    match &v.kind {
        Some(proto::value::Kind::IsNull(true)) | None => relos_table::Value::Null,
        Some(proto::value::Kind::IsNull(false)) => relos_table::Value::Null,
        Some(proto::value::Kind::Int64Value(n)) => relos_table::Value::Int64(*n),
        Some(proto::value::Kind::StringValue(s)) => relos_table::Value::Utf8(s.clone()),
        Some(proto::value::Kind::BytesValue(b)) => relos_table::Value::Bytes(b.clone()),
        Some(proto::value::Kind::BoolValue(b)) => relos_table::Value::Bool(*b),
        Some(proto::value::Kind::Float64Value(f)) => relos_table::Value::Float64(*f),
    }
}

fn relos_value_to_proto(v: &relos_table::Value) -> proto::Value {
    let kind = match v {
        relos_table::Value::Null => Some(proto::value::Kind::IsNull(true)),
        relos_table::Value::Int64(n) => Some(proto::value::Kind::Int64Value(*n)),
        relos_table::Value::Utf8(s) => Some(proto::value::Kind::StringValue(s.clone())),
        relos_table::Value::Bytes(b) => Some(proto::value::Kind::BytesValue(b.clone())),
        relos_table::Value::Bool(b) => Some(proto::value::Kind::BoolValue(*b)),
        relos_table::Value::Float64(f) => Some(proto::value::Kind::Float64Value(*f)),
    };
    proto::Value { kind }
}

fn proto_column_def_to_relos(cd: &proto::ColumnDef) -> Result<relos_table::ColumnDef, Status> {
    Ok(relos_table::ColumnDef {
        name: cd.name.clone(),
        col_type: proto_column_type_to_relos(cd.column_type)?,
        nullable: cd.nullable,
    })
}

fn relos_column_def_to_proto(cd: &relos_table::ColumnDef) -> proto::ColumnDef {
    proto::ColumnDef {
        name: cd.name.clone(),
        column_type: relos_column_type_to_proto(&cd.col_type).into(),
        nullable: cd.nullable,
    }
}

fn proto_schema_to_relos(s: &proto::TableSchema) -> Result<relos_table::TableSchema, Status> {
    let columns = s
        .columns
        .iter()
        .map(proto_column_def_to_relos)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(relos_table::TableSchema {
        name: s.name.clone(),
        columns,
        primary_key: s.primary_key.clone(),
    })
}

#[allow(dead_code)]
fn relos_schema_to_proto(s: &relos_table::TableSchema) -> proto::TableSchema {
    proto::TableSchema {
        name: s.name.clone(),
        columns: s.columns.iter().map(relos_column_def_to_proto).collect(),
        primary_key: s.primary_key.clone(),
    }
}

fn proto_row_to_relos(r: &proto::Row) -> relos_table::Row {
    relos_table::Row {
        values: r.values.iter().map(proto_value_to_relos).collect(),
    }
}

fn relos_row_to_proto(r: &relos_table::Row) -> proto::Row {
    proto::Row {
        values: r.values.iter().map(relos_value_to_proto).collect(),
    }
}

fn relos_err_to_status(e: relos_core::RelosError) -> Status {
    use relos_core::RelosError::*;
    match e {
        NotFound(msg) => Status::not_found(msg),
        InvalidOperation(msg) => Status::invalid_argument(msg),
        Serialization(msg) => Status::internal(format!("serialization error: {msg}")),
        Store(msg) => Status::internal(format!("store error: {msg}")),
        Internal(msg) => Status::internal(msg),
        other => Status::internal(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// gRPC service implementation
// ---------------------------------------------------------------------------

pub struct RelosTableService {
    table: Arc<DelosTable>,
}

impl RelosTableService {
    pub fn new(table: Arc<DelosTable>) -> Self {
        Self { table }
    }
}

#[tonic::async_trait]
impl RelosTable for RelosTableService {
    async fn create_table(
        &self,
        request: Request<proto::CreateTableRequest>,
    ) -> Result<Response<proto::CreateTableResponse>, Status> {
        let req = request.into_inner();
        let schema = req
            .schema
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing schema"))?;
        let relos_schema = proto_schema_to_relos(schema)?;

        self.table
            .create_table(relos_schema)
            .await
            .map_err(relos_err_to_status)?;

        Ok(Response::new(proto::CreateTableResponse {}))
    }

    async fn drop_table(
        &self,
        request: Request<proto::DropTableRequest>,
    ) -> Result<Response<proto::DropTableResponse>, Status> {
        let req = request.into_inner();
        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name must not be empty"));
        }

        self.table
            .drop_table(&req.table)
            .await
            .map_err(relos_err_to_status)?;

        Ok(Response::new(proto::DropTableResponse {}))
    }

    async fn put(
        &self,
        request: Request<proto::PutRequest>,
    ) -> Result<Response<proto::PutResponse>, Status> {
        let req = request.into_inner();
        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name must not be empty"));
        }
        let row = req
            .row
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing row"))?;
        let relos_row = proto_row_to_relos(row);

        self.table
            .put(&req.table, relos_row)
            .await
            .map_err(relos_err_to_status)?;

        Ok(Response::new(proto::PutResponse {}))
    }

    async fn delete(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::DeleteResponse>, Status> {
        let req = request.into_inner();
        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name must not be empty"));
        }
        let key: Vec<relos_table::Value> = req.key.iter().map(proto_value_to_relos).collect();

        self.table
            .delete(&req.table, key)
            .await
            .map_err(relos_err_to_status)?;

        Ok(Response::new(proto::DeleteResponse {}))
    }

    async fn get(
        &self,
        request: Request<proto::GetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        let req = request.into_inner();
        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name must not be empty"));
        }
        let key: Vec<relos_table::Value> = req.key.iter().map(proto_value_to_relos).collect();

        let maybe_row = self
            .table
            .get(&req.table, &key)
            .await
            .map_err(relos_err_to_status)?;

        match maybe_row {
            Some(row) => Ok(Response::new(proto::GetResponse {
                found: true,
                row: Some(relos_row_to_proto(&row)),
            })),
            None => Ok(Response::new(proto::GetResponse {
                found: false,
                row: None,
            })),
        }
    }

    async fn scan(
        &self,
        request: Request<proto::ScanRequest>,
    ) -> Result<Response<proto::ScanResponse>, Status> {
        let req = request.into_inner();
        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name must not be empty"));
        }

        let rows = self
            .table
            .scan(&req.table)
            .await
            .map_err(relos_err_to_status)?;

        Ok(Response::new(proto::ScanResponse {
            rows: rows.iter().map(relos_row_to_proto).collect(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Tests for type conversions
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_roundtrip_null() {
        let relos_val = relos_table::Value::Null;
        let proto_val = relos_value_to_proto(&relos_val);
        let back = proto_value_to_relos(&proto_val);
        assert_eq!(back, relos_table::Value::Null);
    }

    #[test]
    fn test_value_roundtrip_int64() {
        let relos_val = relos_table::Value::Int64(42);
        let proto_val = relos_value_to_proto(&relos_val);
        let back = proto_value_to_relos(&proto_val);
        assert_eq!(back, relos_table::Value::Int64(42));
    }

    #[test]
    fn test_value_roundtrip_utf8() {
        let relos_val = relos_table::Value::Utf8("hello".to_string());
        let proto_val = relos_value_to_proto(&relos_val);
        let back = proto_value_to_relos(&proto_val);
        assert_eq!(back, relos_table::Value::Utf8("hello".to_string()));
    }

    #[test]
    fn test_value_roundtrip_bytes() {
        let relos_val = relos_table::Value::Bytes(vec![1, 2, 3]);
        let proto_val = relos_value_to_proto(&relos_val);
        let back = proto_value_to_relos(&proto_val);
        assert_eq!(back, relos_table::Value::Bytes(vec![1, 2, 3]));
    }

    #[test]
    fn test_value_roundtrip_bool() {
        let relos_val = relos_table::Value::Bool(true);
        let proto_val = relos_value_to_proto(&relos_val);
        let back = proto_value_to_relos(&proto_val);
        assert_eq!(back, relos_table::Value::Bool(true));
    }

    #[test]
    fn test_value_roundtrip_float64() {
        let relos_val = relos_table::Value::Float64(3.14);
        let proto_val = relos_value_to_proto(&relos_val);
        let back = proto_value_to_relos(&proto_val);
        assert_eq!(back, relos_table::Value::Float64(3.14));
    }

    #[test]
    fn test_column_type_roundtrip() {
        let types = vec![
            relos_table::ColumnType::Int64,
            relos_table::ColumnType::Utf8,
            relos_table::ColumnType::Bytes,
            relos_table::ColumnType::Bool,
            relos_table::ColumnType::Float64,
        ];
        for ct in types {
            let proto_ct = relos_column_type_to_proto(&ct);
            let back = proto_column_type_to_relos(proto_ct.into()).unwrap();
            // Compare via debug repr since ColumnType doesn't derive PartialEq
            assert_eq!(format!("{:?}", back), format!("{:?}", ct));
        }
    }

    #[test]
    fn test_column_type_unspecified_is_error() {
        let result = proto_column_type_to_relos(proto::ColumnType::Unspecified.into());
        assert!(result.is_err());
    }

    #[test]
    fn test_row_roundtrip() {
        let relos_row = relos_table::Row {
            values: vec![
                relos_table::Value::Int64(1),
                relos_table::Value::Utf8("test".to_string()),
                relos_table::Value::Null,
            ],
        };
        let proto_row = relos_row_to_proto(&relos_row);
        let back = proto_row_to_relos(&proto_row);
        assert_eq!(back.values.len(), 3);
        assert_eq!(back.values[0], relos_table::Value::Int64(1));
        assert_eq!(back.values[1], relos_table::Value::Utf8("test".to_string()));
        assert_eq!(back.values[2], relos_table::Value::Null);
    }

    #[test]
    fn test_schema_conversion() {
        let proto_schema = proto::TableSchema {
            name: "users".to_string(),
            columns: vec![
                proto::ColumnDef {
                    name: "id".to_string(),
                    column_type: proto::ColumnType::Int64.into(),
                    nullable: false,
                },
                proto::ColumnDef {
                    name: "name".to_string(),
                    column_type: proto::ColumnType::Utf8.into(),
                    nullable: true,
                },
            ],
            primary_key: vec!["id".to_string()],
        };
        let relos_schema = proto_schema_to_relos(&proto_schema).unwrap();
        assert_eq!(relos_schema.name, "users");
        assert_eq!(relos_schema.columns.len(), 2);
        assert_eq!(relos_schema.columns[0].name, "id");
        assert!(!relos_schema.columns[0].nullable);
        assert_eq!(relos_schema.columns[1].name, "name");
        assert!(relos_schema.columns[1].nullable);
        assert_eq!(relos_schema.primary_key, vec!["id".to_string()]);
    }
}
