use std::sync::Arc;

use relos_core::Result;
use relos_engine::BaseEngine;
use relos_log::{LogChain, Loglet, LogletFactory, MemoryLoglet, MemoryMetaStore, VirtualLog};
use relos_store::MemoryStore;
use relos_table::{DelosTable, TableApplicator};

mod config;
mod grpc_service;

/// Generated protobuf / tonic types.
mod proto {
    tonic::include_proto!("relos");
}

use config::ServerConfig;
use grpc_service::RelosTableService;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = ServerConfig::default();
    tracing::info!("Starting Relos server with config: {:?}", config);

    // Check if --demo flag was passed
    let args: Vec<String> = std::env::args().collect();
    let demo_mode = args.iter().any(|a| a == "--demo");

    // Build the stack: MemoryStore -> VirtualLog -> BaseEngine -> DelosTable
    let store = Arc::new(MemoryStore::new());

    let initial_chain = LogChain::new("memory-0".to_string());
    let meta_store: Arc<dyn relos_log::MetaStore> = Arc::new(MemoryMetaStore::new(initial_chain));
    let factory: Arc<dyn LogletFactory> = Arc::new(MemoryLogletFactory);
    let virtual_log: Arc<dyn Loglet> = Arc::new(VirtualLog::new(meta_store, factory).await?);

    let applicator = Arc::new(TableApplicator::new());
    let engine = BaseEngine::new(virtual_log, store.clone(), applicator).await?;

    let table = Arc::new(DelosTable::new(engine, store));

    if demo_mode {
        tracing::info!("Running in demo mode");
        demo(&table).await?;
    } else {
        // Start gRPC server
        let addr = config.grpc_addr().parse()?;
        tracing::info!("Relos gRPC server listening on {}", addr);

        let svc = RelosTableService::new(table);
        tonic::transport::Server::builder()
            .add_service(proto::relos_table_server::RelosTableServer::new(svc))
            .serve(addr)
            .await?;
    }

    Ok(())
}

struct MemoryLogletFactory;

#[async_trait::async_trait]
impl LogletFactory for MemoryLogletFactory {
    async fn create(&self, _loglet_id: &str) -> Result<Arc<dyn Loglet>> {
        Ok(Arc::new(MemoryLoglet::new()))
    }
}

async fn demo(table: &DelosTable) -> Result<()> {
    use relos_table::{ColumnDef, ColumnType, Row, TableSchema, Value};

    // Create a table
    let schema = TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::Int64,
                nullable: false,
            },
            ColumnDef {
                name: "name".to_string(),
                col_type: ColumnType::Utf8,
                nullable: false,
            },
            ColumnDef {
                name: "email".to_string(),
                col_type: ColumnType::Utf8,
                nullable: true,
            },
        ],
        primary_key: vec!["id".to_string()],
    };

    table.create_table(schema).await?;
    tracing::info!("Created table 'users'");

    // Insert some rows
    table
        .put(
            "users",
            Row {
                values: vec![
                    Value::Int64(1),
                    Value::Utf8("Alice".to_string()),
                    Value::Utf8("alice@example.com".to_string()),
                ],
            },
        )
        .await?;

    table
        .put(
            "users",
            Row {
                values: vec![
                    Value::Int64(2),
                    Value::Utf8("Bob".to_string()),
                    Value::Utf8("bob@example.com".to_string()),
                ],
            },
        )
        .await?;

    tracing::info!("Inserted 2 rows");

    // Read back
    let row = table.get("users", &[Value::Int64(1)]).await?;
    tracing::info!("Get user 1: {:?}", row);

    let all_rows = table.scan("users").await?;
    tracing::info!("Scan users: {} rows", all_rows.len());
    for row in &all_rows {
        tracing::info!("  {:?}", row);
    }

    // Delete
    table.delete("users", vec![Value::Int64(2)]).await?;
    tracing::info!("Deleted user 2");

    let all_rows = table.scan("users").await?;
    tracing::info!("After delete: {} rows", all_rows.len());

    tracing::info!("Demo complete!");
    Ok(())
}
