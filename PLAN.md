# Relos: Delos Table in Rust — Implementation Plan

## Overview

Relos is a Rust implementation of the Delos system described in two papers:
1. **"Virtual Consensus in Delos"** (OSDI 2020) — the VirtualLog and Loglet abstraction
2. **"Log-structured Protocols in Delos"** (SOSP 2021) — the engine stack and DelosTable

The system has three layers:
- **Bottom**: VirtualLog (chains Loglets to provide a shared log)
- **Middle**: Engine stack (stackable replicated state machines over the shared log)
- **Top**: DelosTable application (relational table API)

---

## Project Structure

```
relos/
├── Cargo.toml                    # Workspace root
├── crates/
│   ├── relos-core/               # Core types, traits, errors
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── types.rs          # LogPos, ServerId, EntryType, ReturnType
│   │   │   ├── error.rs          # Error types
│   │   │   └── entry.rs          # Log entry with header map + payload
│   │   └── Cargo.toml
│   │
│   ├── relos-store/              # LocalStore abstraction
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── traits.rs         # LocalStore, RWTx, ROTx traits
│   │   │   └── memory.rs         # In-memory implementation
│   │   └── Cargo.toml
│   │
│   ├── relos-log/                # Shared log: VirtualLog + Loglets
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── loglet.rs         # Loglet trait
│   │   │   ├── memory_loglet.rs  # In-memory loglet (for testing)
│   │   │   ├── virtual_log.rs    # VirtualLog: chains loglets
│   │   │   ├── metastore.rs      # MetaStore: versioned register for chain config
│   │   │   ├── chain.rs          # LogChain: sequence of (loglet, start, end) segments
│   │   │   └── native_loglet.rs  # NativeLoglet: sequencer + log servers (later phase)
│   │   └── Cargo.toml
│   │
│   ├── relos-engine/             # Engine stack framework
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── traits.rs         # IEngine, IApplicator traits
│   │   │   ├── base_engine.rs    # BaseEngine: bottom of stack, drives the shared log
│   │   │   ├── stack.rs          # EngineStack: composes engines in order
│   │   │   ├── view_tracking.rs  # ViewTrackingEngine: log trimming coordination
│   │   │   ├── observer.rs       # ObserverEngine: latency monitoring
│   │   │   └── batching.rs       # BatchingEngine: group commit optimization
│   │   └── Cargo.toml
│   │
│   ├── relos-table/              # DelosTable application
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── schema.rs         # Table schema definitions
│   │   │   ├── table.rs          # Table operations (CRUD)
│   │   │   ├── applicator.rs     # DelosTable IApplicator implementation
│   │   │   ├── wrapper.rs        # DelosTable Wrapper (external API -> propose)
│   │   │   └── query.rs          # Query execution
│   │   └── Cargo.toml
│   │
│   └── relos-server/             # Server binary: wires everything together
│       ├── src/
│       │   ├── main.rs
│       │   ├── config.rs         # Server configuration
│       │   └── server.rs         # Server lifecycle
│       └── Cargo.toml
│
└── tests/                        # Integration tests
    └── integration.rs
```

---

## Phase 1: Core Types & LocalStore

### 1.1 `relos-core` — Foundational types

```rust
/// Position in the shared log
pub type LogPos = u64;

/// Unique server identifier
pub type ServerId = u64;

/// A log entry: map of engine headers + application payload
#[derive(Clone, Serialize, Deserialize)]
pub struct Entry {
    pub headers: HashMap<String, Vec<u8>>,  // engine_name -> serialized header
    pub payload: Vec<u8>,                    // application payload
}

/// Result type alias
pub type Result<T> = std::result::Result<T, RelosError>;
```

Key design decisions:
- Use `serde` for serialization (entries stored as bincode or MessagePack)
- Headers stored as a map (not a literal stack of buffers) — the paper explicitly recommends this for resilience to stack upgrades
- `LogPos` is a simple `u64` — the global position in the virtual log

### 1.2 `relos-store` — LocalStore abstraction

The LocalStore provides a key-value API with transactional semantics:

```rust
#[async_trait]
pub trait LocalStore: Send + Sync {
    /// Begin a read-write transaction
    fn begin_rw_txn(&self) -> Result<Box<dyn RWTx>>;
    /// Begin a read-only transaction (snapshot)
    fn begin_ro_txn(&self) -> Result<Box<dyn ROTx>>;
    /// Flush pending writes to durable storage
    async fn flush(&self) -> Result<()>;
}

pub trait ROTx: Send {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

pub trait RWTx: ROTx {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&mut self, key: &[u8]) -> Result<()>;
    fn commit(self: Box<Self>) -> Result<()>;
    fn rollback(self: Box<Self>) -> Result<()>;
}
```

Implementations:
- **Phase 1**: In-memory `BTreeMap`-based store with MVCC snapshots
- **Later**: RocksDB backend via `rust-rocksdb`

---

## Phase 2: Shared Log Layer (VirtualLog + Loglets)

### 2.1 Loglet trait

```rust
#[async_trait]
pub trait Loglet: Send + Sync {
    /// Append an entry; returns the assigned log position
    async fn append(&self, entry: &Entry) -> Result<LogPos>;

    /// Check the current tail (last committed position + 1)
    async fn check_tail(&self) -> Result<LogPos>;

    /// Read the next entry at or after `pos`; returns (entry, position)
    async fn read_next(&self, pos: LogPos) -> Result<Option<(Entry, LogPos)>>;

    /// Trim all entries before `pos`
    async fn trim(&self, pos: LogPos) -> Result<()>;

    /// Seal the loglet — no further appends will be accepted
    /// Returns the final tail position after sealing
    async fn seal(&self) -> Result<LogPos>;
}
```

### 2.2 MemoryLoglet (for testing and single-node)

An in-memory loglet backed by a `Vec<Entry>` protected by `RwLock`. Supports seal.

### 2.3 MetaStore

The MetaStore is a versioned register storing the log chain configuration:

```rust
pub struct LogChain {
    pub segments: Vec<ChainSegment>,
}

pub struct ChainSegment {
    pub loglet_id: String,
    pub start_pos: LogPos,     // inclusive
    pub end_pos: Option<LogPos>, // exclusive; None = active/open segment
}

#[async_trait]
pub trait MetaStore: Send + Sync {
    /// Read the current chain and its version
    async fn read(&self) -> Result<(LogChain, u64)>;
    /// Conditionally write a new chain (CAS on version)
    async fn write(&self, chain: LogChain, expected_version: u64) -> Result<bool>;
}
```

Phase 1 implementation: in-memory `Mutex<(LogChain, u64)>`.
Later: backed by a separate fault-tolerant store (or even another Delos instance).

### 2.4 VirtualLog

The VirtualLog chains loglets per the MetaStore configuration:

```rust
pub struct VirtualLog {
    meta_store: Arc<dyn MetaStore>,
    loglet_factory: Arc<dyn LogletFactory>,  // creates loglets by ID
    chain: RwLock<LogChain>,
    loglets: RwLock<HashMap<String, Arc<dyn Loglet>>>,
}
```

Key operations:
- **append**: Append to the active (last) segment's loglet. If sealed, trigger reconfiguration.
- **check_tail**: Return the tail of the active segment's loglet, offset by the segment's global start.
- **read_next(pos)**: Find which segment contains `pos`, read from that loglet.
- **trim(pos)**: Trim all segments whose end ≤ pos.
- **reconfigure**: Seal active loglet → write new chain to MetaStore → instantiate new loglet.

Position mapping: `global_pos = segment.start_pos + local_loglet_pos`.

---

## Phase 3: Engine Stack Framework

### 3.1 Core traits

```rust
#[async_trait]
pub trait Engine: Send + Sync {
    /// Propose an entry to be appended to the log
    async fn propose(&self, entry: Entry) -> Result<ReturnValue>;

    /// Sync: ensure all committed entries have been applied locally
    async fn sync(&self) -> Result<Arc<dyn ROTx>>;

    /// Set the trimmable prefix
    fn set_trim_prefix(&self, pos: LogPos);
}

pub trait Applicator: Send + Sync {
    /// Apply a log entry (within a transaction)
    fn apply(&self, txn: &mut dyn RWTx, entry: &Entry, pos: LogPos) -> Result<ReturnValue>;

    /// Post-apply callback (after successful commit)
    fn post_apply(&self, entry: &Entry, pos: LogPos);
}
```

### 3.2 BaseEngine

The BaseEngine sits at the bottom of the stack and drives the shared log:

- Owns the **apply thread** (a single dedicated `tokio::task`)
- Maintains a **cursor** in the LocalStore tracking the last applied position
- On **propose**: append to VirtualLog → play log forward until the new entry → return result
- On **sync**: check_tail on VirtualLog → play log forward until tail → return ROTx snapshot
- On **play forward**: for each entry from cursor to target:
  1. Begin LocalStore transaction
  2. Update cursor in the transaction
  3. Call `apply` on the upstream engine/applicator
  4. Commit the transaction
- Runs a background task for periodic **LocalStore flush** and **log trimming**
- Queues multiple sync calls behind a single outstanding tail check (optimization from paper)

### 3.3 EngineStack builder

```rust
pub struct EngineStackBuilder {
    engines: Vec<Box<dyn MiddleEngine>>,
}

impl EngineStackBuilder {
    pub fn new() -> Self { ... }
    pub fn push(mut self, engine: Box<dyn MiddleEngine>) -> Self { ... }
    pub fn build(
        self,
        virtual_log: Arc<VirtualLog>,
        local_store: Arc<dyn LocalStore>,
        applicator: Box<dyn Applicator>,
    ) -> Arc<dyn Engine> { ... }
}
```

Each middle engine wraps a downstream `Engine` and an upstream `Applicator`, implementing both traits. The builder wires them together bottom-up: BaseEngine → Engine1 → Engine2 → ... → Applicator.

### 3.4 ViewTrackingEngine

- Piggybacks local durable playback position on each outgoing propose
- Maintains a map `ServerId -> LogPos` in the LocalStore
- Computes the minimum across all live servers as the safe trim prefix
- Detects server failures via absence of log entries within a timeout
- Calls `set_trim_prefix` on the engine below

### 3.5 ObserverEngine

- Lightweight pass-through that measures propose/sync latencies
- Reports metrics via a `Metrics` trait (pluggable backend)

### 3.6 BatchingEngine (optimization)

- Accumulates multiple propose calls within a time window or batch size
- Proposes a single batched entry downstream
- On apply, unbatches and applies each sub-entry individually within one transaction (group commit)
- 2X throughput improvement per the paper

---

## Phase 4: DelosTable Application

### 4.1 Table Schema

```rust
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<String>,
    pub secondary_indices: Vec<IndexDef>,
}

pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
}

pub enum ColumnType {
    Int64,
    Utf8String,
    Bytes,
    Bool,
    Float64,
}
```

### 4.2 Table Operations (the "entry" types proposed into the log)

```rust
pub enum TableCommand {
    CreateTable(TableSchema),
    DropTable(String),
    Put { table: String, row: Row },
    Delete { table: String, key: Vec<Value> },
    BatchWrite { ops: Vec<TableCommand> },
}

pub enum TableResponse {
    Ok,
    Row(Option<Row>),
    Rows(Vec<Row>),
    Error(String),
}
```

### 4.3 DelosTable Applicator

- Receives `TableCommand` entries via the `apply` upcall
- Executes them against the LocalStore within the provided transaction
- Rows stored as: key = `table_name/primary_key_values`, value = serialized row
- Secondary indices stored as: key = `idx/index_name/index_values`, value = primary key

### 4.4 DelosTable Wrapper

- Exposes the external Table API (get, put, delete, scan, create_table)
- **Writes**: serialize as `TableCommand`, call `propose` on top engine, await response
- **Reads**: call `sync` on top engine, then read directly from LocalStore snapshot
- Read-only operations do NOT go through the apply thread (just sync + local read)

---

## Phase 5: Server & Networking

### 5.1 Server binary

- Parses configuration (cluster members, loglet config, engine stack config)
- Instantiates: LocalStore → VirtualLog → Engine Stack → DelosTable
- Starts the apply thread and background tasks
- Exposes the Table API via gRPC (tonic) or a simple TCP protocol

### 5.2 Networking (for multi-node)

- **Client ↔ Server**: gRPC with tonic (Table API)
- **Server ↔ Loglet**: for NativeLoglet, the sequencer and log servers communicate via gRPC
- **MetaStore**: for production, backed by a simple Raft-based register or external coordination service

---

## Phase 6: NativeLoglet (Distributed Consensus)

Implement the NativeLoglet from the OSDI 2020 paper:

- **Sequencer**: assigns monotonic positions to append requests
- **LogServers**: store entries at assigned positions, respond to reads
- **Append protocol**: client → sequencer (get position) → fan out to log servers → ack when majority respond
- **Seal protocol**: send seal command to all log servers; sealed when majority have sealed; returns final tail
- **Recovery**: after seal, read entries from log servers, fill holes, determine definitive tail

---

## Implementation Order & Milestones

| Milestone | Crates | Deliverable |
|-----------|--------|-------------|
| **M1** | `relos-core`, `relos-store` | Core types, in-memory LocalStore with transactions |
| **M2** | `relos-log` (MemoryLoglet, MetaStore, VirtualLog) | Single-node shared log that works in-memory |
| **M3** | `relos-engine` (BaseEngine) | Minimal engine that drives apply loop over VirtualLog |
| **M4** | `relos-table` | DelosTable applicator + wrapper; end-to-end single-node table |
| **M5** | `relos-engine` (ViewTracking, Observer, Batching) | Full engine stack with optimizations |
| **M6** | `relos-server` | Server binary with gRPC API |
| **M7** | `relos-log` (NativeLoglet) | Distributed loglet with sequencer + log servers |
| **M8** | Multi-node | Full multi-node DelosTable cluster |

---

## Key Rust Design Decisions

1. **Async runtime**: `tokio` — the apply thread is a dedicated `tokio::task` using channels for coordination
2. **Serialization**: `serde` + `bincode` for entries and headers
3. **Trait objects vs generics**: Use `dyn Trait` for Engine/Applicator/Loglet to allow runtime composition of engine stacks (matching the paper's dynamic stack approach)
4. **Error handling**: `thiserror` for error types, `anyhow` at the binary level
5. **Concurrency**: The apply thread is single-threaded by design (paper confirms this is NOT the bottleneck). Propose threads are concurrent. Use `tokio::sync::mpsc` channels to funnel work to the apply thread.
6. **LocalStore transactions**: MVCC-style — RWTx acquires exclusive lock, ROTx reads from a consistent snapshot
7. **Testing**: Each crate has unit tests; integration tests compose the full stack with MemoryLoglet

---

## Dependencies

```toml
[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
bincode = "1"
async-trait = "0.1"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = "0.3"
tonic = "0.12"       # gRPC (Phase 6)
prost = "0.13"       # protobuf (Phase 6)
```
