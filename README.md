# Relos

A Rust implementation of the [Delos](https://research.facebook.com/publications/virtual-consensus-in-delos/) system, based on two papers from Meta:

- *Virtual Consensus in Delos* (OSDI 2020)
- *Log-structured Protocols in Delos* (SOSP 2021)

Relos is a replicated table store built on a shared log abstraction. It layers three components:

1. **VirtualLog** — chains pluggable loglets into a single shared log
2. **Engine stack** — stackable replicated state machines over the shared log
3. **DelosTable** — relational table API (create, put, get, delete, scan)

## Architecture

```
┌─────────────────────────────────┐
│          DelosTable             │  ← relational table API
├─────────────────────────────────┤
│     BatchingEngine (optional)   │  ← groups proposals for throughput
├─────────────────────────────────┤
│          BaseEngine             │  ← single-threaded apply loop
├─────────────────────────────────┤
│          VirtualLog             │  ← chains loglets via reconfiguration
├──────────┬──────────┬───────────┤
│ Loglet 1 │ Loglet 2 │ Loglet N  │  ← pluggable log segments
└──────────┴──────────┴───────────┘
│          LocalStore             │  ← transactional KV for local state
└─────────────────────────────────┘
```

## Crates

| Crate | Description |
|-------|-------------|
| `relos-core` | Shared types (`LogPos`, `Entry`, `RelosError`) |
| `relos-log` | Shared log: `Loglet` trait, `MemoryLoglet`, `VirtualLog`, `MetaStore` |
| `relos-store` | Local state: `LocalStore` trait, `MemoryStore` with MVCC transactions |
| `relos-engine` | Engine stack: `BaseEngine` (apply loop), `BatchingEngine` (throughput optimization) |
| `relos-table` | `DelosTable` API, `TableSchema`, `TableApplicator`, column types |
| `relos-server` | Binary entry point with demo wiring |

## Dependencies

All dependencies are managed through workspace-level settings in the root `Cargo.toml`:

| Dependency | Version | Purpose |
|------------|---------|---------|
| `tokio` | 1 (full) | Async runtime |
| `serde` | 1 (derive) | Serialization framework |
| `bincode` | 1 | Binary encoding for entries and payloads |
| `async-trait` | 0.1 | Async trait support |
| `thiserror` | 2 | Typed error handling |
| `tracing` | 0.1 | Structured logging |
| `tracing-subscriber` | 0.3 | Log output formatting |
| `bytes` | 1 | Byte buffer utilities |
| `hex` | 0.4 | Hex encoding for primary keys |

## Prerequisites

- [Rust](https://rustup.rs/) (stable toolchain, 1.75+ recommended)

## Building

```sh
cargo build
```

## Running

The server binary runs a demo that creates a table, inserts rows, queries, and scans:

```sh
cargo run --bin relos-server
```

This wires up the full stack in-memory:

```
MemoryStore → VirtualLog(MemoryLoglet) → BaseEngine → TableApplicator → DelosTable
```

Example output:

```
Creating 'users' table...
Inserting rows...
Querying user 1: Row([Int64(1), Utf8("alice"), Int64(30)])
Scanning all users:
  Row([Int64(1), Utf8("alice"), Int64(30)])
  Row([Int64(2), Utf8("bob"), Int64(25)])
Deleting user 1...
User 1 after delete: None
```

## Testing

```sh
cargo test
```

54 tests across 4 crates:

| Crate | Tests | What's covered |
|-------|-------|----------------|
| `relos-log` | 28 | Loglet append/read/seal/trim, VirtualLog chaining, MetaStore CAS |
| `relos-store` | 14 | KV operations, transactions, snapshot isolation, prefix scan |
| `relos-engine` | 12 | Propose, sync, cursor persistence, batching, timeout flush |
| `relos-core` | 0 | Type definitions only |
| `relos-table` | 0 | Covered via engine integration |
| `relos-server` | 0 | Scaffold |

## Key Design Decisions

- **Single-threaded apply loop**: The BaseEngine processes entries sequentially through a channel, matching the Delos paper's insight that the apply thread is not the bottleneck.
- **Trait objects for stacking**: Engine layers compose via `dyn Engine` / `dyn Applicator`, enabling runtime stack assembly.
- **Cursor persistence**: The engine stores its log cursor in the same LocalStore transaction as each apply, providing exactly-once semantics on recovery.
- **MVCC-style isolation**: The MemoryStore uses `RwLock` with snapshot reads so queries don't block the apply loop.

## Project Status

This implements Milestones M1–M4 from the [implementation plan](PLAN.md):

- [x] M1 — Core types and traits
- [x] M2 — MemoryLoglet + VirtualLog
- [x] M3 — LocalStore with transactions
- [x] M4 — BaseEngine + BatchingEngine + DelosTable
- [ ] M5 — Network loglet (Raft/Paxos)
- [ ] M6 — gRPC server with client API
- [ ] M7 — Reconfiguration protocol
- [ ] M8 — Distributed consensus

## License

This project is not yet licensed. See [PLAN.md](PLAN.md) for the full implementation roadmap.
