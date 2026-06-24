# FFI Binding + SwiftUI macOS App Implementation Plan (Plan 2 of 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **HARD DEPENDENCY — Plan 1 must be complete first.** This plan assumes Plan 1 (`docs/superpowers/plans/2026-06-24-rust-core-cli.md`) has produced a working `crates/transfer-core` in the `mysql-transfer-rs` repo. Specifically, `transfer-core` MUST already expose (from `transfer.rs` / `event.rs` / `config.rs` / `error.rs`):
>
> ```rust
> // Async orchestration entry points
> pub async fn run_transfer(cfg: &TransferConfig, schema: bool, data: bool, sink: &EventSink) -> Result<TransferSummary, TransferError>;
> pub async fn run_inspect(cfg: &TransferConfig) -> Result<Inspection, TransferError>;
> pub async fn run_diff(cfg: &TransferConfig) -> Result<SchemaDiff, TransferError>;
> pub fn test_connection(cfg: &ConnectionConfig) -> (bool, String);  // sync, runs SELECT 1
>
> // Types (fields per the spec §5 and §6.1)
> pub struct EventSink { /* wraps tokio::sync::mpsc::Sender<TransferEvent> */ }
> impl EventSink { pub fn new(tx: tokio::sync::mpsc::Sender<TransferEvent>) -> Self; }
> pub enum TransferEvent { Phase(String), TableCounted{table:String,rows:u64}, TableStarted{table:String,total_rows:u64}, ChunkInserted{table:String,rows:u64}, TableCompleted(TableStats), Log{level:Level,message:String}, Summary(TransferSummary) }
> pub enum Level { Info, Warn, DryRun }
> pub struct TableStats { pub table:String, pub rows_transferred:u64, pub chunks:u64, pub error:Option<String> }
> pub struct TransferSummary { pub stats:Vec<TableStats>, pub total_rows:u64, pub errors:u64, pub elapsed_secs:f64, pub rows_per_sec:f64 }
> pub struct Inspection { pub tables:Vec<TableInfo>, pub views:Vec<String>, pub routines:Vec<RoutineInfo>, pub triggers:Vec<String> }
> pub struct TableInfo { pub name:String, pub rows:u64, pub size_mb:f64, pub engine:String, pub collation:String }
> pub struct RoutineInfo { pub name:String, pub kind:String }  // kind = "PROCEDURE" | "FUNCTION"
> pub struct SchemaDiff { pub source_only:Vec<String>, pub dest_only:Vec<String>, pub in_both:Vec<String> }
> pub enum TransferError { Config(String), Ssh(String), MySql(String), Io(String) }  // thiserror
> pub struct TransferConfig { pub source:ConnectionConfig, pub dest:ConnectionConfig, pub tables:Vec<String>, pub exclude_tables:Vec<String>, pub chunk_size:u32, pub workers:u32, pub drop_existing:bool, pub truncate:bool, pub incremental:bool, pub incremental_column:String, pub include_views:bool, pub include_routines:bool, pub include_triggers:bool, pub dry_run:bool }
> pub struct ConnectionConfig { pub host:String, pub port:u16, pub user:String, pub password:String, pub database:String, pub charset:String, pub ssh:SshConfig }
> pub struct SshConfig { pub enabled:bool, pub host:String, pub port:u16, pub user:String, pub password:Option<String>, pub key_file:Option<String>, pub key_password:Option<String> }
> ```
>
> If any of these names/shapes differ in the delivered Plan 1, reconcile against `transfer-core`'s actual public API before starting — the FFI mirror types and `From` conversions in Task 2 are the single place that adapts to drift.

**Goal:** Expose the Rust `transfer-core` engine to Swift via a UniFFI binding crate packaged as an XCFramework, and build a native SwiftUI macOS app (Connections, Options, Tables, Transfer, Diff) that drives the engine with live progress.

**Architecture:** A new crate `crates/transfer-ffi` defines UniFFI "mirror" types (Records/Enums/Errors) that `From`-convert to/from `transfer-core`'s types, exports async functions plus a foreign `TransferListener` callback interface, and bundles a tokio runtime so Swift just `await`s. A build script produces `MySQLTransfer.xcframework` (universal static lib) consumed by an Xcode SwiftUI app. The app uses MVVM with `@Observable` view models; an `EngineService` calls the FFI off the main thread and a `ListenerBridge` hops every event to the `@MainActor` before touching UI state.

**Tech Stack:** Rust + UniFFI 0.28 (proc-macro mode, `tokio` async runtime feature); `transfer-core` (Plan 1); Swift 5.9+ / SwiftUI / Swift Concurrency; XCTest; Yams (YAML); Xcode 15+.

## Global Constraints

- **macOS-only.** No iOS/Windows/Linux GUI targets. Minimum deployment: macOS 14 (for `@Observable`).
- **SwiftUI + Swift Concurrency** — view models are `@Observable`; all UI-state mutation happens on the `@MainActor`.
- **The Swift `TransferListener` implementation MUST hop to the `@MainActor`** (`Task { @MainActor in … }`) before touching any UI/view-model state. UniFFI delivers callbacks on a worker thread.
- **UniFFI for bindings** (proc-macro mode, no UDL). Bundled async runtime via `#[uniffi::export(async_runtime = "tokio")]`.
- **Yams** for all YAML read/write; the on-disk schema MUST match the CLI's `config.yaml` (spec §6.1) so configs interoperate.
- **Artifact:** `MySQLTransfer.xcframework`. **Rust targets:** `aarch64-apple-darwin` + `x86_64-apple-darwin` (universal via `lipo`).
- **No code-signing, notarization, or App Store** in v1. A local unsigned `.app` is acceptable.
- **UniFFI adaptations (apply everywhere):** UniFFI does not support bare tuples or `f64` field names colliding with Swift keywords — so `test_connection` is re-exposed returning a `ConnectionTestResult` record (Task 4), and all exported types are the `transfer-ffi` mirror types, never `transfer-core`'s types directly.

---

## File Structure

```
mysql-transfer-rs/
  Cargo.toml                              # workspace — add "crates/transfer-ffi" member
  build-xcframework.sh                    # Task 7 — universal lib + bindgen + xcframework
  crates/transfer-ffi/
    Cargo.toml                            # Task 1
    src/lib.rs                            # Tasks 1–6 — uniffi scaffolding, types, exports
    src/types.rs                          # Task 2 — mirror records/enums + From conversions
    src/listener.rs                       # Task 5 — TransferListener trait + sink bridge
    tests/ffi_tests.rs                    # Tasks 4–5 — Rust-side unit tests
  bindings/swift/                         # generated by build-xcframework.sh (Task 7)
    transfer_ffi.swift                    # generated Swift bindings
    MySQLTransfer.xcframework/            # generated artifact
  smoke/
    main.swift                            # Task 6 — links framework, calls testConnection
    run-smoke.sh                          # Task 6
  app/MySQLTransfer.xcodeproj             # Task 8
  app/Sources/
    App/MySQLTransferApp.swift            # Task 8 — @main
    App/RootView.swift                    # Task 8, extended Task 16/17
    Models/ConnectionFormModel.swift      # Task 9
    Models/TransferConfigModel.swift       # Task 9, extended Tasks 12/14
    Models/ConfigMapping.swift             # Task 9 — Swift model <-> FFI record
    Services/EngineService.swift           # Task 10
    Services/ListenerBridge.swift          # Task 15
    Models/TransferProgressModel.swift     # Task 15
    Models/InspectModel.swift              # Task 13
    Models/DiffModel.swift                 # Task 17
    Models/ConfigYaml.swift                # Task 18 — Yams load/save
    Views/ConnectionsView.swift            # Task 11
    Views/OptionsView.swift                # Task 12
    Views/TablesView.swift                 # Task 14
    Views/TransferView.swift               # Task 16
    Views/DiffView.swift                   # Task 17
  app/Tests/
    MySQLTransferTests/...                 # XCTest targets per task
```

---

## PHASE 8 — FFI binding crate + XCFramework

### Task 1: `transfer-ffi` crate skeleton with UniFFI scaffolding

**Files:**
- Modify: `Cargo.toml` (workspace members)
- Create: `crates/transfer-ffi/Cargo.toml`
- Create: `crates/transfer-ffi/src/lib.rs`

**Interfaces:**
- Consumes: workspace `transfer-core` crate (path dependency).
- Produces: a buildable crate `transfer_ffi` with `uniffi::setup_scaffolding!()`; crate-types `lib`, `staticlib`, `cdylib`.

- [ ] **Step 1: Add the crate to the workspace**

Edit `Cargo.toml` (repo root) members list:

```toml
[workspace]
members = ["crates/transfer-core", "crates/transfer-cli", "crates/transfer-ffi"]
resolver = "2"
```

- [ ] **Step 2: Write `crates/transfer-ffi/Cargo.toml`**

```toml
[package]
name = "transfer-ffi"
version = "0.1.0"
edition = "2021"

[lib]
name = "transfer_ffi"
crate-type = ["lib", "staticlib", "cdylib"]

[dependencies]
transfer-core = { path = "../transfer-core" }
uniffi = { version = "0.28", features = ["tokio"] }
tokio = { version = "1", features = ["rt-multi-thread", "sync"] }

[build-dependencies]
uniffi = { version = "0.28", features = ["build"] }

[dev-dependencies]
uniffi = { version = "0.28", features = ["bindgen-tests"] }
```

- [ ] **Step 3: Write the scaffolding entrypoint `crates/transfer-ffi/src/lib.rs`**

```rust
//! UniFFI binding layer over `transfer-core`. Defines mirror types and exported
//! functions; never depends on UI. See plan Task 2+ for the rest.

uniffi::setup_scaffolding!();
```

- [ ] **Step 4: Add the build script `crates/transfer-ffi/build.rs`**

```rust
fn main() {
    uniffi::generate_scaffolding("src/transfer_ffi.udl").ok();
    // proc-macro mode needs no UDL; this is a no-op guard kept for tooling parity.
}
```

Then simplify — proc-macro mode requires no `build.rs` at all. Delete the file you just wrote:

```bash
rm crates/transfer-ffi/build.rs
```

(Listed explicitly so the implementer does not leave a stray UDL build step.)

- [ ] **Step 5: Verify it compiles**

Run: `cargo build -p transfer-ffi`
Expected: `Finished` with no errors; `target/debug/libtransfer_ffi.{a,dylib}` produced.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/transfer-ffi/Cargo.toml crates/transfer-ffi/src/lib.rs
git commit -m "feat(ffi): scaffold transfer-ffi crate with UniFFI"
```

---

### Task 2: Mirror types + `From` conversions

**Files:**
- Create: `crates/transfer-ffi/src/types.rs`
- Modify: `crates/transfer-ffi/src/lib.rs`

**Interfaces:**
- Consumes: `transfer-core` types (`SshConfig`, `ConnectionConfig`, `TransferConfig`, `TransferEvent`, `Level`, `TableStats`, `TransferSummary`, `Inspection`, `TableInfo`, `RoutineInfo`, `SchemaDiff`, `TransferError`).
- Produces: UniFFI mirror types of the same names in `transfer_ffi`, plus `From<core::T> for T` (and `From<ffi::Config> for core::Config` for the inbound config types). Swift sees: structs `SshConfig`/`ConnectionConfig`/`TransferConfig`/`TableStats`/`TransferSummary`/`Inspection`/`TableInfo`/`RoutineInfo`/`SchemaDiff`, enums `TransferEvent`/`Level`, error `TransferError`.

- [ ] **Step 1: Write the failing test** `crates/transfer-ffi/tests/ffi_tests.rs`

```rust
use transfer_ffi::{ConnectionConfig, SshConfig};

#[test]
fn config_roundtrips_to_core_and_back() {
    let ffi = ConnectionConfig {
        host: "db".into(), port: 3306, user: "u".into(), password: "p".into(),
        database: "d".into(), charset: "utf8mb4".into(),
        ssh: SshConfig { enabled: false, host: "".into(), port: 22, user: "".into(),
                         password: None, key_file: None, key_password: None },
    };
    let core: transfer_core::ConnectionConfig = ffi.clone().into();
    let back: ConnectionConfig = core.into();
    assert_eq!(back.host, "db");
    assert_eq!(back.port, 3306);
    assert!(!back.ssh.enabled);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p transfer-ffi config_roundtrips_to_core_and_back`
Expected: FAIL — `ConnectionConfig` not found in `transfer_ffi`.

- [ ] **Step 3: Write `crates/transfer-ffi/src/types.rs`**

```rust
use transfer_core as core;

#[derive(Clone, uniffi::Record)]
pub struct SshConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub key_file: Option<String>,
    pub key_password: Option<String>,
}

#[derive(Clone, uniffi::Record)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub charset: String,
    pub ssh: SshConfig,
}

#[derive(Clone, uniffi::Record)]
pub struct TransferConfig {
    pub source: ConnectionConfig,
    pub dest: ConnectionConfig,
    pub tables: Vec<String>,
    pub exclude_tables: Vec<String>,
    pub chunk_size: u32,
    pub workers: u32,
    pub drop_existing: bool,
    pub truncate: bool,
    pub incremental: bool,
    pub incremental_column: String,
    pub include_views: bool,
    pub include_routines: bool,
    pub include_triggers: bool,
    pub dry_run: bool,
}

#[derive(Clone, uniffi::Enum)]
pub enum Level { Info, Warn, DryRun }

#[derive(Clone, uniffi::Record)]
pub struct TableStats {
    pub table: String,
    pub rows_transferred: u64,
    pub chunks: u64,
    pub error: Option<String>,
}

#[derive(Clone, uniffi::Record)]
pub struct TransferSummary {
    pub stats: Vec<TableStats>,
    pub total_rows: u64,
    pub errors: u64,
    pub elapsed_secs: f64,
    pub rows_per_sec: f64,
}

#[derive(Clone, uniffi::Record)]
pub struct TableInfo {
    pub name: String,
    pub rows: u64,
    pub size_mb: f64,
    pub engine: String,
    pub collation: String,
}

#[derive(Clone, uniffi::Record)]
pub struct RoutineInfo { pub name: String, pub kind: String }

#[derive(Clone, uniffi::Record)]
pub struct Inspection {
    pub tables: Vec<TableInfo>,
    pub views: Vec<String>,
    pub routines: Vec<RoutineInfo>,
    pub triggers: Vec<String>,
}

#[derive(Clone, uniffi::Record)]
pub struct SchemaDiff {
    pub source_only: Vec<String>,
    pub dest_only: Vec<String>,
    pub in_both: Vec<String>,
}

#[derive(Clone, uniffi::Enum)]
pub enum TransferEvent {
    Phase { name: String },
    TableCounted { table: String, rows: u64 },
    TableStarted { table: String, total_rows: u64 },
    ChunkInserted { table: String, rows: u64 },
    TableCompleted { stats: TableStats },
    Log { level: Level, message: String },
    Summary { summary: TransferSummary },
}

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum TransferError {
    #[error("config: {0}")] Config(String),
    #[error("ssh: {0}")] Ssh(String),
    #[error("mysql: {0}")] MySql(String),
    #[error("io: {0}")] Io(String),
}

// ---- conversions: ffi -> core (inbound config) ----
impl From<SshConfig> for core::SshConfig {
    fn from(s: SshConfig) -> Self {
        core::SshConfig { enabled: s.enabled, host: s.host, port: s.port, user: s.user,
            password: s.password, key_file: s.key_file, key_password: s.key_password }
    }
}
impl From<ConnectionConfig> for core::ConnectionConfig {
    fn from(c: ConnectionConfig) -> Self {
        core::ConnectionConfig { host: c.host, port: c.port, user: c.user, password: c.password,
            database: c.database, charset: c.charset, ssh: c.ssh.into() }
    }
}
impl From<TransferConfig> for core::TransferConfig {
    fn from(c: TransferConfig) -> Self {
        core::TransferConfig { source: c.source.into(), dest: c.dest.into(),
            tables: c.tables, exclude_tables: c.exclude_tables, chunk_size: c.chunk_size,
            workers: c.workers, drop_existing: c.drop_existing, truncate: c.truncate,
            incremental: c.incremental, incremental_column: c.incremental_column,
            include_views: c.include_views, include_routines: c.include_routines,
            include_triggers: c.include_triggers, dry_run: c.dry_run }
    }
}

// ---- conversions: core -> ffi (outbound results/events) ----
impl From<core::SshConfig> for SshConfig {
    fn from(s: core::SshConfig) -> Self {
        SshConfig { enabled: s.enabled, host: s.host, port: s.port, user: s.user,
            password: s.password, key_file: s.key_file, key_password: s.key_password }
    }
}
impl From<core::ConnectionConfig> for ConnectionConfig {
    fn from(c: core::ConnectionConfig) -> Self {
        ConnectionConfig { host: c.host, port: c.port, user: c.user, password: c.password,
            database: c.database, charset: c.charset, ssh: c.ssh.into() }
    }
}
impl From<core::Level> for Level {
    fn from(l: core::Level) -> Self {
        match l { core::Level::Info => Level::Info, core::Level::Warn => Level::Warn,
                  core::Level::DryRun => Level::DryRun }
    }
}
impl From<core::TableStats> for TableStats {
    fn from(s: core::TableStats) -> Self {
        TableStats { table: s.table, rows_transferred: s.rows_transferred,
            chunks: s.chunks, error: s.error }
    }
}
impl From<core::TransferSummary> for TransferSummary {
    fn from(s: core::TransferSummary) -> Self {
        TransferSummary { stats: s.stats.into_iter().map(Into::into).collect(),
            total_rows: s.total_rows, errors: s.errors, elapsed_secs: s.elapsed_secs,
            rows_per_sec: s.rows_per_sec }
    }
}
impl From<core::TableInfo> for TableInfo {
    fn from(t: core::TableInfo) -> Self {
        TableInfo { name: t.name, rows: t.rows, size_mb: t.size_mb,
            engine: t.engine, collation: t.collation }
    }
}
impl From<core::RoutineInfo> for RoutineInfo {
    fn from(r: core::RoutineInfo) -> Self { RoutineInfo { name: r.name, kind: r.kind } }
}
impl From<core::Inspection> for Inspection {
    fn from(i: core::Inspection) -> Self {
        Inspection { tables: i.tables.into_iter().map(Into::into).collect(),
            views: i.views, routines: i.routines.into_iter().map(Into::into).collect(),
            triggers: i.triggers }
    }
}
impl From<core::SchemaDiff> for SchemaDiff {
    fn from(d: core::SchemaDiff) -> Self {
        SchemaDiff { source_only: d.source_only, dest_only: d.dest_only, in_both: d.in_both }
    }
}
impl From<core::TransferEvent> for TransferEvent {
    fn from(e: core::TransferEvent) -> Self {
        match e {
            core::TransferEvent::Phase(name) => TransferEvent::Phase { name },
            core::TransferEvent::TableCounted { table, rows } => TransferEvent::TableCounted { table, rows },
            core::TransferEvent::TableStarted { table, total_rows } => TransferEvent::TableStarted { table, total_rows },
            core::TransferEvent::ChunkInserted { table, rows } => TransferEvent::ChunkInserted { table, rows },
            core::TransferEvent::TableCompleted(s) => TransferEvent::TableCompleted { stats: s.into() },
            core::TransferEvent::Log { level, message } => TransferEvent::Log { level: level.into(), message },
            core::TransferEvent::Summary(s) => TransferEvent::Summary { summary: s.into() },
        }
    }
}
impl From<core::TransferError> for TransferError {
    fn from(e: core::TransferError) -> Self {
        match e {
            core::TransferError::Config(m) => TransferError::Config(m),
            core::TransferError::Ssh(m) => TransferError::Ssh(m),
            core::TransferError::MySql(m) => TransferError::MySql(m),
            core::TransferError::Io(m) => TransferError::Io(m),
        }
    }
}
```

- [ ] **Step 4: Wire the module into `lib.rs`**

```rust
//! UniFFI binding layer over `transfer-core`.

uniffi::setup_scaffolding!();

mod types;
pub use types::*;
```

Add `thiserror = "1"` to `[dependencies]` in `crates/transfer-ffi/Cargo.toml`.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p transfer-ffi config_roundtrips_to_core_and_back`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/transfer-ffi
git commit -m "feat(ffi): mirror types and core<->ffi conversions"
```

---

### Task 3: `ConnectionTestResult` + exported `test_connection`

**Files:**
- Modify: `crates/transfer-ffi/src/types.rs`
- Modify: `crates/transfer-ffi/src/lib.rs`
- Modify: `crates/transfer-ffi/tests/ffi_tests.rs`

**Interfaces:**
- Produces: `pub struct ConnectionTestResult { success: bool, message: String }` (uniffi Record) and `#[uniffi::export] pub fn test_connection(cfg: ConnectionConfig) -> ConnectionTestResult`. Swift sees `func testConnection(cfg: ConnectionConfig) -> ConnectionTestResult`.

- [ ] **Step 1: Write the failing test** (append to `tests/ffi_tests.rs`)

```rust
#[test]
fn test_connection_returns_result_for_bad_host() {
    let cfg = transfer_ffi::ConnectionConfig {
        host: "127.0.0.1".into(), port: 1, user: "x".into(), password: "x".into(),
        database: "x".into(), charset: "utf8mb4".into(),
        ssh: transfer_ffi::SshConfig { enabled: false, host: "".into(), port: 22,
            user: "".into(), password: None, key_file: None, key_password: None },
    };
    let r = transfer_ffi::test_connection(cfg);
    assert!(!r.success);
    assert!(!r.message.is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p transfer-ffi test_connection_returns_result_for_bad_host`
Expected: FAIL — `test_connection` not found.

- [ ] **Step 3: Add the Record to `types.rs`**

```rust
#[derive(Clone, uniffi::Record)]
pub struct ConnectionTestResult { pub success: bool, pub message: String }
```

- [ ] **Step 4: Add the export to `lib.rs`**

```rust
use transfer_core as core;

#[uniffi::export]
pub fn test_connection(cfg: ConnectionConfig) -> ConnectionTestResult {
    let core_cfg: core::ConnectionConfig = cfg.into();
    let (success, message) = core::test_connection(&core_cfg);
    ConnectionTestResult { success, message }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p transfer-ffi test_connection_returns_result_for_bad_host`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/transfer-ffi
git commit -m "feat(ffi): export test_connection returning ConnectionTestResult"
```

---

### Task 4: `TransferListener` callback interface + sink bridge

**Files:**
- Create: `crates/transfer-ffi/src/listener.rs`
- Modify: `crates/transfer-ffi/src/lib.rs`
- Modify: `crates/transfer-ffi/tests/ffi_tests.rs`

**Interfaces:**
- Produces: `#[uniffi::export(with_foreign)] pub trait TransferListener: Send + Sync { fn on_event(&self, event: TransferEvent); }` and a helper `spawn_event_bridge(listener: Arc<dyn TransferListener>) -> (EventSink, JoinHandle<()>)` that creates an mpsc channel, returns a `core::EventSink` over its sender, and spawns a task draining the receiver and calling `listener.on_event(core_event.into())`. Swift sees `protocol TransferListener: AnyObject { func onEvent(event: TransferEvent) }`.

- [ ] **Step 1: Write the failing test** (append to `tests/ffi_tests.rs`)

```rust
use std::sync::{Arc, Mutex};
use transfer_ffi::{TransferEvent, TransferListener};

struct Collector(Arc<Mutex<Vec<String>>>);
impl TransferListener for Collector {
    fn on_event(&self, event: TransferEvent) {
        if let TransferEvent::Phase { name } = event { self.0.lock().unwrap().push(name); }
    }
}

#[tokio::test]
async fn bridge_forwards_core_events_to_listener() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let listener: Arc<dyn TransferListener> = Arc::new(Collector(seen.clone()));
    let (sink, handle) = transfer_ffi::spawn_event_bridge(listener);
    sink.send(transfer_core::TransferEvent::Phase("Connecting".into())).await.unwrap();
    drop(sink);                       // close channel so the bridge task ends
    handle.await.unwrap();
    assert_eq!(seen.lock().unwrap().as_slice(), &["Connecting".to_string()]);
}
```

(Assumes `core::EventSink` has `async fn send(&self, ev: TransferEvent) -> Result<(), _>`. If Plan 1 named it differently, adjust this one call site.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p transfer-ffi bridge_forwards_core_events_to_listener`
Expected: FAIL — `TransferListener` / `spawn_event_bridge` not found.

- [ ] **Step 3: Write `crates/transfer-ffi/src/listener.rs`**

```rust
use std::sync::Arc;
use tokio::task::JoinHandle;
use transfer_core as core;
use crate::TransferEvent;

#[uniffi::export(with_foreign)]
pub trait TransferListener: Send + Sync {
    fn on_event(&self, event: TransferEvent);
}

/// Create a core EventSink whose events are forwarded to a foreign listener.
/// Returns the sink (give to core) and the bridge task handle (await after the run).
pub fn spawn_event_bridge(
    listener: Arc<dyn TransferListener>,
) -> (core::EventSink, JoinHandle<()>) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<core::TransferEvent>(256);
    let handle = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            listener.on_event(TransferEvent::from(ev));
        }
    });
    (core::EventSink::new(tx), handle)
}
```

- [ ] **Step 4: Wire into `lib.rs`**

```rust
mod listener;
pub use listener::*;
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p transfer-ffi bridge_forwards_core_events_to_listener`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/transfer-ffi
git commit -m "feat(ffi): TransferListener callback interface and event bridge"
```

---

### Task 5: Exported async `run_transfer` / `run_inspect` / `run_diff`

**Files:**
- Modify: `crates/transfer-ffi/src/lib.rs`
- Modify: `crates/transfer-ffi/tests/ffi_tests.rs`

**Interfaces:**
- Produces (Swift sees these):
  - `func runTransfer(cfg: TransferConfig, schema: Bool, data: Bool, listener: TransferListener) async throws -> TransferSummary`
  - `func runInspect(cfg: TransferConfig) async throws -> Inspection`
  - `func runDiff(cfg: TransferConfig) async throws -> SchemaDiff`

- [ ] **Step 1: Write the failing test** (append to `tests/ffi_tests.rs`)

```rust
#[tokio::test]
async fn run_inspect_errors_cleanly_on_unreachable_host() {
    let conn = transfer_ffi::ConnectionConfig {
        host: "127.0.0.1".into(), port: 1, user: "x".into(), password: "x".into(),
        database: "x".into(), charset: "utf8mb4".into(),
        ssh: transfer_ffi::SshConfig { enabled: false, host: "".into(), port: 22,
            user: "".into(), password: None, key_file: None, key_password: None },
    };
    let cfg = transfer_ffi::TransferConfig {
        source: conn.clone(), dest: conn, tables: vec![], exclude_tables: vec![],
        chunk_size: 10000, workers: 4, drop_existing: false, truncate: false,
        incremental: false, incremental_column: "".into(), include_views: false,
        include_routines: false, include_triggers: false, dry_run: false,
    };
    let err = transfer_ffi::run_inspect(cfg).await.unwrap_err();
    matches!(err, transfer_ffi::TransferError::MySql(_) | transfer_ffi::TransferError::Io(_));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p transfer-ffi run_inspect_errors_cleanly_on_unreachable_host`
Expected: FAIL — `run_inspect` not found.

- [ ] **Step 3: Add exports to `lib.rs`**

```rust
use std::sync::Arc;

#[uniffi::export(async_runtime = "tokio")]
pub async fn run_transfer(
    cfg: TransferConfig,
    schema: bool,
    data: bool,
    listener: Arc<dyn TransferListener>,
) -> Result<TransferSummary, TransferError> {
    let core_cfg: core::TransferConfig = cfg.into();
    let (sink, bridge) = spawn_event_bridge(listener);
    let result = core::run_transfer(&core_cfg, schema, data, &sink).await;
    drop(sink);            // close channel so the bridge drains and exits
    let _ = bridge.await;  // ensure all events delivered before returning
    result.map(Into::into).map_err(Into::into)
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn run_inspect(cfg: TransferConfig) -> Result<Inspection, TransferError> {
    let core_cfg: core::TransferConfig = cfg.into();
    core::run_inspect(&core_cfg).await.map(Into::into).map_err(Into::into)
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn run_diff(cfg: TransferConfig) -> Result<SchemaDiff, TransferError> {
    let core_cfg: core::TransferConfig = cfg.into();
    core::run_diff(&core_cfg).await.map(Into::into).map_err(Into::into)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p transfer-ffi run_inspect_errors_cleanly_on_unreachable_host`
Expected: PASS (the connect fails fast and is mapped to a `TransferError`).

- [ ] **Step 5: Run the whole crate's tests**

Run: `cargo test -p transfer-ffi`
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/transfer-ffi
git commit -m "feat(ffi): export async run_transfer/run_inspect/run_diff"
```

---

### Task 6: Swift smoke test (links the lib, calls `testConnection`)

**Files:**
- Create: `smoke/main.swift`
- Create: `smoke/run-smoke.sh`

**Interfaces:**
- Consumes: the generated `bindings/swift/transfer_ffi.swift` + the universal static lib (produced inline by the script). Validates the Swift seam end-to-end before the Xcode app exists.

- [ ] **Step 1: Write `smoke/main.swift`**

```swift
import Foundation

let cfg = ConnectionConfig(
    host: "127.0.0.1", port: 1, user: "x", password: "x",
    database: "x", charset: "utf8mb4",
    ssh: SshConfig(enabled: false, host: "", port: 22, user: "",
                   password: nil, keyFile: nil, keyPassword: nil)
)
let result = testConnection(cfg: cfg)
print("smoke: success=\(result.success) message=\(result.message)")
precondition(result.success == false, "expected failure connecting to 127.0.0.1:1")
print("SMOKE OK")
```

- [ ] **Step 2: Write `smoke/run-smoke.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# 1. Build a host cdylib so uniffi-bindgen can read the metadata.
cargo build -p transfer-ffi
DYLIB="target/debug/libtransfer_ffi.dylib"

# 2. Generate Swift bindings (transfer_ffi.swift, transfer_ffiFFI.h, modulemap).
mkdir -p bindings/swift
cargo run -p transfer-ffi --bin uniffi-bindgen -- \
  generate --library "$DYLIB" --language swift --out-dir bindings/swift \
  || cargo run --features=uniffi/cli --bin uniffi-bindgen -- \
       generate --library "$DYLIB" --language swift --out-dir bindings/swift

# 3. Build a host static lib for linking the smoke binary.
cargo build -p transfer-ffi
LIB="target/debug/libtransfer_ffi.a"

# 4. Compile the Swift smoke binary against the generated module + static lib.
swiftc \
  -I bindings/swift \
  -Xcc -fmodule-map-file=bindings/swift/transfer_ffiFFI.modulemap \
  bindings/swift/transfer_ffi.swift smoke/main.swift \
  -L target/debug -ltransfer_ffi -o target/smoke_test

# 5. Run it.
./target/smoke_test
```

Make it executable: `chmod +x smoke/run-smoke.sh`.

> Note: the `uniffi-bindgen` binary is provided by adding a tiny bin to the crate. If `cargo run -p transfer-ffi --bin uniffi-bindgen` fails because no such bin exists, create `crates/transfer-ffi/src/bin/uniffi-bindgen.rs` containing exactly `fn main() { uniffi::uniffi_bindgen_main() }` and add `uniffi = { version = "0.28", features = ["cli"] }` to dependencies, then re-run. Commit that bin with this task.

- [ ] **Step 3: Create the bindgen bin (so step 2 resolves)**

Create `crates/transfer-ffi/src/bin/uniffi-bindgen.rs`:

```rust
fn main() {
    uniffi::uniffi_bindgen_main()
}
```

Add to `crates/transfer-ffi/Cargo.toml` dependencies: change the `uniffi` line to
`uniffi = { version = "0.28", features = ["tokio", "cli"] }`.

- [ ] **Step 4: Run the smoke test**

Run: `./smoke/run-smoke.sh`
Expected: prints `smoke: success=false message=...` then `SMOKE OK` and exits 0.

- [ ] **Step 5: Commit**

```bash
git add smoke crates/transfer-ffi/src/bin/uniffi-bindgen.rs crates/transfer-ffi/Cargo.toml
git commit -m "test(ffi): Swift smoke test linking the binding and calling testConnection"
```

---

### Task 7: `build-xcframework.sh` — universal lib → `MySQLTransfer.xcframework`

**Files:**
- Create: `build-xcframework.sh` (repo root)

**Interfaces:**
- Produces: `bindings/swift/transfer_ffi.swift` (added to the app target) and `bindings/swift/MySQLTransfer.xcframework` (a static-library xcframework with the FFI header + modulemap), consumed by Task 8.

- [ ] **Step 1: Write `build-xcframework.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

OUT="bindings/swift"
XCF="$OUT/MySQLTransfer.xcframework"
mkdir -p "$OUT"

rustup target add aarch64-apple-darwin x86_64-apple-darwin

# 1. Release static libs for both arches.
cargo build -p transfer-ffi --release --target aarch64-apple-darwin
cargo build -p transfer-ffi --release --target x86_64-apple-darwin

# 2. Universal static lib via lipo.
mkdir -p target/universal-macos/release
lipo -create \
  target/aarch64-apple-darwin/release/libtransfer_ffi.a \
  target/x86_64-apple-darwin/release/libtransfer_ffi.a \
  -output target/universal-macos/release/libtransfer_ffi.a

# 3. Generate Swift bindings + headers from a release dylib.
cargo build -p transfer-ffi --release
cargo run -p transfer-ffi --bin uniffi-bindgen -- \
  generate --library target/release/libtransfer_ffi.dylib \
  --language swift --out-dir "$OUT"

# 4. Assemble a headers dir (header + modulemap) for the xcframework.
HEADERS="$OUT/headers"
rm -rf "$HEADERS"; mkdir -p "$HEADERS"
cp "$OUT/transfer_ffiFFI.h" "$HEADERS/"
# UniFFI emits a modulemap named transfer_ffiFFI.modulemap; xcframework expects module.modulemap
cp "$OUT/transfer_ffiFFI.modulemap" "$HEADERS/module.modulemap"

# 5. Build the xcframework.
rm -rf "$XCF"
xcodebuild -create-xcframework \
  -library target/universal-macos/release/libtransfer_ffi.a \
  -headers "$HEADERS" \
  -output "$XCF"

echo "Built $XCF"
echo "Swift bindings: $OUT/transfer_ffi.swift"
```

Make executable: `chmod +x build-xcframework.sh`.

- [ ] **Step 2: Run it**

Run: `./build-xcframework.sh`
Expected: ends with `Built bindings/swift/MySQLTransfer.xcframework`; the `.xcframework` directory exists and contains a `macos-arm64_x86_64/` slice.

- [ ] **Step 3: Verify the artifact**

Run: `ls bindings/swift/MySQLTransfer.xcframework && lipo -info target/universal-macos/release/libtransfer_ffi.a`
Expected: lists `Info.plist` + a `macos-*` slice; `lipo -info` reports `arm64 x86_64`.

- [ ] **Step 4: Commit**

```bash
git add build-xcframework.sh
echo "bindings/swift/MySQLTransfer.xcframework/" >> .gitignore
echo "bindings/swift/headers/" >> .gitignore
git add .gitignore
git commit -m "build(ffi): build-xcframework.sh producing MySQLTransfer.xcframework"
```

---

## PHASE 9 — SwiftUI app skeleton, EngineService, Connections, Options

### Task 8: Xcode app skeleton linking the XCFramework

**Files:**
- Create: `app/MySQLTransfer.xcodeproj` (+ targets `MySQLTransfer`, `MySQLTransferTests`)
- Create: `app/Sources/App/MySQLTransferApp.swift`
- Create: `app/Sources/App/RootView.swift`
- Add: `bindings/swift/transfer_ffi.swift` and `MySQLTransfer.xcframework` to the app target.

**Interfaces:**
- Consumes: Task 7 artifacts.
- Produces: a launchable empty-window macOS app target named `MySQLTransfer` (macOS 14 deployment) and a unit-test target `MySQLTransferTests`.

- [ ] **Step 1: Create the Xcode project**

In Xcode: File ▸ New ▸ Project ▸ macOS ▸ App. Product Name `MySQLTransfer`, Interface SwiftUI, Language Swift, include Tests. Save into `app/`. Set Deployment Target macOS 14.0. Set "User Script Sandboxing" off (so the framework links).

- [ ] **Step 2: Link the framework + bindings**

- Drag `bindings/swift/MySQLTransfer.xcframework` into the project; in the `MySQLTransfer` target ▸ General ▸ Frameworks, Libraries, set it to "Embed & Sign" → change to "Do Not Embed" (static lib).
- Add `bindings/swift/transfer_ffi.swift` to the `MySQLTransfer` target's Compile Sources.
- In Build Settings, add to "Import Paths"/"Header Search Paths" nothing extra (the xcframework provides the module). Confirm `import` of the generated module works (the generated file declares everything in-module).

- [ ] **Step 3: Write `app/Sources/App/MySQLTransferApp.swift`**

```swift
import SwiftUI

@main
struct MySQLTransferApp: App {
    var body: some Scene {
        WindowGroup("MySQL Transfer") {
            RootView()
        }
        .defaultSize(width: 900, height: 640)
    }
}
```

- [ ] **Step 4: Write `app/Sources/App/RootView.swift`**

```swift
import SwiftUI

struct RootView: View {
    var body: some View {
        NavigationSplitView {
            List {
                Label("Connections", systemImage: "network")
                Label("Options", systemImage: "slider.horizontal.3")
                Label("Tables", systemImage: "tablecells")
                Label("Transfer", systemImage: "arrow.left.arrow.right")
                Label("Diff", systemImage: "doc.on.doc")
            }
            .navigationTitle("MySQL Transfer")
        } detail: {
            Text("Select a section")
                .foregroundStyle(.secondary)
        }
    }
}
```

- [ ] **Step 5: Build & verify (manual checklist)**

Run: `xcodebuild -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' build`
Expected: `BUILD SUCCEEDED`.

Manual verification:
1. Open the project in Xcode and Run (⌘R).
2. A window titled "MySQL Transfer" appears with a sidebar listing the five sections.
3. No console errors about missing symbols (confirms the xcframework linked).

- [ ] **Step 6: Commit**

```bash
git add app
git commit -m "feat(app): SwiftUI skeleton linking MySQLTransfer.xcframework"
```

---

### Task 9: Config view models + Swift⇄FFI mapping

**Files:**
- Create: `app/Sources/Models/ConnectionFormModel.swift`
- Create: `app/Sources/Models/TransferConfigModel.swift`
- Create: `app/Sources/Models/ConfigMapping.swift`
- Create: `app/Tests/MySQLTransferTests/ConfigMappingTests.swift`

**Interfaces:**
- Produces:
  - `@Observable final class ConnectionFormModel` — string-backed editable fields (`host`, `portText`, `user`, `password`, `database`, `charset`, `sshEnabled`, `sshHost`, `sshPortText`, `sshUser`, `sshPassword`, `sshKeyFile`) + `func toFFI() -> ConnectionConfig`.
  - `@Observable final class TransferConfigModel` — `source`/`dest: ConnectionFormModel`, plus options (`chunkSizeText`, `workersText`, `dropExisting`, `truncate`, `incremental`, `incrementalColumn`, `includeViews`, `includeRoutines`, `includeTriggers`, `dryRun`), `selectedTables: Set<String>`, and `func toFFI() -> TransferConfig`.
  - Free funcs in `ConfigMapping.swift`: `func makeConnection(_ m: ConnectionFormModel) -> ConnectionConfig`.

- [ ] **Step 1: Write the failing test** `app/Tests/MySQLTransferTests/ConfigMappingTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

final class ConfigMappingTests: XCTestCase {
    func testConnectionFormMapsToFFI() {
        let m = ConnectionFormModel()
        m.host = "db.example.com"; m.portText = "3306"; m.user = "u"
        m.password = "p"; m.database = "shop"; m.charset = "utf8mb4"
        m.sshEnabled = true; m.sshHost = "bastion"; m.sshPortText = "22"
        m.sshUser = "deploy"; m.sshKeyFile = "/k.pem"
        let c = m.toFFI()
        XCTAssertEqual(c.host, "db.example.com")
        XCTAssertEqual(c.port, 3306)
        XCTAssertEqual(c.database, "shop")
        XCTAssertTrue(c.ssh.enabled)
        XCTAssertEqual(c.ssh.keyFile, "/k.pem")
        XCTAssertNil(c.ssh.password)         // empty string -> nil
    }

    func testInvalidPortFallsBackToDefault() {
        let m = ConnectionFormModel()
        m.portText = "not-a-number"
        XCTAssertEqual(m.toFFI().port, 3306) // default when unparseable
    }

    func testTransferConfigMapsOptions() {
        let t = TransferConfigModel()
        t.chunkSizeText = "5000"; t.workersText = "8"; t.dryRun = true
        t.selectedTables = ["a", "b"]
        let cfg = t.toFFI()
        XCTAssertEqual(cfg.chunkSize, 5000)
        XCTAssertEqual(cfg.workers, 8)
        XCTAssertTrue(cfg.dryRun)
        XCTAssertEqual(Set(cfg.tables), ["a", "b"])
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/ConfigMappingTests`
Expected: FAIL — `ConnectionFormModel` not found.

- [ ] **Step 3: Write `app/Sources/Models/ConnectionFormModel.swift`**

```swift
import Foundation

@Observable
final class ConnectionFormModel: Identifiable {
    let id = UUID()
    var host = ""
    var portText = "3306"
    var user = ""
    var password = ""
    var database = ""
    var charset = "utf8mb4"
    var sshEnabled = false
    var sshHost = ""
    var sshPortText = "22"
    var sshUser = ""
    var sshPassword = ""
    var sshKeyFile = ""

    func toFFI() -> ConnectionConfig {
        ConnectionConfig(
            host: host,
            port: UInt16(portText) ?? 3306,
            user: user,
            password: password,
            database: database,
            charset: charset.isEmpty ? "utf8mb4" : charset,
            ssh: SshConfig(
                enabled: sshEnabled,
                host: sshHost,
                port: UInt16(sshPortText) ?? 22,
                user: sshUser,
                password: sshPassword.isEmpty ? nil : sshPassword,
                keyFile: sshKeyFile.isEmpty ? nil : sshKeyFile,
                keyPassword: nil
            )
        )
    }
}
```

- [ ] **Step 4: Write `app/Sources/Models/TransferConfigModel.swift`**

```swift
import Foundation

@Observable
final class TransferConfigModel {
    var source = ConnectionFormModel()
    var dest = ConnectionFormModel()

    var chunkSizeText = "10000"
    var workersText = "4"
    var dropExisting = false
    var truncate = false
    var incremental = false
    var incrementalColumn = ""
    var includeViews = false
    var includeRoutines = false
    var includeTriggers = false
    var dryRun = false

    var selectedTables: Set<String> = []
    var excludeTables: [String] = []

    func toFFI() -> TransferConfig {
        TransferConfig(
            source: source.toFFI(),
            dest: dest.toFFI(),
            tables: Array(selectedTables).sorted(),
            excludeTables: excludeTables,
            chunkSize: UInt32(chunkSizeText) ?? 10000,
            workers: UInt32(workersText) ?? 4,
            dropExisting: dropExisting,
            truncate: truncate,
            incremental: incremental,
            incrementalColumn: incrementalColumn,
            includeViews: includeViews,
            includeRoutines: includeRoutines,
            includeTriggers: includeTriggers,
            dryRun: dryRun
        )
    }
}
```

- [ ] **Step 5: Write `app/Sources/Models/ConfigMapping.swift`**

```swift
import Foundation

/// Convenience for code paths that have a form but want the FFI record directly.
func makeConnection(_ m: ConnectionFormModel) -> ConnectionConfig { m.toFFI() }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/ConfigMappingTests`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add app/Sources/Models app/Tests
git commit -m "feat(app): config view models with Swift<->FFI mapping"
```

---

### Task 10: `EngineService` (off-main FFI calls)

**Files:**
- Create: `app/Sources/Services/EngineService.swift`
- Create: `app/Tests/MySQLTransferTests/EngineServiceTests.swift`

**Interfaces:**
- Produces:
  ```swift
  final class EngineService {
      func test(_ cfg: ConnectionConfig) async -> ConnectionTestResult
      func inspect(_ cfg: TransferConfig) async throws -> Inspection
      func diff(_ cfg: TransferConfig) async throws -> SchemaDiff
      func transfer(_ cfg: TransferConfig, schema: Bool, data: Bool, listener: TransferListener) async throws -> TransferSummary
  }
  ```
  Each method `await`s the FFI async function (which runs on the bundled tokio runtime, off the Swift main thread). `test` never throws — the FFI `testConnection` is non-throwing.

- [ ] **Step 1: Write the failing test** `app/Tests/MySQLTransferTests/EngineServiceTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

final class EngineServiceTests: XCTestCase {
    func testTestConnectionFailsForUnreachableHost() async {
        let svc = EngineService()
        let cfg = ConnectionConfig(host: "127.0.0.1", port: 1, user: "x", password: "x",
                                   database: "x", charset: "utf8mb4",
                                   ssh: SshConfig(enabled: false, host: "", port: 22, user: "",
                                                  password: nil, keyFile: nil, keyPassword: nil))
        let r = await svc.test(cfg)
        XCTAssertFalse(r.success)
        XCTAssertFalse(r.message.isEmpty)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/EngineServiceTests`
Expected: FAIL — `EngineService` not found.

- [ ] **Step 3: Write `app/Sources/Services/EngineService.swift`**

```swift
import Foundation

/// Thin wrapper over the UniFFI-generated free functions. The FFI functions are
/// async and run on the crate's bundled tokio runtime, so calls do not block the
/// Swift main thread. Callbacks (TransferListener) arrive on a worker thread.
final class EngineService {
    func test(_ cfg: ConnectionConfig) async -> ConnectionTestResult {
        testConnection(cfg: cfg)
    }

    func inspect(_ cfg: TransferConfig) async throws -> Inspection {
        try await runInspect(cfg: cfg)
    }

    func diff(_ cfg: TransferConfig) async throws -> SchemaDiff {
        try await runDiff(cfg: cfg)
    }

    func transfer(_ cfg: TransferConfig, schema: Bool, data: Bool,
                  listener: TransferListener) async throws -> TransferSummary {
        try await runTransfer(cfg: cfg, schema: schema, data: data, listener: listener)
    }
}
```

> If `testConnection` is generated as non-`async`, the `await` is harmless (Swift accepts `await` on a sync call inside an async context only if it's actually async — if the compiler objects, drop the `async` round-trip and call `testConnection(cfg:)` directly inside the `async` method body, which is what is shown).

- [ ] **Step 4: Run test to verify it passes**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/EngineServiceTests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/Sources/Services app/Tests
git commit -m "feat(app): EngineService wrapping the FFI functions"
```

---

### Task 11: Connections screen + Test Connection

**Files:**
- Create: `app/Sources/Views/ConnectionsView.swift`
- Modify: `app/Sources/App/RootView.swift`
- Create: `app/Tests/MySQLTransferTests/ConnectionsViewModelTests.swift`

**Interfaces:**
- Produces:
  - `@Observable final class ConnectionsViewModel` with `var config: TransferConfigModel`, `var sourceStatus: String?`, `var destStatus: String?`, `var testing: Bool`, and `func testSource() async` / `func testDest() async` (call `EngineService.test`, set `*Status` on the `@MainActor`).
  - `struct ConnectionsView: View` rendering source + dest forms bound to `config.source` / `config.dest` with two "Test Connection" buttons.

- [ ] **Step 1: Write the failing test** `app/Tests/MySQLTransferTests/ConnectionsViewModelTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

@MainActor
final class ConnectionsViewModelTests: XCTestCase {
    func testTestSourceSetsStatusMessage() async {
        let cfgModel = TransferConfigModel()
        cfgModel.source.host = "127.0.0.1"; cfgModel.source.portText = "1"
        cfgModel.source.user = "x"; cfgModel.source.database = "x"
        let vm = ConnectionsViewModel(config: cfgModel, engine: EngineService())
        await vm.testSource()
        XCTAssertNotNil(vm.sourceStatus)
        XCTAssertFalse(vm.testing)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/ConnectionsViewModelTests`
Expected: FAIL — `ConnectionsViewModel` not found.

- [ ] **Step 3: Write `app/Sources/Views/ConnectionsView.swift`**

```swift
import SwiftUI

@Observable
@MainActor
final class ConnectionsViewModel {
    var config: TransferConfigModel
    private let engine: EngineService
    var sourceStatus: String?
    var destStatus: String?
    var testing = false

    init(config: TransferConfigModel, engine: EngineService) {
        self.config = config
        self.engine = engine
    }

    func testSource() async { await test(config.source) { self.sourceStatus = $0 } }
    func testDest() async { await test(config.dest) { self.destStatus = $0 } }

    private func test(_ form: ConnectionFormModel, set: @escaping (String) -> Void) async {
        testing = true
        defer { testing = false }
        let result = await engine.test(form.toFFI())
        set((result.success ? "✓ " : "✗ ") + result.message)
    }
}

struct ConnectionsView: View {
    @Bindable var vm: ConnectionsViewModel

    var body: some View {
        Form {
            connectionSection("Source", form: vm.config.source,
                              status: vm.sourceStatus) { await vm.testSource() }
            connectionSection("Destination", form: vm.config.dest,
                              status: vm.destStatus) { await vm.testDest() }
        }
        .formStyle(.grouped)
        .disabled(vm.testing)
        .navigationTitle("Connections")
    }

    @ViewBuilder
    private func connectionSection(_ title: String, form: ConnectionFormModel,
                                   status: String?, test: @escaping () async -> Void) -> some View {
        @Bindable var form = form
        Section(title) {
            TextField("Host", text: $form.host)
            TextField("Port", text: $form.portText)
            TextField("User", text: $form.user)
            SecureField("Password", text: $form.password)
            TextField("Database", text: $form.database)
            TextField("Charset", text: $form.charset)
            Toggle("Use SSH tunnel", isOn: $form.sshEnabled)
            if form.sshEnabled {
                TextField("SSH Host", text: $form.sshHost)
                TextField("SSH Port", text: $form.sshPortText)
                TextField("SSH User", text: $form.sshUser)
                SecureField("SSH Password", text: $form.sshPassword)
                TextField("SSH Key File", text: $form.sshKeyFile)
            }
            HStack {
                Button("Test Connection") { Task { await test() } }
                if let status { Text(status).font(.callout).foregroundStyle(.secondary) }
            }
        }
    }
}
```

- [ ] **Step 4: Wire into `RootView.swift`**

Replace `RootView` body with a selection-driven split view:

```swift
import SwiftUI

enum Section: String, CaseIterable, Identifiable {
    case connections = "Connections", options = "Options", tables = "Tables",
         transfer = "Transfer", diff = "Diff"
    var id: String { rawValue }
    var icon: String {
        switch self {
        case .connections: "network"; case .options: "slider.horizontal.3"
        case .tables: "tablecells"; case .transfer: "arrow.left.arrow.right"
        case .diff: "doc.on.doc"
        }
    }
}

struct RootView: View {
    @State private var selection: Section? = .connections
    @State private var config = TransferConfigModel()
    @State private var engine = EngineService()

    var body: some View {
        NavigationSplitView {
            List(Section.allCases, selection: $selection) { s in
                Label(s.rawValue, systemImage: s.icon).tag(s)
            }
            .navigationTitle("MySQL Transfer")
        } detail: {
            switch selection {
            case .connections:
                ConnectionsView(vm: ConnectionsViewModel(config: config, engine: engine))
            default:
                Text("\(selection?.rawValue ?? "Select a section")")
                    .foregroundStyle(.secondary)
            }
        }
    }
}
```

- [ ] **Step 5: Run the view-model test**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/ConnectionsViewModelTests`
Expected: PASS.

- [ ] **Step 6: Manual verification checklist**

1. Run the app; select "Connections".
2. Source and Destination forms render with all fields.
3. Toggling "Use SSH tunnel" reveals the SSH fields.
4. Fill an invalid source (host `127.0.0.1`, port `1`) and click Test Connection → an `✗ …` status appears within a moment and the form is briefly disabled.

- [ ] **Step 7: Commit**

```bash
git add app/Sources/Views/ConnectionsView.swift app/Sources/App/RootView.swift app/Tests
git commit -m "feat(app): Connections screen with Test Connection"
```

---

### Task 12: Options screen

**Files:**
- Create: `app/Sources/Views/OptionsView.swift`
- Modify: `app/Sources/App/RootView.swift` (route `.options`)
- Create: `app/Tests/MySQLTransferTests/OptionsBindingTests.swift`

**Interfaces:**
- Consumes: `TransferConfigModel` (Task 9).
- Produces: `struct OptionsView: View` bound directly to a `@Bindable TransferConfigModel`. No new view model — options are plain bindings; the test asserts the model reflects edits and `toFFI()` carries them.

- [ ] **Step 1: Write the failing test** `app/Tests/MySQLTransferTests/OptionsBindingTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

final class OptionsBindingTests: XCTestCase {
    func testOptionsFlowIntoFFIConfig() {
        let m = TransferConfigModel()
        m.chunkSizeText = "2500"; m.workersText = "6"
        m.dropExisting = true; m.incremental = true; m.incrementalColumn = "updated_at"
        m.includeViews = true; m.includeRoutines = true; m.includeTriggers = true
        let cfg = m.toFFI()
        XCTAssertEqual(cfg.chunkSize, 2500)
        XCTAssertEqual(cfg.workers, 6)
        XCTAssertTrue(cfg.dropExisting)
        XCTAssertTrue(cfg.incremental)
        XCTAssertEqual(cfg.incrementalColumn, "updated_at")
        XCTAssertTrue(cfg.includeViews && cfg.includeRoutines && cfg.includeTriggers)
    }
}
```

- [ ] **Step 2: Run test to verify it fails (or passes mapping but proves intent)**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/OptionsBindingTests`
Expected: This compiles and PASSES already (mapping exists from Task 9) — it is the regression guard for the Options screen's bindings. If it fails to compile, the model field names drifted; fix them to match Task 9.

- [ ] **Step 3: Write `app/Sources/Views/OptionsView.swift`**

```swift
import SwiftUI

struct OptionsView: View {
    @Bindable var config: TransferConfigModel

    var body: some View {
        Form {
            Section("Performance") {
                TextField("Chunk size (rows)", text: $config.chunkSizeText)
                TextField("Workers", text: $config.workersText)
            }
            Section("Write mode") {
                Toggle("Drop & recreate destination tables", isOn: $config.dropExisting)
                Toggle("Truncate tables before insert", isOn: $config.truncate)
                Toggle("Incremental / delta sync", isOn: $config.incremental)
                if config.incremental {
                    TextField("Incremental column", text: $config.incrementalColumn)
                }
            }
            Section("Schema objects") {
                Toggle("Include views", isOn: $config.includeViews)
                Toggle("Include routines (procs & functions)", isOn: $config.includeRoutines)
                Toggle("Include triggers", isOn: $config.includeTriggers)
            }
            Section {
                Toggle("Dry run (preview, no writes)", isOn: $config.dryRun)
            }
        }
        .formStyle(.grouped)
        .navigationTitle("Options")
    }
}
```

- [ ] **Step 4: Route `.options` in `RootView`**

In the `detail:` switch add:

```swift
            case .options:
                OptionsView(config: config)
```

- [ ] **Step 5: Run tests**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/OptionsBindingTests`
Expected: PASS.

- [ ] **Step 6: Manual verification checklist**

1. Run the app; select "Options".
2. Toggle "Incremental" → the "Incremental column" field appears.
3. Edit chunk size / workers; values persist when switching sections and back.

- [ ] **Step 7: Commit**

```bash
git add app/Sources/Views/OptionsView.swift app/Sources/App/RootView.swift app/Tests
git commit -m "feat(app): Options screen bound to TransferConfigModel"
```

---

## PHASE 10 — Tables screen (inspect + selection)

### Task 13: Inspect view model

**Files:**
- Create: `app/Sources/Models/InspectModel.swift`
- Create: `app/Tests/MySQLTransferTests/InspectModelTests.swift`

**Interfaces:**
- Produces:
  ```swift
  @Observable @MainActor final class InspectModel {
      var tables: [TableInfo]
      var loading: Bool
      var error: String?
      func load(config: TransferConfigModel) async   // calls EngineService.inspect, fills tables/error
  }
  ```

- [ ] **Step 1: Write the failing test** `app/Tests/MySQLTransferTests/InspectModelTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

@MainActor
final class InspectModelTests: XCTestCase {
    func testLoadSetsErrorForUnreachableSource() async {
        let cfg = TransferConfigModel()
        cfg.source.host = "127.0.0.1"; cfg.source.portText = "1"
        cfg.source.user = "x"; cfg.source.database = "x"
        let m = InspectModel(engine: EngineService())
        await m.load(config: cfg)
        XCTAssertFalse(m.loading)
        XCTAssertNotNil(m.error)
        XCTAssertTrue(m.tables.isEmpty)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/InspectModelTests`
Expected: FAIL — `InspectModel` not found.

- [ ] **Step 3: Write `app/Sources/Models/InspectModel.swift`**

```swift
import Foundation

@Observable
@MainActor
final class InspectModel {
    private let engine: EngineService
    var tables: [TableInfo] = []
    var loading = false
    var error: String?

    init(engine: EngineService) { self.engine = engine }

    func load(config: TransferConfigModel) async {
        loading = true
        error = nil
        defer { loading = false }
        do {
            let inspection = try await engine.inspect(config.toFFI())
            tables = inspection.tables
        } catch {
            self.error = String(describing: error)
            tables = []
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/InspectModelTests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/Sources/Models/InspectModel.swift app/Tests
git commit -m "feat(app): InspectModel loads source tables via FFI"
```

---

### Task 14: Tables screen with multi-select

**Files:**
- Create: `app/Sources/Views/TablesView.swift`
- Modify: `app/Sources/App/RootView.swift` (route `.tables`)
- Create: `app/Tests/MySQLTransferTests/TableSelectionTests.swift`

**Interfaces:**
- Consumes: `InspectModel` (Task 13), `TransferConfigModel.selectedTables` (Task 9).
- Produces: `struct TablesView: View` showing `TableInfo` rows (name, rows, size MB) with checkbox multi-select bound to `config.selectedTables`, a "Refresh" (inspect) button, and "Select all / none". Selection feeds `cfg.tables` via `toFFI()`.

- [ ] **Step 1: Write the failing test** `app/Tests/MySQLTransferTests/TableSelectionTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

final class TableSelectionTests: XCTestCase {
    func testToggleSelectionUpdatesConfig() {
        let config = TransferConfigModel()
        let helper = TableSelectionHelper(config: config)
        helper.toggle("users")
        helper.toggle("orders")
        XCTAssertEqual(config.selectedTables, ["users", "orders"])
        helper.toggle("users")
        XCTAssertEqual(config.selectedTables, ["orders"])
        helper.selectAll(["a", "b", "c"])
        XCTAssertEqual(config.selectedTables, ["a", "b", "c"])
        helper.selectNone()
        XCTAssertTrue(config.selectedTables.isEmpty)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/TableSelectionTests`
Expected: FAIL — `TableSelectionHelper` not found.

- [ ] **Step 3: Write `app/Sources/Views/TablesView.swift`**

```swift
import SwiftUI

/// Pure selection logic, unit-tested independent of SwiftUI.
struct TableSelectionHelper {
    let config: TransferConfigModel
    func toggle(_ name: String) {
        if config.selectedTables.contains(name) { config.selectedTables.remove(name) }
        else { config.selectedTables.insert(name) }
    }
    func selectAll(_ names: [String]) { config.selectedTables = Set(names) }
    func selectNone() { config.selectedTables = [] }
}

struct TablesView: View {
    @Bindable var config: TransferConfigModel
    @State private var inspect: InspectModel

    init(config: TransferConfigModel, engine: EngineService) {
        self._config = Bindable(config)
        self._inspect = State(initialValue: InspectModel(engine: engine))
    }

    private var helper: TableSelectionHelper { TableSelectionHelper(config: config) }

    var body: some View {
        VStack(alignment: .leading) {
            HStack {
                Button("Refresh") { Task { await inspect.load(config: config) } }
                Button("Select All") { helper.selectAll(inspect.tables.map(\.name)) }
                Button("Select None") { helper.selectNone() }
                if inspect.loading { ProgressView().controlSize(.small) }
                Spacer()
                Text("\(config.selectedTables.count) selected").foregroundStyle(.secondary)
            }
            .padding(.horizontal)

            if let error = inspect.error {
                Text(error).foregroundStyle(.red).padding(.horizontal)
            }

            Table(inspect.tables) {
                TableColumn("") { t in
                    Toggle("", isOn: Binding(
                        get: { config.selectedTables.contains(t.name) },
                        set: { _ in helper.toggle(t.name) }
                    )).labelsHidden()
                }.width(28)
                TableColumn("Table", value: \.name)
                TableColumn("Rows") { t in Text("\(t.rows)") }
                TableColumn("Size (MB)") { t in Text(String(format: "%.2f", t.sizeMb)) }
            }
        }
        .navigationTitle("Tables")
        .task { if inspect.tables.isEmpty { await inspect.load(config: config) } }
    }
}
```

- [ ] **Step 4: Route `.tables` in `RootView`**

```swift
            case .tables:
                TablesView(config: config, engine: engine)
```

- [ ] **Step 5: Run tests**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/TableSelectionTests`
Expected: PASS.

- [ ] **Step 6: Manual verification checklist** (requires a reachable MySQL — otherwise verify the error path)

1. With valid source credentials in Connections, select "Tables".
2. The table list loads (rows + size populated); a spinner shows while loading.
3. Check a few tables → the "N selected" counter updates; "Select All"/"Select None" work.
4. With an unreachable source, an error message shows instead of a list.

- [ ] **Step 7: Commit**

```bash
git add app/Sources/Views/TablesView.swift app/Sources/App/RootView.swift app/Tests
git commit -m "feat(app): Tables screen with inspect + multi-select"
```

---

## PHASE 11 — Transfer, Diff, Config I/O

### Task 15: `ListenerBridge` + `TransferProgressModel`

**Files:**
- Create: `app/Sources/Services/ListenerBridge.swift`
- Create: `app/Sources/Models/TransferProgressModel.swift`
- Create: `app/Tests/MySQLTransferTests/TransferProgressModelTests.swift`

**Interfaces:**
- Produces:
  - `final class ListenerBridge: TransferListener` — implements `func onEvent(event: TransferEvent)` by hopping to the `@MainActor` and forwarding to a `TransferProgressModel`.
  - `@Observable @MainActor final class TransferProgressModel` with `var perTable: [String: TableProgress]`, `var orderedTables: [String]`, `var logLines: [String]`, `var summary: TransferSummary?`, `var running: Bool`; and `func apply(_ event: TransferEvent)` reducing events into state. `struct TableProgress { var total: UInt64; var done: UInt64; var finished: Bool; var error: String? }`.

- [ ] **Step 1: Write the failing test** `app/Tests/MySQLTransferTests/TransferProgressModelTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

@MainActor
final class TransferProgressModelTests: XCTestCase {
    func testEventsReduceIntoProgress() {
        let m = TransferProgressModel()
        m.apply(.phase(name: "Data"))
        m.apply(.tableStarted(table: "users", totalRows: 100))
        m.apply(.chunkInserted(table: "users", rows: 40))
        m.apply(.chunkInserted(table: "users", rows: 60))
        m.apply(.tableCompleted(stats: TableStats(table: "users", rowsTransferred: 100,
                                                  chunks: 2, error: nil)))
        XCTAssertEqual(m.perTable["users"]?.total, 100)
        XCTAssertEqual(m.perTable["users"]?.done, 100)
        XCTAssertTrue(m.perTable["users"]?.finished ?? false)
        XCTAssertEqual(m.orderedTables, ["users"])

        m.apply(.log(level: .warn, message: "retrying"))
        XCTAssertTrue(m.logLines.contains { $0.contains("retrying") })

        let summary = TransferSummary(stats: [], totalRows: 100, errors: 0,
                                      elapsedSecs: 1.0, rowsPerSec: 100)
        m.apply(.summary(summary: summary))
        XCTAssertEqual(m.summary?.totalRows, 100)
    }
}
```

> Note: the exact Swift enum case spelling (`.tableStarted(table:totalRows:)` etc.) is what UniFFI generates from the Rust `TransferEvent` in Task 2. If the generated names differ, align the `apply` switch and this test to the generated cases.

- [ ] **Step 2: Run test to verify it fails**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/TransferProgressModelTests`
Expected: FAIL — `TransferProgressModel` not found.

- [ ] **Step 3: Write `app/Sources/Models/TransferProgressModel.swift`**

```swift
import Foundation

struct TableProgress {
    var total: UInt64 = 0
    var done: UInt64 = 0
    var finished = false
    var error: String?
    var fraction: Double { total == 0 ? (finished ? 1 : 0) : min(1, Double(done) / Double(total)) }
}

@Observable
@MainActor
final class TransferProgressModel {
    var perTable: [String: TableProgress] = [:]
    var orderedTables: [String] = []
    var logLines: [String] = []
    var summary: TransferSummary?
    var running = false

    func reset() {
        perTable = [:]; orderedTables = []; logLines = []; summary = nil
    }

    private func ensure(_ table: String) {
        if perTable[table] == nil { perTable[table] = TableProgress(); orderedTables.append(table) }
    }

    func apply(_ event: TransferEvent) {
        switch event {
        case .phase(let name):
            logLines.append("— \(name) —")
        case .tableCounted(let table, let rows):
            ensure(table); perTable[table]?.total = rows
        case .tableStarted(let table, let totalRows):
            ensure(table); perTable[table]?.total = totalRows
        case .chunkInserted(let table, let rows):
            ensure(table); perTable[table]?.done += rows
        case .tableCompleted(let stats):
            ensure(stats.table)
            perTable[stats.table]?.finished = true
            perTable[stats.table]?.error = stats.error
            if perTable[stats.table]?.total == 0 {
                perTable[stats.table]?.total = stats.rowsTransferred
                perTable[stats.table]?.done = stats.rowsTransferred
            }
        case .log(let level, let message):
            logLines.append("[\(level)] \(message)")
        case .summary(let summary):
            self.summary = summary
        }
    }
}
```

- [ ] **Step 4: Write `app/Sources/Services/ListenerBridge.swift`**

```swift
import Foundation

/// Implements the UniFFI foreign callback. UniFFI invokes `onEvent` on a worker
/// thread, so every call hops to the main actor before touching UI state.
final class ListenerBridge: TransferListener {
    private let model: TransferProgressModel
    init(model: TransferProgressModel) { self.model = model }

    func onEvent(event: TransferEvent) {
        Task { @MainActor in
            model.apply(event)
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/TransferProgressModelTests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add app/Sources/Services/ListenerBridge.swift app/Sources/Models/TransferProgressModel.swift app/Tests
git commit -m "feat(app): event reducer and main-actor listener bridge"
```

---

### Task 16: Transfer screen (progress + log + summary)

**Files:**
- Create: `app/Sources/Views/TransferView.swift`
- Modify: `app/Sources/App/RootView.swift` (route `.transfer`)
- Create: `app/Tests/MySQLTransferTests/TransferRunnerTests.swift`

**Interfaces:**
- Consumes: `EngineService.transfer` (Task 10), `ListenerBridge` + `TransferProgressModel` (Task 15).
- Produces:
  - `@Observable @MainActor final class TransferRunner` with `var progress: TransferProgressModel`, `var runError: String?`, `func run(config: TransferConfigModel, schema: Bool, data: Bool) async` (resets progress, builds a `ListenerBridge`, calls `engine.transfer`, sets `progress.running` around the call, captures thrown errors).
  - `struct TransferView: View` with three buttons (Transfer = schema+data, Schema only, Data only), a per-table progress list (`ProgressView(value:)`), a scrolling log, and a summary panel.

- [ ] **Step 1: Write the failing test** `app/Tests/MySQLTransferTests/TransferRunnerTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

@MainActor
final class TransferRunnerTests: XCTestCase {
    func testRunCapturesErrorForUnreachableHost() async {
        let cfg = TransferConfigModel()
        cfg.source.host = "127.0.0.1"; cfg.source.portText = "1"
        cfg.source.user = "x"; cfg.source.database = "x"
        cfg.dest = cfg.source
        let runner = TransferRunner(engine: EngineService())
        await runner.run(config: cfg, schema: true, data: true)
        XCTAssertFalse(runner.progress.running)
        XCTAssertNotNil(runner.runError)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/TransferRunnerTests`
Expected: FAIL — `TransferRunner` not found.

- [ ] **Step 3: Write `app/Sources/Views/TransferView.swift`**

```swift
import SwiftUI

@Observable
@MainActor
final class TransferRunner {
    private let engine: EngineService
    var progress = TransferProgressModel()
    var runError: String?

    init(engine: EngineService) { self.engine = engine }

    func run(config: TransferConfigModel, schema: Bool, data: Bool) async {
        progress.reset()
        progress.running = true
        runError = nil
        defer { progress.running = false }
        let bridge = ListenerBridge(model: progress)
        do {
            _ = try await engine.transfer(config.toFFI(), schema: schema, data: data,
                                          listener: bridge)
        } catch {
            runError = String(describing: error)
        }
    }
}

struct TransferView: View {
    @Bindable var config: TransferConfigModel
    @State private var runner: TransferRunner

    init(config: TransferConfigModel, engine: EngineService) {
        self._config = Bindable(config)
        self._runner = State(initialValue: TransferRunner(engine: engine))
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Button("Transfer") { Task { await runner.run(config: config, schema: true, data: true) } }
                Button("Schema only") { Task { await runner.run(config: config, schema: true, data: false) } }
                Button("Data only") { Task { await runner.run(config: config, schema: false, data: true) } }
                if runner.progress.running { ProgressView().controlSize(.small) }
            }
            .disabled(runner.progress.running)
            .padding(.horizontal)

            if let err = runner.runError {
                Text(err).foregroundStyle(.red).padding(.horizontal)
            }

            List(runner.progress.orderedTables, id: \.self) { name in
                let p = runner.progress.perTable[name]
                VStack(alignment: .leading, spacing: 2) {
                    HStack {
                        Text(name).font(.body.monospaced())
                        Spacer()
                        if let p { Text("\(p.done)/\(p.total)").foregroundStyle(.secondary) }
                        if let e = p?.error { Text("✗ \(e)").foregroundStyle(.red) }
                    }
                    ProgressView(value: p?.fraction ?? 0)
                }
            }
            .frame(minHeight: 160)

            if let s = runner.progress.summary {
                GroupBox("Summary") {
                    Text("Total rows: \(s.totalRows)   Errors: \(s.errors)   " +
                         "Time: \(String(format: "%.1f", s.elapsedSecs))s   " +
                         "Speed: \(String(format: "%.0f", s.rowsPerSec)) rows/s")
                }
                .padding(.horizontal)
            }

            DisclosureGroup("Log") {
                ScrollView {
                    VStack(alignment: .leading) {
                        ForEach(Array(runner.progress.logLines.enumerated()), id: \.offset) { _, line in
                            Text(line).font(.caption.monospaced())
                        }
                    }.frame(maxWidth: .infinity, alignment: .leading)
                }.frame(height: 120)
            }
            .padding(.horizontal)
        }
        .navigationTitle("Transfer")
    }
}
```

- [ ] **Step 4: Route `.transfer` in `RootView`**

```swift
            case .transfer:
                TransferView(config: config, engine: engine)
```

- [ ] **Step 5: Run tests**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/TransferRunnerTests`
Expected: PASS.

- [ ] **Step 6: Manual verification checklist** (with a reachable source + dest)

1. Configure valid source/dest, select a couple of tables, select "Transfer".
2. Click "Transfer" → per-table progress bars fill; the log shows phase/`✓` lines; buttons disabled while running.
3. On completion a Summary box shows total rows / errors / time / speed.
4. "Schema only" creates tables without data; "Data only" inserts rows assuming tables exist.

- [ ] **Step 7: Commit**

```bash
git add app/Sources/Views/TransferView.swift app/Sources/App/RootView.swift app/Tests
git commit -m "feat(app): Transfer screen with live progress, log, and summary"
```

---

### Task 17: Diff screen

**Files:**
- Create: `app/Sources/Models/DiffModel.swift`
- Create: `app/Sources/Views/DiffView.swift`
- Modify: `app/Sources/App/RootView.swift` (route `.diff`)
- Create: `app/Tests/MySQLTransferTests/DiffModelTests.swift`

**Interfaces:**
- Produces:
  - `@Observable @MainActor final class DiffModel` with `var diff: SchemaDiff?`, `var loading: Bool`, `var error: String?`, `func load(config: TransferConfigModel) async` (calls `EngineService.diff`).
  - `struct DiffView: View` listing source-only / dest-only / in-both tables with a Refresh button.

- [ ] **Step 1: Write the failing test** `app/Tests/MySQLTransferTests/DiffModelTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

@MainActor
final class DiffModelTests: XCTestCase {
    func testLoadSetsErrorForUnreachableHosts() async {
        let cfg = TransferConfigModel()
        cfg.source.host = "127.0.0.1"; cfg.source.portText = "1"
        cfg.source.user = "x"; cfg.source.database = "x"
        cfg.dest = cfg.source
        let m = DiffModel(engine: EngineService())
        await m.load(config: cfg)
        XCTAssertFalse(m.loading)
        XCTAssertNotNil(m.error)
        XCTAssertNil(m.diff)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/DiffModelTests`
Expected: FAIL — `DiffModel` not found.

- [ ] **Step 3: Write `app/Sources/Models/DiffModel.swift`**

```swift
import Foundation

@Observable
@MainActor
final class DiffModel {
    private let engine: EngineService
    var diff: SchemaDiff?
    var loading = false
    var error: String?

    init(engine: EngineService) { self.engine = engine }

    func load(config: TransferConfigModel) async {
        loading = true; error = nil
        defer { loading = false }
        do { diff = try await engine.diff(config.toFFI()) }
        catch { self.error = String(describing: error); diff = nil }
    }
}
```

- [ ] **Step 4: Write `app/Sources/Views/DiffView.swift`**

```swift
import SwiftUI

struct DiffView: View {
    @Bindable var config: TransferConfigModel
    @State private var model: DiffModel

    init(config: TransferConfigModel, engine: EngineService) {
        self._config = Bindable(config)
        self._model = State(initialValue: DiffModel(engine: engine))
    }

    var body: some View {
        VStack(alignment: .leading) {
            HStack {
                Button("Refresh") { Task { await model.load(config: config) } }
                if model.loading { ProgressView().controlSize(.small) }
            }.padding(.horizontal)

            if let error = model.error {
                Text(error).foregroundStyle(.red).padding(.horizontal)
            }

            if let diff = model.diff {
                List {
                    Section("Missing at destination (\(diff.sourceOnly.count))") {
                        ForEach(diff.sourceOnly, id: \.self) { Text($0).foregroundStyle(.orange) }
                    }
                    Section("Extra at destination (\(diff.destOnly.count))") {
                        ForEach(diff.destOnly, id: \.self) { Text($0).foregroundStyle(.red) }
                    }
                    Section("In both (\(diff.inBoth.count))") {
                        ForEach(diff.inBoth, id: \.self) { Text($0).foregroundStyle(.green) }
                    }
                }
            }
        }
        .navigationTitle("Diff")
        .task { if model.diff == nil { await model.load(config: config) } }
    }
}
```

- [ ] **Step 5: Route `.diff` in `RootView`**

```swift
            case .diff:
                DiffView(config: config, engine: engine)
```

Also remove the now-unused `default:` placeholder branch if all five cases are handled (keep a `case .none:` / `default:` returning the "Select a section" text to satisfy exhaustiveness).

- [ ] **Step 6: Run tests**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/DiffModelTests`
Expected: PASS.

- [ ] **Step 7: Manual verification checklist** (reachable source + dest)

1. Select "Diff" → three sections populate (missing/extra/in-both) with colored rows.
2. Refresh re-runs the comparison.

- [ ] **Step 8: Commit**

```bash
git add app/Sources/Models/DiffModel.swift app/Sources/Views/DiffView.swift app/Sources/App/RootView.swift app/Tests
git commit -m "feat(app): Diff screen comparing source vs destination"
```

---

### Task 18: Config load/save (Yams, CLI-interoperable)

**Files:**
- Create: `app/Sources/Models/ConfigYaml.swift`
- Modify: `app/Sources/App/RootView.swift` (toolbar Load/Save)
- Create: `app/Tests/MySQLTransferTests/ConfigYamlTests.swift`
- Add: Yams via Swift Package Manager dependency on the app target.

**Interfaces:**
- Produces:
  - `enum ConfigYaml { static func load(_ url: URL, into model: TransferConfigModel) throws; static func save(_ model: TransferConfigModel, to url: URL) throws }`
  - YAML shape matches the CLI (`source:` / `destination:` blocks with `host/port/user/password/database/charset` + optional `ssh:` `{enabled,host,port,user,password,key_file}`, and `options:` with `tables, exclude_tables, chunk_size, workers, drop_existing, truncate, incremental, incremental_column, include_views, include_routines, include_triggers, dry_run`).

- [ ] **Step 1: Add Yams**

In Xcode: app target ▸ Package Dependencies ▸ add `https://github.com/jpsim/Yams` (Up to Next Major). Add `Yams` to the `MySQLTransfer` target's frameworks.

- [ ] **Step 2: Write the failing test** `app/Tests/MySQLTransferTests/ConfigYamlTests.swift`

```swift
import XCTest
@testable import MySQLTransfer

final class ConfigYamlTests: XCTestCase {
    func testSaveThenLoadRoundTrips() throws {
        let m = TransferConfigModel()
        m.source.host = "src.example.com"; m.source.portText = "3306"
        m.source.user = "su"; m.source.password = "sp"; m.source.database = "shop"
        m.source.sshEnabled = true; m.source.sshHost = "bastion"; m.source.sshKeyFile = "/k.pem"
        m.dest.host = "dst.example.com"; m.dest.user = "du"; m.dest.database = "shop2"
        m.chunkSizeText = "5000"; m.workersText = "8"; m.dropExisting = true
        m.includeViews = true; m.incremental = true; m.incrementalColumn = "updated_at"

        let url = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("cfg-\(UUID()).yaml")
        try ConfigYaml.save(m, to: url)

        let loaded = TransferConfigModel()
        try ConfigYaml.load(url, into: loaded)
        XCTAssertEqual(loaded.source.host, "src.example.com")
        XCTAssertTrue(loaded.source.sshEnabled)
        XCTAssertEqual(loaded.source.sshKeyFile, "/k.pem")
        XCTAssertEqual(loaded.dest.database, "shop2")
        XCTAssertEqual(loaded.chunkSizeText, "5000")
        XCTAssertEqual(loaded.workersText, "8")
        XCTAssertTrue(loaded.dropExisting)
        XCTAssertTrue(loaded.includeViews)
        XCTAssertTrue(loaded.incremental)
        XCTAssertEqual(loaded.incrementalColumn, "updated_at")
    }

    func testLoadReadsCliStyleConfig() throws {
        let yaml = """
        source:
          host: db
          port: 3306
          user: u
          password: p
          database: d
          charset: utf8mb4
        destination:
          host: db2
          port: 3306
          user: u2
          password: p2
          database: d2
        options:
          chunk_size: 2000
          workers: 2
          include_triggers: true
        """
        let url = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("cli-\(UUID()).yaml")
        try yaml.write(to: url, atomically: true, encoding: .utf8)
        let m = TransferConfigModel()
        try ConfigYaml.load(url, into: m)
        XCTAssertEqual(m.source.host, "db")
        XCTAssertEqual(m.dest.host, "db2")
        XCTAssertEqual(m.chunkSizeText, "2000")
        XCTAssertEqual(m.workersText, "2")
        XCTAssertTrue(m.includeTriggers)
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/ConfigYamlTests`
Expected: FAIL — `ConfigYaml` not found.

- [ ] **Step 4: Write `app/Sources/Models/ConfigYaml.swift`**

```swift
import Foundation
import Yams

enum ConfigYaml {
    static func save(_ m: TransferConfigModel, to url: URL) throws {
        let dict: [String: Any] = [
            "source": connDict(m.source),
            "destination": connDict(m.dest),
            "options": [
                "tables": Array(m.selectedTables).sorted(),
                "exclude_tables": m.excludeTables,
                "chunk_size": Int(m.chunkSizeText) ?? 10000,
                "workers": Int(m.workersText) ?? 4,
                "drop_existing": m.dropExisting,
                "truncate": m.truncate,
                "incremental": m.incremental,
                "incremental_column": m.incrementalColumn,
                "include_views": m.includeViews,
                "include_routines": m.includeRoutines,
                "include_triggers": m.includeTriggers,
                "dry_run": m.dryRun,
            ],
        ]
        let yaml = try Yams.dump(object: dict)
        try yaml.write(to: url, atomically: true, encoding: .utf8)
    }

    static func load(_ url: URL, into m: TransferConfigModel) throws {
        let text = try String(contentsOf: url, encoding: .utf8)
        guard let root = try Yams.load(yaml: text) as? [String: Any] else { return }
        if let s = root["source"] as? [String: Any] { applyConn(s, to: m.source) }
        let destDict = (root["destination"] as? [String: Any]) ?? (root["dest"] as? [String: Any])
        if let d = destDict { applyConn(d, to: m.dest) }
        if let o = root["options"] as? [String: Any] {
            if let v = o["tables"] as? [String] { m.selectedTables = Set(v) }
            if let v = o["exclude_tables"] as? [String] { m.excludeTables = v }
            if let v = o["chunk_size"] { m.chunkSizeText = "\(v)" }
            if let v = o["workers"] { m.workersText = "\(v)" }
            if let v = o["drop_existing"] as? Bool { m.dropExisting = v }
            if let v = o["truncate"] as? Bool { m.truncate = v }
            if let v = o["incremental"] as? Bool { m.incremental = v }
            if let v = o["incremental_column"] as? String { m.incrementalColumn = v }
            if let v = o["include_views"] as? Bool { m.includeViews = v }
            if let v = o["include_routines"] as? Bool { m.includeRoutines = v }
            if let v = o["include_triggers"] as? Bool { m.includeTriggers = v }
            if let v = o["dry_run"] as? Bool { m.dryRun = v }
        }
    }

    private static func connDict(_ f: ConnectionFormModel) -> [String: Any] {
        var d: [String: Any] = [
            "host": f.host, "port": Int(f.portText) ?? 3306, "user": f.user,
            "password": f.password, "database": f.database, "charset": f.charset,
        ]
        if f.sshEnabled {
            var ssh: [String: Any] = [
                "enabled": true, "host": f.sshHost,
                "port": Int(f.sshPortText) ?? 22, "user": f.sshUser,
            ]
            if !f.sshPassword.isEmpty { ssh["password"] = f.sshPassword }
            if !f.sshKeyFile.isEmpty { ssh["key_file"] = f.sshKeyFile }
            d["ssh"] = ssh
        }
        return d
    }

    private static func applyConn(_ d: [String: Any], to f: ConnectionFormModel) {
        if let v = d["host"] as? String { f.host = v }
        if let v = d["port"] { f.portText = "\(v)" }
        if let v = d["user"] as? String { f.user = v }
        if let v = d["password"] as? String { f.password = v }
        if let v = d["database"] as? String { f.database = v }
        if let v = d["charset"] as? String { f.charset = v }
        if let ssh = d["ssh"] as? [String: Any] {
            f.sshEnabled = (ssh["enabled"] as? Bool) ?? true
            if let v = ssh["host"] as? String { f.sshHost = v }
            if let v = ssh["port"] { f.sshPortText = "\(v)" }
            if let v = ssh["user"] as? String { f.sshUser = v }
            if let v = ssh["password"] as? String { f.sshPassword = v }
            if let v = ssh["key_file"] as? String { f.sshKeyFile = v }
        }
    }
}
```

- [ ] **Step 5: Add Load/Save toolbar to `RootView`**

Wrap the `NavigationSplitView` with a toolbar using `.fileImporter` / `.fileExporter`:

```swift
    @State private var importing = false
    @State private var exporting = false
    @State private var ioError: String?
    // ... inside body, attach to the NavigationSplitView:
        .toolbar {
            ToolbarItem { Button("Load") { importing = true } }
            ToolbarItem { Button("Save") { exporting = true } }
        }
        .fileImporter(isPresented: $importing, allowedContentTypes: [.yaml]) { result in
            if case .success(let url) = result {
                do { try ConfigYaml.load(url, into: config) } catch { ioError = "\(error)" }
            }
        }
        .fileExporter(isPresented: $exporting, document: YamlDocument(config: config),
                      contentType: .yaml, defaultFilename: "config") { _ in }
        .alert("Config error", isPresented: .constant(ioError != nil)) {
            Button("OK") { ioError = nil }
        } message: { Text(ioError ?? "") }
```

Add a minimal `FileDocument` in the same file (or `ConfigYaml.swift`):

```swift
import UniformTypeIdentifiers
import SwiftUI

struct YamlDocument: FileDocument {
    static var readableContentTypes: [UTType] { [.yaml] }
    let text: String
    init(config: TransferConfigModel) {
        let tmp = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("export.yaml")
        try? ConfigYaml.save(config, to: tmp)
        text = (try? String(contentsOf: tmp, encoding: .utf8)) ?? ""
    }
    init(configuration: ReadConfiguration) throws { text = "" }
    func fileWrapper(configuration: WriteConfiguration) throws -> FileWrapper {
        FileWrapper(regularFileWithContents: Data(text.utf8))
    }
}
```

(`.yaml` UTType is available on macOS 14; if the SDK lacks it, declare `static let yaml = UTType(filenameExtension: "yaml")!` and use that.)

- [ ] **Step 6: Run tests**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS' -only-testing:MySQLTransferTests/ConfigYamlTests`
Expected: PASS (2 tests).

- [ ] **Step 7: Manual verification checklist**

1. Fill in connections + options; click "Save" → choose a path → a `config.yaml` is written.
2. Open that file in a text editor and confirm the `source:`/`destination:`/`options:` shape matches the CLI's `config.example.yaml`.
3. Run the **CLI** (`mysql-transfer inspect -c <that file>`) and confirm it accepts the file.
4. Click "Load" on a CLI-authored config → the form fields populate.

- [ ] **Step 8: Run the full app test suite**

Run: `xcodebuild test -project app/MySQLTransfer.xcodeproj -scheme MySQLTransfer -destination 'platform=macOS'`
Expected: all targets PASS.

- [ ] **Step 9: Commit**

```bash
git add app
git commit -m "feat(app): YAML config load/save interoperable with the CLI"
```

---

## Self-Review

**Spec coverage (phases 8–11):**
- Phase 8 (FFI + XCFramework): Tasks 1–7 — crate scaffold, mirror types + conversions, `test_connection`, `TransferListener` + sink bridge, async exports, Swift smoke test, `build-xcframework.sh`. ✔
- Phase 9 (skeleton + EngineService + Connections + Options): Tasks 8–12. ✔
- Phase 10 (Tables: inspect + multi-select): Tasks 13–14. ✔
- Phase 11 (Transfer + Diff + config I/O): Tasks 15–18. ✔ All `TransferEvent` cases (`Phase`, `TableCounted`, `TableStarted`, `ChunkInserted`, `TableCompleted`, `Log`, `Summary`) are consumed by the Task 15 reducer; transfer/schema-only/data-only buttons map to `(schema,data)` = `(true,true)/(true,false)/(false,true)`.

**Placeholder scan:** No `TODO`/`TBD`/"implement later"/"similar to Task N". Every code step carries real code; the one deleted `build.rs` is shown then explicitly removed; the `uniffi-bindgen` bin contingency is spelled out with exact contents.

**Type consistency:** FFI function names (`runTransfer/runInspect/runDiff/testConnection`) and types (`TransferConfig/ConnectionConfig/SshConfig/Inspection/SchemaDiff/TransferSummary/TableStats/TableInfo/RoutineInfo/TransferEvent/Level/ConnectionTestResult`) are used identically across Rust definitions (Task 2/3) and Swift consumers (Tasks 9–18). View-model names (`EngineService`, `ListenerBridge`, `TransferProgressModel`, `TransferRunner`, `ConnectionsViewModel`, `InspectModel`, `DiffModel`, `TableSelectionHelper`, `ConnectionFormModel`, `TransferConfigModel`) are consistent between their defining task and every reference. Two explicit reconciliation notes flag where UniFFI-generated Swift case spellings (Task 15) or a non-async `testConnection` (Task 10) may differ from the assumed shape, with the exact fix to apply.

**Resolved gaps:** (1) UniFFI tuple limitation → introduced `ConnectionTestResult` record rather than returning `(bool,String)`. (2) `run_transfer`'s `schema`/`data` flags were not in the directive's stated core signature but are required by the spec §5 and the three transfer buttons — used the spec signature and noted the dependency in the header. (3) The directive named no Plan 1 filename; referenced it as `2026-06-24-rust-core-cli.md` for the dependency note (adjust if Plan 1 lands under a different name).
