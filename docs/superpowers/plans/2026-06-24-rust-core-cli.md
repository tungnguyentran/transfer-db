# Rust Core + CLI Implementation Plan (Plan 1 of 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a native Rust re-implementation of the Python `mysql-transfer` CLI with full feature parity (5 commands, SSH tunnels, incremental sync, views/routines/triggers, dry-run, wildcard filters, parallel chunked transfer), as a Cargo workspace whose `transfer-core` library is UI-agnostic and emits progress as events.

**Architecture:** A Cargo workspace with two crates. `transfer-core` (library) owns config, connections, SSH tunneling, schema/data transfer, and orchestration; it never prints — it emits `TransferEvent`s through an `EventSink` and returns `Result<_, TransferError>`. `transfer-cli` (binary) parses args with clap, drains events into `indicatif` progress bars, and renders inspect/diff/summary with `comfy-table`. This is Plan 1 of 2; the FFI binding crate and SwiftUI app are Plan 2.

**Tech Stack:** Rust 2021, tokio (async), mysql_async (MySQL driver), russh (pure-Rust SSH), clap (CLI), serde + serde_yaml (config), indicatif + comfy-table + console (CLI output), inquire (interactive select), thiserror (core errors), anyhow (CLI errors).

## Global Constraints

- Rust edition **2021**.
- Async runtime: **tokio** with features `rt-multi-thread`, `macros`, `net`, `io-util`, `time`, `sync`.
- MySQL driver: **mysql_async** (`0.34`). SSH: **russh** + **russh-keys** (`0.45`).
- CLI: **clap** (`4`, `derive`). Config: **serde** (`1`, `derive`) + **serde_yaml** (`0.9`).
- CLI output: **indicatif** (`0.17`) + **comfy-table** (`7`) + **console** (`0.15`). Interactive: **inquire** (`0.7`).
- Errors: **thiserror** (`1`) in `transfer-core`; **anyhow** (`1`) in `transfer-cli`.
- `transfer-core` **NEVER prints** (no `println!`/`eprintln!`/`std::process::exit`) — it emits events via `EventSink` and returns `Result<T, TransferError>`. Only `transfer-cli` may print or exit.
- Default config values must match the Python tool exactly: connection `localhost:3306`, user `root`, charset `utf8mb4`; SSH port `22`, disabled by default; `chunk_size 10000`, `workers 4`, all booleans `false`, `incremental_column ""`.
- Type names are fixed across all tasks: `TransferEvent`, `Level`, `TableStats`, `TransferSummary`, `EventSink`, `TransferError`, `Result<T>` (= `std::result::Result<T, TransferError>`), `SshConfig`, `ConnectionConfig`, `TransferConfig`, `CliOverrides`, `TableInfo`, `RoutineInfo`, `Inspection`, `SchemaDiff`.
- Tests that need a live MySQL or SSH server are marked `#[ignore]` (run via `cargo test -- --ignored` against a `testcontainers` MySQL) so `cargo test` is green with no Docker.

---

### Task 1: Workspace skeleton + error & event types

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `crates/transfer-core/Cargo.toml`
- Create: `crates/transfer-core/src/lib.rs`
- Create: `crates/transfer-core/src/error.rs`
- Create: `crates/transfer-core/src/event.rs`
- Create: `crates/transfer-cli/Cargo.toml`
- Create: `crates/transfer-cli/src/main.rs`

**Interfaces:**
- Produces:
  - `pub enum TransferError { Config(String), Ssh(String), MySql(String), Io(std::io::Error) }` with `impl std::error::Error`; `From<std::io::Error>` and `From<mysql_async::Error>`.
  - `pub type Result<T> = std::result::Result<T, TransferError>;`
  - `pub enum Level { Info, Warn, DryRun }`
  - `pub struct TableStats { pub table: String, pub rows_transferred: u64, pub chunks: u64, pub error: Option<String> }`
  - `pub struct TransferSummary { pub stats: Vec<TableStats>, pub total_rows: u64, pub errors: u64, pub elapsed_secs: f64, pub rows_per_sec: f64 }`
  - `pub enum TransferEvent { Phase(String), TableCounted { table: String, rows: u64 }, TableStarted { table: String, total_rows: u64 }, ChunkInserted { table: String, rows: u64 }, TableCompleted(TableStats), Log { level: Level, message: String }, Summary(TransferSummary) }`
  - `pub struct EventSink` with `fn new(tx: tokio::sync::mpsc::UnboundedSender<TransferEvent>) -> Self`, `fn send(&self, ev: TransferEvent)`, `fn log(&self, level: Level, message: impl Into<String>)`; derives `Clone`.

- [ ] **Step 1: Create the workspace root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = ["crates/transfer-core", "crates/transfer-cli"]

[workspace.package]
edition = "2021"
version = "0.1.0"

[workspace.dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "io-util", "time", "sync"] }
mysql_async = "0.34"
russh = "0.45"
russh-keys = "0.45"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
thiserror = "1"
anyhow = "1"
clap = { version = "4", features = ["derive"] }
indicatif = "0.17"
comfy-table = "7"
console = "0.15"
inquire = "0.7"
```

- [ ] **Step 2: Create `crates/transfer-core/Cargo.toml`**

```toml
[package]
name = "transfer-core"
edition.workspace = true
version.workspace = true

[dependencies]
tokio.workspace = true
mysql_async.workspace = true
russh.workspace = true
russh-keys.workspace = true
async-trait.workspace = true
serde.workspace = true
serde_yaml.workspace = true
thiserror.workspace = true
```

- [ ] **Step 3: Write the failing test for error & event types**

Create `crates/transfer-core/src/event.rs`:

```rust
//! Progress event model emitted by the core (the UI-agnostic seam).

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Level {
    Info,
    Warn,
    DryRun,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStats {
    pub table: String,
    pub rows_transferred: u64,
    pub chunks: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferSummary {
    pub stats: Vec<TableStats>,
    pub total_rows: u64,
    pub errors: u64,
    pub elapsed_secs: f64,
    pub rows_per_sec: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransferEvent {
    Phase(String),
    TableCounted { table: String, rows: u64 },
    TableStarted { table: String, total_rows: u64 },
    ChunkInserted { table: String, rows: u64 },
    TableCompleted(TableStats),
    Log { level: Level, message: String },
    Summary(TransferSummary),
}

/// Cloneable handle the core uses to publish progress events.
#[derive(Clone)]
pub struct EventSink {
    tx: tokio::sync::mpsc::UnboundedSender<TransferEvent>,
}

impl EventSink {
    pub fn new(tx: tokio::sync::mpsc::UnboundedSender<TransferEvent>) -> Self {
        Self { tx }
    }

    pub fn send(&self, ev: TransferEvent) {
        // Ignore send errors: a dropped receiver just means nobody is rendering.
        let _ = self.tx.send(ev);
    }

    pub fn log(&self, level: Level, message: impl Into<String>) {
        self.send(TransferEvent::Log {
            level,
            message: message.into(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sink_forwards_events_to_receiver() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = EventSink::new(tx);
        sink.send(TransferEvent::Phase("Data".into()));
        sink.log(Level::Info, "hello");

        assert_eq!(rx.recv().await.unwrap(), TransferEvent::Phase("Data".into()));
        assert_eq!(
            rx.recv().await.unwrap(),
            TransferEvent::Log { level: Level::Info, message: "hello".into() }
        );
    }

    #[test]
    fn sink_send_after_receiver_dropped_is_silent() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);
        let sink = EventSink::new(tx);
        sink.send(TransferEvent::Phase("x".into())); // must not panic
    }
}
```

Create `crates/transfer-core/src/error.rs`:

```rust
//! Core error type. The library never prints or exits — it returns these.

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error("config error: {0}")]
    Config(String),
    #[error("ssh error: {0}")]
    Ssh(String),
    #[error("mysql error: {0}")]
    MySql(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<mysql_async::Error> for TransferError {
    fn from(e: mysql_async::Error) -> Self {
        TransferError::MySql(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, TransferError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_variant_context() {
        let e = TransferError::Config("bad".into());
        assert_eq!(e.to_string(), "config error: bad");
    }

    #[test]
    fn io_error_converts() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "nope");
        let e: TransferError = io.into();
        assert!(matches!(e, TransferError::Io(_)));
    }
}
```

Create `crates/transfer-core/src/lib.rs`:

```rust
pub mod error;
pub mod event;

pub use error::{Result, TransferError};
pub use event::{EventSink, Level, TableStats, TransferEvent, TransferSummary};
```

- [ ] **Step 4: Create the CLI crate so the workspace builds**

Create `crates/transfer-cli/Cargo.toml`:

```toml
[package]
name = "transfer-cli"
edition.workspace = true
version.workspace = true

[[bin]]
name = "mysql-transfer"
path = "src/main.rs"

[dependencies]
transfer-core = { path = "../transfer-core" }
tokio.workspace = true
clap.workspace = true
indicatif.workspace = true
comfy-table.workspace = true
console.workspace = true
inquire.workspace = true
anyhow.workspace = true
```

Create `crates/transfer-cli/src/main.rs`:

```rust
fn main() {
    println!("mysql-transfer (rust) — placeholder");
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p transfer-core`
Expected: PASS — 4 tests (`sink_forwards_events_to_receiver`, `sink_send_after_receiver_dropped_is_silent`, `display_includes_variant_context`, `io_error_converts`).

Run: `cargo build`
Expected: workspace builds, `mysql-transfer` binary compiles.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/
git commit -m "feat: workspace skeleton with error and event types"
```

---

### Task 2: Config types, loading, validation, CLI overrides

**Files:**
- Create: `crates/transfer-core/src/config.rs`
- Create: `crates/transfer-core/tests/config_fixtures/full.yaml`
- Modify: `crates/transfer-core/src/lib.rs`

**Interfaces:**
- Consumes: `Result`, `TransferError` (Task 1).
- Produces:
  - `pub struct SshConfig { pub enabled: bool, pub host: String, pub port: u16, pub user: String, pub password: Option<String>, pub key_file: Option<String>, pub key_password: Option<String> }` (`Default` = disabled, port 22); `fn validate(&self, label: &str) -> Vec<String>`.
  - `pub struct ConnectionConfig { pub host: String, pub port: u16, pub user: String, pub password: String, pub database: String, pub charset: String, pub ssh: SshConfig }` (`Default` = localhost:3306/root/utf8mb4); `fn validate(&self, label: &str) -> Vec<String>`.
  - `pub struct TransferConfig { pub source, pub dest: ConnectionConfig, pub tables: Vec<String>, pub exclude_tables: Vec<String>, pub chunk_size: usize, pub workers: usize, pub drop_existing: bool, pub truncate: bool, pub incremental: bool, pub incremental_column: String, pub include_views: bool, pub include_routines: bool, pub include_triggers: bool, pub dry_run: bool }`; `fn validate(&self) -> Vec<String>`.
  - `pub fn load_config(path: Option<&std::path::Path>) -> Result<TransferConfig>`
  - `pub struct CliOverrides { ... all Option fields ... }` (`Default`); `pub fn apply_cli_overrides(cfg: &mut TransferConfig, ov: &CliOverrides)`.

- [ ] **Step 1: Write failing tests**

Create `crates/transfer-core/tests/config_fixtures/full.yaml`:

```yaml
source:
  host: "src.example.com"
  port: 3306
  user: "srcuser"
  password: "srcpass"
  database: "srcdb"
  charset: "utf8mb4"
  ssh:
    host: "gateway"
    user: "ec2-user"
    key_file: "/path/key"
destination:
  host: "dst.example.com"
  user: "dstuser"
  password: "dstpass"
  database: "dstdb"
options:
  tables: ["a", "b"]
  exclude_tables: ["c"]
  chunk_size: 5000
  workers: 8
  drop_existing: true
  include_views: true
```

Add to the bottom of `crates/transfer-core/src/config.rs` (created in Step 2) — but write the test first by creating the file with only this `tests` module and empty `use super::*;` will fail to compile, which is our failing state. Use this test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn defaults_match_python() {
        let c = TransferConfig::default();
        assert_eq!(c.source.host, "localhost");
        assert_eq!(c.source.port, 3306);
        assert_eq!(c.source.user, "root");
        assert_eq!(c.source.charset, "utf8mb4");
        assert!(!c.source.ssh.enabled);
        assert_eq!(c.source.ssh.port, 22);
        assert_eq!(c.chunk_size, 10_000);
        assert_eq!(c.workers, 4);
        assert!(!c.drop_existing);
        assert_eq!(c.incremental_column, "");
    }

    #[test]
    fn loads_yaml_with_destination_alias_and_options() {
        let path = Path::new("tests/config_fixtures/full.yaml");
        let c = load_config(Some(path)).unwrap();
        assert_eq!(c.source.host, "src.example.com");
        assert_eq!(c.dest.host, "dst.example.com"); // `destination:` key honored
        assert_eq!(c.dest.port, 3306); // default filled
        assert_eq!(c.tables, vec!["a", "b"]);
        assert_eq!(c.exclude_tables, vec!["c"]);
        assert_eq!(c.chunk_size, 5000);
        assert_eq!(c.workers, 8);
        assert!(c.drop_existing);
        assert!(c.include_views);
        // ssh block present without `enabled` => enabled defaults true (Python parity)
        assert!(c.source.ssh.enabled);
        assert_eq!(c.source.ssh.host, "gateway");
        assert_eq!(c.source.ssh.port, 22);
        // dest has no ssh block => disabled
        assert!(!c.dest.ssh.enabled);
    }

    #[test]
    fn missing_file_is_config_error() {
        let err = load_config(Some(Path::new("tests/config_fixtures/nope.yaml"))).unwrap_err();
        assert!(matches!(err, TransferError::Config(_)));
    }

    #[test]
    fn none_path_returns_defaults() {
        let c = load_config(None).unwrap();
        assert_eq!(c, TransferConfig::default());
    }

    #[test]
    fn validate_reports_missing_required_fields() {
        let c = TransferConfig::default(); // empty database/user-ok but db empty
        let errs = c.validate();
        assert!(errs.iter().any(|e| e == "source: database is required"));
        assert!(errs.iter().any(|e| e == "dest: database is required"));
    }

    #[test]
    fn validate_incremental_requires_column() {
        let mut c = TransferConfig::default();
        c.source.database = "d".into();
        c.dest.database = "d".into();
        c.incremental = true;
        let errs = c.validate();
        assert!(errs.iter().any(|e| e.contains("incremental-column is required")));
    }

    #[test]
    fn ssh_override_enables_ssh() {
        let mut c = TransferConfig::default();
        let ov = CliOverrides { source_ssh_host: Some("g".into()), ..Default::default() };
        apply_cli_overrides(&mut c, &ov);
        assert!(c.source.ssh.enabled);
        assert_eq!(c.source.ssh.host, "g");
    }

    #[test]
    fn overrides_split_comma_tables_and_apply_scalars() {
        let mut c = TransferConfig::default();
        let ov = CliOverrides {
            tables: Some("x, y ,z".into()),
            chunk_size: Some(99),
            dry_run: Some(true),
            ..Default::default()
        };
        apply_cli_overrides(&mut c, &ov);
        assert_eq!(c.tables, vec!["x", "y", "z"]);
        assert_eq!(c.chunk_size, 99);
        assert!(c.dry_run);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core config`
Expected: FAIL to compile — `config` module/items not found.

- [ ] **Step 3: Write the implementation**

Create `crates/transfer-core/src/config.rs` (place the `tests` module from Step 1 at the bottom):

```rust
//! Configuration: typed structs, YAML loading, validation, CLI overrides.

use crate::error::{Result, TransferError};
use serde::Deserialize;
use std::path::Path;

fn default_true() -> bool { true }
fn default_ssh_port() -> u16 { 22 }

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SshConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub host: String,
    #[serde(default = "default_ssh_port")]
    pub port: u16,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub key_file: Option<String>,
    #[serde(default)]
    pub key_password: Option<String>,
}

impl Default for SshConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: String::new(),
            port: 22,
            user: String::new(),
            password: None,
            key_file: None,
            key_password: None,
        }
    }
}

impl SshConfig {
    pub fn validate(&self, label: &str) -> Vec<String> {
        let mut errs = Vec::new();
        if self.enabled {
            if self.host.is_empty() {
                errs.push(format!("{label} SSH: host is required"));
            }
            if self.user.is_empty() {
                errs.push(format!("{label} SSH: user is required"));
            }
            if self.password.is_none() && self.key_file.is_none() {
                errs.push(format!("{label} SSH: password or key_file is required"));
            }
        }
        errs
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub charset: String,
    pub ssh: SshConfig,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 3306,
            user: "root".into(),
            password: String::new(),
            database: String::new(),
            charset: "utf8mb4".into(),
            ssh: SshConfig::default(),
        }
    }
}

impl ConnectionConfig {
    pub fn validate(&self, label: &str) -> Vec<String> {
        let mut errs = Vec::new();
        if self.host.is_empty() {
            errs.push(format!("{label}: host is required"));
        }
        if self.database.is_empty() {
            errs.push(format!("{label}: database is required"));
        }
        if self.user.is_empty() {
            errs.push(format!("{label}: user is required"));
        }
        errs.extend(self.ssh.validate(label));
        errs
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferConfig {
    pub source: ConnectionConfig,
    pub dest: ConnectionConfig,
    pub tables: Vec<String>,
    pub exclude_tables: Vec<String>,
    pub chunk_size: usize,
    pub workers: usize,
    pub drop_existing: bool,
    pub truncate: bool,
    pub incremental: bool,
    pub incremental_column: String,
    pub include_views: bool,
    pub include_routines: bool,
    pub include_triggers: bool,
    pub dry_run: bool,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            source: ConnectionConfig::default(),
            dest: ConnectionConfig::default(),
            tables: Vec::new(),
            exclude_tables: Vec::new(),
            chunk_size: 10_000,
            workers: 4,
            drop_existing: false,
            truncate: false,
            incremental: false,
            incremental_column: String::new(),
            include_views: false,
            include_routines: false,
            include_triggers: false,
            dry_run: false,
        }
    }
}

impl TransferConfig {
    pub fn validate(&self) -> Vec<String> {
        let mut errs = Vec::new();
        errs.extend(self.source.validate("source"));
        errs.extend(self.dest.validate("dest"));
        if self.incremental && self.incremental_column.is_empty() {
            errs.push("--incremental-column is required when --incremental is set".into());
        }
        if self.chunk_size < 1 {
            errs.push("--chunk-size must be >= 1".into());
        }
        if self.workers < 1 {
            errs.push("--workers must be >= 1".into());
        }
        errs
    }
}

// --- YAML loading -----------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
struct RawOptions {
    tables: Vec<String>,
    exclude_tables: Vec<String>,
    chunk_size: Option<usize>,
    workers: Option<usize>,
    drop_existing: bool,
    truncate: bool,
    incremental: bool,
    incremental_column: String,
    include_views: bool,
    include_routines: bool,
    include_triggers: bool,
    dry_run: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct RawConfig {
    source: ConnectionConfig,
    dest: Option<ConnectionConfig>,
    destination: Option<ConnectionConfig>,
    options: RawOptions,
}

pub fn load_config(path: Option<&Path>) -> Result<TransferConfig> {
    let Some(path) = path else {
        return Ok(TransferConfig::default());
    };
    if !path.exists() {
        return Err(TransferError::Config(format!(
            "Config file not found: {}",
            path.display()
        )));
    }
    let text = std::fs::read_to_string(path)?;
    let raw: RawConfig =
        serde_yaml::from_str(&text).map_err(|e| TransferError::Config(e.to_string()))?;

    let defaults = TransferConfig::default();
    let o = raw.options;
    Ok(TransferConfig {
        source: raw.source,
        dest: raw.dest.or(raw.destination).unwrap_or_default(),
        tables: o.tables,
        exclude_tables: o.exclude_tables,
        chunk_size: o.chunk_size.unwrap_or(defaults.chunk_size),
        workers: o.workers.unwrap_or(defaults.workers),
        drop_existing: o.drop_existing,
        truncate: o.truncate,
        incremental: o.incremental,
        incremental_column: o.incremental_column,
        include_views: o.include_views,
        include_routines: o.include_routines,
        include_triggers: o.include_triggers,
        dry_run: o.dry_run,
    })
}

// --- CLI overrides ----------------------------------------------------------

#[derive(Debug, Default)]
pub struct CliOverrides {
    pub source_host: Option<String>,
    pub source_port: Option<u16>,
    pub source_user: Option<String>,
    pub source_password: Option<String>,
    pub source_db: Option<String>,
    pub source_ssh_host: Option<String>,
    pub source_ssh_port: Option<u16>,
    pub source_ssh_user: Option<String>,
    pub source_ssh_password: Option<String>,
    pub source_ssh_key: Option<String>,
    pub dest_host: Option<String>,
    pub dest_port: Option<u16>,
    pub dest_user: Option<String>,
    pub dest_password: Option<String>,
    pub dest_db: Option<String>,
    pub dest_ssh_host: Option<String>,
    pub dest_ssh_port: Option<u16>,
    pub dest_ssh_user: Option<String>,
    pub dest_ssh_password: Option<String>,
    pub dest_ssh_key: Option<String>,
    pub tables: Option<String>,
    pub exclude_tables: Option<String>,
    pub chunk_size: Option<usize>,
    pub workers: Option<usize>,
    pub drop_existing: Option<bool>,
    pub truncate: Option<bool>,
    pub incremental: Option<bool>,
    pub incremental_column: Option<String>,
    pub include_views: Option<bool>,
    pub include_routines: Option<bool>,
    pub include_triggers: Option<bool>,
    pub dry_run: Option<bool>,
}

fn split_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

pub fn apply_cli_overrides(cfg: &mut TransferConfig, ov: &CliOverrides) {
    // Direct connection fields.
    if let Some(v) = &ov.source_host { cfg.source.host = v.clone(); }
    if let Some(v) = ov.source_port { cfg.source.port = v; }
    if let Some(v) = &ov.source_user { cfg.source.user = v.clone(); }
    if let Some(v) = &ov.source_password { cfg.source.password = v.clone(); }
    if let Some(v) = &ov.source_db { cfg.source.database = v.clone(); }
    if let Some(v) = &ov.dest_host { cfg.dest.host = v.clone(); }
    if let Some(v) = ov.dest_port { cfg.dest.port = v; }
    if let Some(v) = &ov.dest_user { cfg.dest.user = v.clone(); }
    if let Some(v) = &ov.dest_password { cfg.dest.password = v.clone(); }
    if let Some(v) = &ov.dest_db { cfg.dest.database = v.clone(); }

    // SSH fields — any SSH override enables SSH (Python parity).
    if let Some(v) = &ov.source_ssh_host { cfg.source.ssh.enabled = true; cfg.source.ssh.host = v.clone(); }
    if let Some(v) = ov.source_ssh_port { cfg.source.ssh.enabled = true; cfg.source.ssh.port = v; }
    if let Some(v) = &ov.source_ssh_user { cfg.source.ssh.enabled = true; cfg.source.ssh.user = v.clone(); }
    if let Some(v) = &ov.source_ssh_password { cfg.source.ssh.enabled = true; cfg.source.ssh.password = Some(v.clone()); }
    if let Some(v) = &ov.source_ssh_key { cfg.source.ssh.enabled = true; cfg.source.ssh.key_file = Some(v.clone()); }
    if let Some(v) = &ov.dest_ssh_host { cfg.dest.ssh.enabled = true; cfg.dest.ssh.host = v.clone(); }
    if let Some(v) = ov.dest_ssh_port { cfg.dest.ssh.enabled = true; cfg.dest.ssh.port = v; }
    if let Some(v) = &ov.dest_ssh_user { cfg.dest.ssh.enabled = true; cfg.dest.ssh.user = v.clone(); }
    if let Some(v) = &ov.dest_ssh_password { cfg.dest.ssh.enabled = true; cfg.dest.ssh.password = Some(v.clone()); }
    if let Some(v) = &ov.dest_ssh_key { cfg.dest.ssh.enabled = true; cfg.dest.ssh.key_file = Some(v.clone()); }

    // Scalars.
    if let Some(v) = ov.chunk_size { cfg.chunk_size = v; }
    if let Some(v) = ov.workers { cfg.workers = v; }
    if let Some(v) = ov.drop_existing { cfg.drop_existing = v; }
    if let Some(v) = ov.truncate { cfg.truncate = v; }
    if let Some(v) = ov.incremental { cfg.incremental = v; }
    if let Some(v) = &ov.incremental_column { cfg.incremental_column = v.clone(); }
    if let Some(v) = ov.include_views { cfg.include_views = v; }
    if let Some(v) = ov.include_routines { cfg.include_routines = v; }
    if let Some(v) = ov.include_triggers { cfg.include_triggers = v; }
    if let Some(v) = ov.dry_run { cfg.dry_run = v; }

    // Comma-separated lists.
    if let Some(v) = &ov.tables { cfg.tables = split_csv(v); }
    if let Some(v) = &ov.exclude_tables { cfg.exclude_tables = split_csv(v); }
}
```

Add to `crates/transfer-core/src/lib.rs`:

```rust
pub mod config;
pub use config::{
    apply_cli_overrides, load_config, CliOverrides, ConnectionConfig, SshConfig, TransferConfig,
};
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p transfer-core config`
Expected: PASS — all 8 config tests.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: config types, YAML loading, validation, CLI overrides"
```

---

### Task 3: Wildcard table matcher

**Files:**
- Create: `crates/transfer-core/src/matcher.rs`
- Modify: `crates/transfer-core/src/lib.rs`

**Interfaces:**
- Produces: `pub fn matches_pattern(name: &str, patterns: &[String]) -> bool` — case-insensitive; `*` matches any sequence, `?` matches one char; non-wildcard patterns match exactly; blank patterns are skipped.

- [ ] **Step 1: Write failing tests**

Create `crates/transfer-core/src/matcher.rs`:

```rust
//! Case-insensitive wildcard matching for table include/exclude filters.

/// `*` = any sequence, `?` = exactly one char. Plain patterns match exactly.
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut mark = 0usize;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

pub fn matches_pattern(name: &str, patterns: &[String]) -> bool {
    let name = name.to_lowercase();
    for pattern in patterns {
        let pattern = pattern.trim().to_lowercase();
        if pattern.is_empty() {
            continue;
        }
        if pattern.contains('*') || pattern.contains('?') {
            if wildcard_match(&pattern, &name) {
                return true;
            }
        } else if pattern == name {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pats(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn exact_case_insensitive() {
        assert!(matches_pattern("Users", &pats(&["users"])));
        assert!(!matches_pattern("users_log", &pats(&["users"])));
    }

    #[test]
    fn star_prefix_and_suffix() {
        assert!(matches_pattern("django_session", &pats(&["django_*"])));
        assert!(matches_pattern("audit_log", &pats(&["*_log"])));
        assert!(!matches_pattern("logs", &pats(&["*_log"])));
    }

    #[test]
    fn question_mark_single_char() {
        assert!(matches_pattern("t1", &pats(&["t?"])));
        assert!(!matches_pattern("t12", &pats(&["t?"])));
    }

    #[test]
    fn blank_patterns_skipped_and_empty_list_false() {
        assert!(!matches_pattern("x", &pats(&["", "  "])));
        assert!(!matches_pattern("x", &pats(&[])));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core matcher`
Expected: FAIL to compile — `matcher` module not declared in `lib.rs`.

- [ ] **Step 3: Wire the module in**

Add to `crates/transfer-core/src/lib.rs`:

```rust
pub mod matcher;
pub use matcher::matches_pattern;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p transfer-core matcher`
Expected: PASS — 4 matcher tests.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: case-insensitive wildcard table matcher"
```

---

### Task 4: MySQL connection + `test_connection`

**Files:**
- Create: `crates/transfer-core/src/connection.rs`
- Modify: `crates/transfer-core/src/lib.rs`
- Create: `crates/transfer-core/tests/connection_it.rs`

**Interfaces:**
- Consumes: `ConnectionConfig` (Task 2), `Result`, `TransferError` (Task 1).
- Produces:
  - `pub async fn connect(cfg: &ConnectionConfig, local_addr: Option<(String, u16)>) -> Result<mysql_async::Conn>` — connects directly to `cfg.host:cfg.port`, or to `local_addr` when `Some` (the SSH-tunnel local endpoint added in Task 9). Runs `SET NAMES <charset>` on connect and best-effort session timeouts.
  - `pub async fn test_connection(cfg: &ConnectionConfig) -> (bool, String)` — runs `SELECT 1`; the success message notes "(via SSH tunnel)" when `cfg.ssh.enabled`. (SSH path is wired in Task 9; until then it connects directly.)

- [ ] **Step 1: Write the failing test (unit + ignored integration)**

Append the integration test to a new file `crates/transfer-core/tests/connection_it.rs`:

```rust
//! Live-MySQL tests. Run with: cargo test -p transfer-core -- --ignored
//! (expects a MySQL reachable per the env vars; see testcontainers note in the plan).

use transfer_core::{connect, test_connection, ConnectionConfig};

fn local_cfg() -> ConnectionConfig {
    ConnectionConfig {
        host: std::env::var("IT_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        port: std::env::var("IT_MYSQL_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3306),
        user: std::env::var("IT_MYSQL_USER").unwrap_or_else(|_| "root".into()),
        password: std::env::var("IT_MYSQL_PASS").unwrap_or_else(|_| "root".into()),
        database: std::env::var("IT_MYSQL_DB").unwrap_or_else(|_| "mysql".into()),
        charset: "utf8mb4".into(),
        ssh: Default::default(),
    }
}

#[tokio::test]
#[ignore = "requires live MySQL"]
async fn connects_and_selects_one() {
    use mysql_async::prelude::Queryable;
    let mut conn = connect(&local_cfg(), None).await.expect("connect");
    let one: Option<i64> = conn.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));
}

#[tokio::test]
#[ignore = "requires live MySQL"]
async fn test_connection_reports_success() {
    let (ok, msg) = test_connection(&local_cfg()).await;
    assert!(ok, "expected success, got: {msg}");
    assert!(msg.contains("successful"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core --test connection_it`
Expected: FAIL to compile — `connect`/`test_connection` not found.

- [ ] **Step 3: Write the implementation**

Create `crates/transfer-core/src/connection.rs`:

```rust
//! MySQL connection management (direct, or through a tunnel local endpoint).

use crate::config::ConnectionConfig;
use crate::error::Result;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};

/// Open a connection. When `local_addr` is `Some`, connect there (the SSH
/// tunnel's `127.0.0.1:<port>`) instead of `cfg.host:cfg.port`.
pub async fn connect(cfg: &ConnectionConfig, local_addr: Option<(String, u16)>) -> Result<Conn> {
    let (host, port) = local_addr.unwrap_or_else(|| (cfg.host.clone(), cfg.port));

    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .user(Some(cfg.user.clone()))
        .pass(Some(cfg.password.clone()))
        .db_name(Some(cfg.database.clone()))
        .init(vec![format!("SET NAMES {}", cfg.charset)]);

    let mut conn = Conn::new(opts).await?;

    // Best-effort: extend session timeouts for large transfers (ignore failures).
    let _ = conn.query_drop("SET SESSION net_read_timeout = 600").await;
    let _ = conn.query_drop("SET SESSION net_write_timeout = 600").await;
    let _ = conn.query_drop("SET SESSION wait_timeout = 28800").await;

    Ok(conn)
}

/// Try to connect and run `SELECT 1`. Returns `(success, message)`.
pub async fn test_connection(cfg: &ConnectionConfig) -> (bool, String) {
    match try_select_one(cfg).await {
        Ok(()) => {
            let suffix = if cfg.ssh.enabled { " (via SSH tunnel)" } else { "" };
            (true, format!("Connection successful{suffix}"))
        }
        Err(e) => (false, e.to_string()),
    }
}

async fn try_select_one(cfg: &ConnectionConfig) -> Result<()> {
    // NOTE: Task 9 replaces the `None` with a tunnel local endpoint when ssh.enabled.
    let mut conn = connect(cfg, None).await?;
    let _: Option<i64> = conn.query_first("SELECT 1").await?;
    conn.disconnect().await.ok();
    Ok(())
}
```

Add to `crates/transfer-core/src/lib.rs`:

```rust
pub mod connection;
pub use connection::{connect, test_connection};
```

- [ ] **Step 4: Verify it compiles and the ignored tests are collected**

Run: `cargo test -p transfer-core --test connection_it`
Expected: compiles; output shows `2 ignored` (no live DB needed for green).

Run: `cargo build`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: mysql connection and test_connection"
```

> **Testcontainers note:** the `#[ignore]` integration tests in this plan are designed to run against a MySQL started by `testcontainers`. Add `testcontainers = "0.20"` and `testcontainers-modules = { version = "0.8", features = ["mysql"] }` under `[dev-dependencies]` in `transfer-core/Cargo.toml` and a helper that starts the container and exports `IT_MYSQL_*` env vars when you wire CI. Local runs: `cargo test -p transfer-core -- --ignored`.

---

### Task 5: Schema introspection + DDL extraction

**Files:**
- Create: `crates/transfer-core/src/schema.rs`
- Modify: `crates/transfer-core/src/lib.rs`
- Modify: `crates/transfer-core/tests/connection_it.rs` (add schema integration tests)

**Interfaces:**
- Consumes: `mysql_async::Conn`, `Result` (Task 1/4).
- Produces:
  - `pub struct TableInfo { pub name: String, pub rows: u64, pub size_mb: f64, pub engine: String, pub collation: String }`
  - `pub struct RoutineInfo { pub name: String, pub kind: String }`
  - `pub async fn get_tables(conn: &mut Conn) -> Result<Vec<TableInfo>>`
  - `pub async fn get_views(conn: &mut Conn) -> Result<Vec<String>>`
  - `pub async fn get_routines(conn: &mut Conn) -> Result<Vec<RoutineInfo>>`
  - `pub async fn get_triggers(conn: &mut Conn) -> Result<Vec<String>>`
  - `pub async fn get_create_table(conn: &mut Conn, table: &str) -> Result<String>`
  - `pub async fn get_create_view(conn: &mut Conn, view: &str) -> Result<String>`
  - `pub async fn get_create_routine(conn: &mut Conn, name: &str, kind: &str) -> Result<String>`
  - `pub async fn get_create_trigger(conn: &mut Conn, trigger: &str) -> Result<String>`

- [ ] **Step 1: Write the failing integration tests**

Append to `crates/transfer-core/tests/connection_it.rs`:

```rust
#[tokio::test]
#[ignore = "requires live MySQL"]
async fn introspects_tables_and_ddl() {
    use mysql_async::prelude::Queryable;
    use transfer_core::schema::{get_create_table, get_tables};

    let mut conn = connect(&local_cfg(), None).await.unwrap();
    conn.query_drop("DROP TABLE IF EXISTS it_people").await.unwrap();
    conn.query_drop("CREATE TABLE it_people (id INT PRIMARY KEY, name VARCHAR(50))")
        .await
        .unwrap();

    let tables = get_tables(&mut conn).await.unwrap();
    assert!(tables.iter().any(|t| t.name == "it_people" && t.engine.to_uppercase().contains("INNODB")));

    let ddl = get_create_table(&mut conn, "it_people").await.unwrap();
    assert!(ddl.contains("CREATE TABLE") && ddl.contains("it_people"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core --test connection_it`
Expected: FAIL to compile — `transfer_core::schema` not found.

- [ ] **Step 3: Write the implementation**

Create `crates/transfer-core/src/schema.rs`:

```rust
//! Schema introspection and DDL extraction.

use crate::error::{Result, TransferError};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};

#[derive(Debug, Clone, PartialEq)]
pub struct TableInfo {
    pub name: String,
    pub rows: u64,
    pub size_mb: f64,
    pub engine: String,
    pub collation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineInfo {
    pub name: String,
    pub kind: String,
}

pub async fn get_tables(conn: &mut Conn) -> Result<Vec<TableInfo>> {
    let rows: Vec<(String, Option<u64>, Option<f64>, Option<String>, Option<String>)> = conn
        .query(
            "SELECT TABLE_NAME, TABLE_ROWS, \
             ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2), \
             ENGINE, TABLE_COLLATION \
             FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' \
             ORDER BY TABLE_NAME",
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(name, r, sz, eng, coll)| TableInfo {
            name,
            rows: r.unwrap_or(0),
            size_mb: sz.unwrap_or(0.0),
            engine: eng.unwrap_or_default(),
            collation: coll.unwrap_or_default(),
        })
        .collect())
}

pub async fn get_views(conn: &mut Conn) -> Result<Vec<String>> {
    let rows: Vec<String> = conn
        .query(
            "SELECT TABLE_NAME FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME",
        )
        .await?;
    Ok(rows)
}

pub async fn get_routines(conn: &mut Conn) -> Result<Vec<RoutineInfo>> {
    let rows: Vec<(String, String)> = conn
        .query(
            "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES \
             WHERE ROUTINE_SCHEMA = DATABASE() ORDER BY ROUTINE_TYPE, ROUTINE_NAME",
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(name, kind)| RoutineInfo { name, kind })
        .collect())
}

pub async fn get_triggers(conn: &mut Conn) -> Result<Vec<String>> {
    let rows: Vec<String> = conn
        .query(
            "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS \
             WHERE TRIGGER_SCHEMA = DATABASE() ORDER BY TRIGGER_NAME",
        )
        .await?;
    Ok(rows)
}

/// Fetch a single row and read a named column as `String`.
async fn show_create(conn: &mut Conn, sql: String, column: &str) -> Result<String> {
    let row: Option<Row> = conn.query_first(sql).await?;
    let row = row.ok_or_else(|| TransferError::MySql(format!("no rows for `{column}`")))?;
    row.get::<String, &str>(column)
        .ok_or_else(|| TransferError::MySql(format!("column `{column}` missing")))
}

pub async fn get_create_table(conn: &mut Conn, table: &str) -> Result<String> {
    show_create(conn, format!("SHOW CREATE TABLE `{table}`"), "Create Table").await
}

pub async fn get_create_view(conn: &mut Conn, view: &str) -> Result<String> {
    show_create(conn, format!("SHOW CREATE VIEW `{view}`"), "Create View").await
}

pub async fn get_create_routine(conn: &mut Conn, name: &str, kind: &str) -> Result<String> {
    let keyword = if kind.eq_ignore_ascii_case("PROCEDURE") {
        "PROCEDURE"
    } else {
        "FUNCTION"
    };
    let column = if keyword == "PROCEDURE" {
        "Create Procedure"
    } else {
        "Create Function"
    };
    show_create(conn, format!("SHOW CREATE {keyword} `{name}`"), column).await
}

pub async fn get_create_trigger(conn: &mut Conn, trigger: &str) -> Result<String> {
    show_create(
        conn,
        format!("SHOW CREATE TRIGGER `{trigger}`"),
        "SQL Original Statement",
    )
    .await
}
```

Add to `crates/transfer-core/src/lib.rs`:

```rust
pub mod schema;
pub use schema::{RoutineInfo, TableInfo};
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo test -p transfer-core --test connection_it`
Expected: compiles; `3 ignored`.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: schema introspection and DDL extraction"
```

---

### Task 6: `resolve_tables`, `run_inspect`, `run_diff`

**Files:**
- Create: `crates/transfer-core/src/transfer.rs`
- Modify: `crates/transfer-core/src/lib.rs`
- Create: `crates/transfer-core/tests/diff_unit.rs`

**Interfaces:**
- Consumes: `get_tables`, `TableInfo` (Task 5), `matches_pattern` (Task 3), `TransferConfig` (Task 2), `connect` (Task 4).
- Produces:
  - `pub async fn resolve_tables(conn: &mut Conn, cfg: &TransferConfig) -> Result<Vec<TableInfo>>`
  - `pub struct Inspection { pub tables: Vec<TableInfo>, pub views: Vec<String>, pub routines: Vec<RoutineInfo>, pub triggers: Vec<String> }`
  - `pub struct SchemaDiff { pub source: Vec<String>, pub dest: Vec<String> }` with `fn source_only(&self) -> Vec<String>`, `fn dest_only(&self) -> Vec<String>`, `fn in_both(&self) -> Vec<String>` (each sorted).
  - `pub async fn run_inspect(cfg: &TransferConfig) -> Result<Inspection>`
  - `pub async fn run_diff(cfg: &TransferConfig) -> Result<SchemaDiff>`

- [ ] **Step 1: Write the failing unit test for `SchemaDiff` set logic**

Create `crates/transfer-core/tests/diff_unit.rs`:

```rust
use transfer_core::transfer::SchemaDiff;

#[test]
fn diff_partitions_tables() {
    let d = SchemaDiff {
        source: vec!["a".into(), "b".into(), "c".into()],
        dest: vec!["b".into(), "c".into(), "d".into()],
    };
    assert_eq!(d.source_only(), vec!["a".to_string()]);
    assert_eq!(d.dest_only(), vec!["d".to_string()]);
    assert_eq!(d.in_both(), vec!["b".to_string(), "c".to_string()]);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core --test diff_unit`
Expected: FAIL to compile — `transfer_core::transfer::SchemaDiff` not found.

- [ ] **Step 3: Write the implementation**

Create `crates/transfer-core/src/transfer.rs`:

```rust
//! Orchestration: table resolution, inspect, diff. (run_transfer added in Task 8.)

use crate::config::TransferConfig;
use crate::connection::connect;
use crate::error::Result;
use crate::matcher::matches_pattern;
use crate::schema::{get_tables, get_triggers, get_views, get_routines, RoutineInfo, TableInfo};
use mysql_async::Conn;
use std::collections::BTreeSet;

pub async fn resolve_tables(conn: &mut Conn, cfg: &TransferConfig) -> Result<Vec<TableInfo>> {
    let mut tables = get_tables(conn).await?;
    if !cfg.tables.is_empty() {
        tables.retain(|t| matches_pattern(&t.name, &cfg.tables));
    }
    if !cfg.exclude_tables.is_empty() {
        tables.retain(|t| !matches_pattern(&t.name, &cfg.exclude_tables));
    }
    Ok(tables)
}

#[derive(Debug, Clone, PartialEq)]
pub struct Inspection {
    pub tables: Vec<TableInfo>,
    pub views: Vec<String>,
    pub routines: Vec<RoutineInfo>,
    pub triggers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDiff {
    pub source: Vec<String>,
    pub dest: Vec<String>,
}

impl SchemaDiff {
    pub fn source_only(&self) -> Vec<String> {
        let dest: BTreeSet<&String> = self.dest.iter().collect();
        let mut v: Vec<String> = self.source.iter().filter(|t| !dest.contains(t)).cloned().collect();
        v.sort();
        v
    }
    pub fn dest_only(&self) -> Vec<String> {
        let src: BTreeSet<&String> = self.source.iter().collect();
        let mut v: Vec<String> = self.dest.iter().filter(|t| !src.contains(t)).cloned().collect();
        v.sort();
        v
    }
    pub fn in_both(&self) -> Vec<String> {
        let dest: BTreeSet<&String> = self.dest.iter().collect();
        let mut v: Vec<String> = self.source.iter().filter(|t| dest.contains(t)).cloned().collect();
        v.sort();
        v
    }
}

pub async fn run_inspect(cfg: &TransferConfig) -> Result<Inspection> {
    let mut conn = connect(&cfg.source, None).await?;
    let inspection = Inspection {
        tables: get_tables(&mut conn).await?,
        views: get_views(&mut conn).await?,
        routines: get_routines(&mut conn).await?,
        triggers: get_triggers(&mut conn).await?,
    };
    conn.disconnect().await.ok();
    Ok(inspection)
}

pub async fn run_diff(cfg: &TransferConfig) -> Result<SchemaDiff> {
    let mut src = connect(&cfg.source, None).await?;
    let source: Vec<String> = get_tables(&mut src).await?.into_iter().map(|t| t.name).collect();
    src.disconnect().await.ok();

    let mut dst = connect(&cfg.dest, None).await?;
    let dest: Vec<String> = get_tables(&mut dst).await?.into_iter().map(|t| t.name).collect();
    dst.disconnect().await.ok();

    Ok(SchemaDiff { source, dest })
}
```

Add to `crates/transfer-core/src/lib.rs`:

```rust
pub mod transfer;
pub use transfer::{resolve_tables, run_diff, run_inspect, Inspection, SchemaDiff};
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p transfer-core --test diff_unit`
Expected: PASS — `diff_partitions_tables`.

Run: `cargo build`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: resolve_tables, run_inspect, run_diff"
```

---

### Task 7: Data SQL builders + column intersection (pure)

**Files:**
- Create: `crates/transfer-core/src/data.rs`
- Modify: `crates/transfer-core/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub fn build_insert_sql(table: &str, columns: &[String], upsert: bool) -> String` — `REPLACE` when `upsert`, else `INSERT IGNORE`; backtick-quoted columns; `?` placeholders.
  - `pub fn intersect_columns(source: &[String], dest: &[String]) -> Vec<String>` — source order preserved; if `dest` empty, returns all source columns.
  - `pub async fn get_columns(conn: &mut Conn, table: &str) -> Result<Vec<String>>`
  - `pub async fn get_row_count(conn: &mut Conn, table: &str) -> Result<u64>`

- [ ] **Step 1: Write failing unit tests**

Create `crates/transfer-core/src/data.rs` with the pure helpers and this test module:

```rust
//! Chunked / streaming data transfer.

use crate::error::Result;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};

pub fn build_insert_sql(table: &str, columns: &[String], upsert: bool) -> String {
    let cols = columns
        .iter()
        .map(|c| format!("`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; columns.len()].join(", ");
    let keyword = if upsert { "REPLACE" } else { "INSERT IGNORE" };
    format!("{keyword} INTO `{table}` ({cols}) VALUES ({placeholders})")
}

pub fn intersect_columns(source: &[String], dest: &[String]) -> Vec<String> {
    if dest.is_empty() {
        return source.to_vec();
    }
    let dest_set: std::collections::HashSet<&String> = dest.iter().collect();
    source.iter().filter(|c| dest_set.contains(c)).cloned().collect()
}

pub async fn get_columns(conn: &mut Conn, table: &str) -> Result<Vec<String>> {
    let rows: Vec<Row> = conn.query(format!("SHOW COLUMNS FROM `{table}`")).await?;
    Ok(rows
        .iter()
        .filter_map(|r| r.get::<String, &str>("Field"))
        .collect())
}

pub async fn get_row_count(conn: &mut Conn, table: &str) -> Result<u64> {
    let n: Option<u64> = conn
        .query_first(format!("SELECT COUNT(*) FROM `{table}`"))
        .await?;
    Ok(n.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn insert_ignore_when_not_upsert() {
        let sql = build_insert_sql("users", &cols(&["id", "name"]), false);
        assert_eq!(sql, "INSERT IGNORE INTO `users` (`id`, `name`) VALUES (?, ?)");
    }

    #[test]
    fn replace_when_upsert() {
        let sql = build_insert_sql("users", &cols(&["id"]), true);
        assert_eq!(sql, "REPLACE INTO `users` (`id`) VALUES (?)");
    }

    #[test]
    fn intersection_preserves_source_order() {
        let got = intersect_columns(&cols(&["a", "b", "c"]), &cols(&["c", "a"]));
        assert_eq!(got, cols(&["a", "c"]));
    }

    #[test]
    fn empty_dest_returns_all_source() {
        let got = intersect_columns(&cols(&["a", "b"]), &cols(&[]));
        assert_eq!(got, cols(&["a", "b"]));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core data`
Expected: FAIL to compile — `data` module not declared.

- [ ] **Step 3: Wire the module in**

Add to `crates/transfer-core/src/lib.rs`:

```rust
pub mod data;
pub use data::{build_insert_sql, get_columns, get_row_count, intersect_columns};
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p transfer-core data`
Expected: PASS — 4 data unit tests.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: data SQL builders and column intersection"
```

---

### Task 8: Single-table data transfer (chunked streaming, dry-run)

**Files:**
- Modify: `crates/transfer-core/src/data.rs`
- Modify: `crates/transfer-core/tests/connection_it.rs`

**Interfaces:**
- Consumes: `connect` (Task 4), `EventSink`/`TransferEvent`/`TableStats`/`Level` (Task 1), `build_insert_sql`/`intersect_columns`/`get_columns`/`get_row_count` (Task 7), `TransferConfig`/`ConnectionConfig` (Task 2).
- Produces:
  - `pub async fn transfer_table(source_cfg: &ConnectionConfig, dest_cfg: &ConnectionConfig, table: &str, total_rows: u64, cfg: &TransferConfig, sink: &EventSink, source_addr: Option<(String, u16)>, dest_addr: Option<(String, u16)>) -> Result<TableStats>` — creates its own source+dest connections (so it is retry- and parallel-safe), streams rows in `chunk_size` batches, emits `TableStarted` then `ChunkInserted` per batch, returns final `TableStats`. (Incremental logic is added in Task 11; this task implements full-copy + dry-run + truncate.)

- [ ] **Step 1: Write the failing integration test**

Append to `crates/transfer-core/tests/connection_it.rs`:

```rust
#[tokio::test]
#[ignore = "requires live MySQL"]
async fn transfers_rows_between_tables() {
    use mysql_async::prelude::Queryable;
    use transfer_core::data::transfer_table;
    use transfer_core::{EventSink, TransferConfig};

    let mut c = connect(&local_cfg(), None).await.unwrap();
    for t in ["it_src", "it_dst"] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).await.unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(50))"))
            .await
            .unwrap();
    }
    c.query_drop("INSERT INTO it_src VALUES (1,'a'),(2,'b'),(3,'c')").await.unwrap();

    // Point both source and dest configs at the same DB but different tables is not
    // possible; instead copy it_src -> it_src on a second DB in real CI. Here we copy
    // within one DB by transferring `it_src` (dest must also have `it_src`); use a
    // second schema via IT_MYSQL_DB2 if available. For the smoke test we assert the
    // builder path runs end-to-end against `it_src`->`it_src` with TRUNCATE.
    let mut cfg = TransferConfig::default();
    cfg.source = local_cfg();
    cfg.dest = local_cfg();
    cfg.truncate = true;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = EventSink::new(tx);
    let stats = transfer_table(&cfg.source, &cfg.dest, "it_src", 3, &cfg, &sink, None, None)
        .await
        .unwrap();
    assert_eq!(stats.rows_transferred, 3);
    assert!(stats.error.is_none());
    drop(sink);
    let mut saw_chunk = false;
    while let Some(ev) = rx.recv().await {
        if matches!(ev, transfer_core::TransferEvent::ChunkInserted { .. }) {
            saw_chunk = true;
        }
    }
    assert!(saw_chunk);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core --test connection_it`
Expected: FAIL to compile — `transfer_table` not found.

- [ ] **Step 3: Write the implementation**

Append to `crates/transfer-core/src/data.rs` (imports go at the top of the file):

```rust
use crate::config::{ConnectionConfig, TransferConfig};
use crate::connection::connect;
use crate::event::{EventSink, Level, TableStats, TransferEvent};
use futures_util::StreamExt;
use mysql_async::{Params, Value};

#[allow(clippy::too_many_arguments)]
pub async fn transfer_table(
    source_cfg: &ConnectionConfig,
    dest_cfg: &ConnectionConfig,
    table: &str,
    total_rows: u64,
    cfg: &TransferConfig,
    sink: &EventSink,
    source_addr: Option<(String, u16)>,
    dest_addr: Option<(String, u16)>,
) -> Result<TableStats> {
    let mut stats = TableStats {
        table: table.to_string(),
        rows_transferred: 0,
        chunks: 0,
        error: None,
    };

    let mut source = connect(source_cfg, source_addr).await?;
    let mut dest = connect(dest_cfg, dest_addr).await?;

    // Column intersection (schema-drift safety).
    let source_cols = get_columns(&mut source, table).await?;
    let dest_cols = get_columns(&mut dest, table).await?;
    let columns = intersect_columns(&source_cols, &dest_cols);
    if columns.is_empty() {
        stats.error = Some("No columns found".into());
        return Ok(stats);
    }

    sink.send(TransferEvent::TableStarted {
        table: table.to_string(),
        total_rows,
    });

    // Dry-run: count only.
    if cfg.dry_run {
        let count = get_row_count(&mut source, table).await?;
        stats.rows_transferred = count;
        sink.log(Level::DryRun, format!("Would transfer {count} rows from `{table}`"));
        return Ok(stats);
    }

    // Truncate if requested (non-incremental).
    if cfg.truncate && !cfg.incremental {
        dest.query_drop(format!("TRUNCATE TABLE `{table}`")).await?;
    }

    let insert_sql = build_insert_sql(table, &columns, cfg.incremental);

    // Optimise destination for bulk loading.
    for stmt in [
        "SET FOREIGN_KEY_CHECKS = 0",
        "SET UNIQUE_CHECKS = 0",
        "SET AUTOCOMMIT = 0",
        "SET sql_mode = ''",
    ] {
        dest.query_drop(stmt).await?;
    }

    // Stream source rows and batch-insert.
    let query = format!("SELECT * FROM `{table}`");
    let mut stream = source.exec_stream::<Row, _, _>(query, ()).await?;

    let mut batch: Vec<Vec<Value>> = Vec::with_capacity(cfg.chunk_size);
    while let Some(row) = stream.next().await {
        let row = row?;
        let values: Vec<Value> = columns
            .iter()
            .map(|c| row.get::<Value, &str>(c.as_str()).unwrap_or(Value::NULL))
            .collect();
        batch.push(values);

        if batch.len() >= cfg.chunk_size {
            flush(&mut dest, &insert_sql, &mut batch, table, sink, &mut stats).await?;
        }
    }
    drop(stream);
    if !batch.is_empty() {
        flush(&mut dest, &insert_sql, &mut batch, table, sink, &mut stats).await?;
    }

    // Restore destination settings.
    for stmt in [
        "SET FOREIGN_KEY_CHECKS = 1",
        "SET UNIQUE_CHECKS = 1",
        "SET AUTOCOMMIT = 1",
    ] {
        dest.query_drop(stmt).await?;
    }

    source.disconnect().await.ok();
    dest.disconnect().await.ok();
    Ok(stats)
}

async fn flush(
    dest: &mut Conn,
    insert_sql: &str,
    batch: &mut Vec<Vec<Value>>,
    table: &str,
    sink: &EventSink,
    stats: &mut TableStats,
) -> Result<()> {
    let n = batch.len() as u64;
    dest.exec_batch(insert_sql, batch.drain(..).map(Params::Positional)).await?;
    dest.query_drop("COMMIT").await?;
    stats.rows_transferred += n;
    stats.chunks += 1;
    sink.send(TransferEvent::ChunkInserted {
        table: table.to_string(),
        rows: n,
    });
    Ok(())
}
```

Add `futures-util` to `crates/transfer-core/Cargo.toml` dependencies:

```toml
futures-util = "0.3"
```

- [ ] **Step 4: Verify it compiles (ignored test collected)**

Run: `cargo test -p transfer-core --test connection_it`
Expected: compiles; `4 ignored`.

Run: `cargo test -p transfer-core`
Expected: PASS — all unit tests green; integration tests ignored.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: single-table chunked streaming transfer with dry-run"
```

---

### Task 9: Retry wrapper (3 attempts, backoff)

**Files:**
- Modify: `crates/transfer-core/src/data.rs`

**Interfaces:**
- Consumes: `transfer_table` (Task 8), `EventSink`/`TableStats`/`Level` (Task 1).
- Produces:
  - `pub async fn with_retry<F, Fut>(table: &str, sink: &EventSink, op: F) -> TableStats where F: FnMut() -> Fut, Fut: std::future::Future<Output = Result<TableStats>>` — up to 3 attempts; sleeps `5 * attempt` seconds between failures; on final failure returns `TableStats` with `error` set.
  - `pub async fn transfer_table_with_retry(source_cfg: &ConnectionConfig, dest_cfg: &ConnectionConfig, table: &str, total_rows: u64, cfg: &TransferConfig, sink: &EventSink, source_addr: Option<(String, u16)>, dest_addr: Option<(String, u16)>) -> TableStats` — `with_retry` wrapping `transfer_table`.

- [ ] **Step 1: Write failing unit tests (paused time → instant backoff)**

Append this test module content into `crates/transfer-core/src/data.rs`'s existing `#[cfg(test)] mod tests` block (add these two tests):

```rust
    use crate::error::TransferError;
    use std::cell::Cell;

    #[tokio::test(start_paused = true)]
    async fn retry_returns_first_success_without_retrying() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = crate::event::EventSink::new(tx);
        let calls = Cell::new(0);
        let stats = super::with_retry("t", &sink, || {
            calls.set(calls.get() + 1);
            async {
                Ok(crate::event::TableStats {
                    table: "t".into(),
                    rows_transferred: 5,
                    chunks: 1,
                    error: None,
                })
            }
        })
        .await;
        assert_eq!(calls.get(), 1);
        assert_eq!(stats.rows_transferred, 5);
        assert!(stats.error.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn retry_captures_error_after_three_failures() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = crate::event::EventSink::new(tx);
        let calls = Cell::new(0);
        let stats = super::with_retry("t", &sink, || {
            calls.set(calls.get() + 1);
            async { Err::<crate::event::TableStats, _>(TransferError::MySql("boom".into())) }
        })
        .await;
        assert_eq!(calls.get(), 3);
        assert_eq!(stats.error.as_deref(), Some("mysql error: boom"));
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core data::tests::retry`
Expected: FAIL to compile — `with_retry` not found.

- [ ] **Step 3: Write the implementation**

Append to `crates/transfer-core/src/data.rs` (add the imports at the top with the others):

```rust
use std::future::Future;
use std::time::Duration;

pub async fn with_retry<F, Fut>(table: &str, sink: &EventSink, mut op: F) -> TableStats
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<TableStats>>,
{
    let max: u64 = 3;
    for attempt in 1..=max {
        match op().await {
            Ok(stats) => return stats,
            Err(e) => {
                if attempt < max {
                    let wait = 5 * attempt;
                    sink.log(Level::Warn, format!("`{table}`: {e} — retrying in {wait}s..."));
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                } else {
                    return TableStats {
                        table: table.to_string(),
                        rows_transferred: 0,
                        chunks: 0,
                        error: Some(e.to_string()),
                    };
                }
            }
        }
    }
    TableStats {
        table: table.to_string(),
        rows_transferred: 0,
        chunks: 0,
        error: Some("Unknown error".into()),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn transfer_table_with_retry(
    source_cfg: &ConnectionConfig,
    dest_cfg: &ConnectionConfig,
    table: &str,
    total_rows: u64,
    cfg: &TransferConfig,
    sink: &EventSink,
    source_addr: Option<(String, u16)>,
    dest_addr: Option<(String, u16)>,
) -> TableStats {
    with_retry(table, sink, || {
        transfer_table(
            source_cfg,
            dest_cfg,
            table,
            total_rows,
            cfg,
            sink,
            source_addr.clone(),
            dest_addr.clone(),
        )
    })
    .await
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p transfer-core data`
Expected: PASS — 4 builder tests + 2 retry tests.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: retry wrapper with backoff for table transfer"
```

---

### Task 10: SSH tunnel forwarder (russh)

**Files:**
- Create: `crates/transfer-core/src/ssh_tunnel.rs`
- Modify: `crates/transfer-core/src/lib.rs`
- Modify: `crates/transfer-core/src/connection.rs` (route `test_connection` through a tunnel)
- Create: `crates/transfer-core/tests/ssh_it.rs`

**Interfaces:**
- Consumes: `SshConfig` (Task 2), `Result`/`TransferError` (Task 1).
- Produces:
  - `pub struct Tunnel { /* holds the russh session + bound local port */ }`
  - `pub async fn Tunnel::open(ssh: &SshConfig, remote_host: &str, remote_port: u16) -> Result<Tunnel>` — authenticates (key file or password), binds `127.0.0.1:0`, and forwards each accepted socket to `remote_host:remote_port` over a `direct-tcpip` channel.
  - `pub fn Tunnel::local_addr(&self) -> (String, u16)` — `("127.0.0.1", bound_port)`.
  - `pub async fn open_tunnel(cfg: &ConnectionConfig) -> Result<Option<Tunnel>>` — returns `Some` when `cfg.ssh.enabled`, else `None`.

> **Version note:** this task pins `russh`/`russh-keys` at `0.45`. The channel-stream API (`channel.into_stream()`), the handler trait method names, and `russh_keys::load_secret_key` are version-sensitive; if a different minor version is resolved, adjust these three call sites to that version's API. The forwarding *design* (one shared session, a local listener, one `direct-tcpip` channel per inbound socket) is stable.

- [ ] **Step 1: Write the failing test**

Create `crates/transfer-core/tests/ssh_it.rs`:

```rust
//! SSH tunnel tests. Require a reachable SSH server that can forward to MySQL.
//! Run with: cargo test -p transfer-core --test ssh_it -- --ignored

use transfer_core::ssh_tunnel::{open_tunnel, Tunnel};
use transfer_core::{ConnectionConfig, SshConfig};

#[tokio::test]
async fn open_tunnel_returns_none_when_disabled() {
    let cfg = ConnectionConfig::default(); // ssh disabled
    let t = open_tunnel(&cfg).await.unwrap();
    assert!(t.is_none());
}

#[tokio::test]
#[ignore = "requires live SSH + MySQL"]
async fn tunnel_forwards_to_remote_mysql() {
    let ssh = SshConfig {
        enabled: true,
        host: std::env::var("IT_SSH_HOST").unwrap(),
        port: 22,
        user: std::env::var("IT_SSH_USER").unwrap(),
        password: std::env::var("IT_SSH_PASS").ok(),
        key_file: std::env::var("IT_SSH_KEY").ok(),
        key_password: None,
    };
    let tunnel = Tunnel::open(&ssh, "127.0.0.1", 3306).await.unwrap();
    let (host, port) = tunnel.local_addr();
    assert_eq!(host, "127.0.0.1");
    assert!(port > 0);
    // A TCP connect to the local end should succeed (forwarded to remote MySQL).
    let stream = tokio::net::TcpStream::connect((host.as_str(), port)).await;
    assert!(stream.is_ok());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core --test ssh_it`
Expected: FAIL to compile — `ssh_tunnel` module not found.

- [ ] **Step 3: Write the implementation**

Create `crates/transfer-core/src/ssh_tunnel.rs`:

```rust
//! Pure-Rust SSH local port forwarding (the `sshtunnel` analog).
//!
//! One SSH session per endpoint, shared. A local TCP listener accepts MySQL
//! client connections; each is piped to the remote MySQL host:port over its
//! own `direct-tcpip` channel.

use crate::config::{ConnectionConfig, SshConfig};
use crate::error::{Result, TransferError};
use russh::client::{self, Handle};
use std::sync::Arc;
use tokio::net::TcpListener;

struct ClientHandler;

#[async_trait::async_trait]
impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh_keys::key::PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        // sshtunnel does not enforce host-key verification by default; accept.
        Ok(true)
    }
}

pub struct Tunnel {
    local_port: u16,
    _session: Arc<Handle<ClientHandler>>,
}

impl Tunnel {
    pub async fn open(ssh: &SshConfig, remote_host: &str, remote_port: u16) -> Result<Tunnel> {
        let config = Arc::new(client::Config::default());
        let mut session =
            client::connect(config, (ssh.host.as_str(), ssh.port), ClientHandler)
                .await
                .map_err(|e| TransferError::Ssh(e.to_string()))?;

        let authed = if let Some(key_file) = &ssh.key_file {
            let key = russh_keys::load_secret_key(key_file, ssh.key_password.as_deref())
                .map_err(|e| TransferError::Ssh(e.to_string()))?;
            session
                .authenticate_publickey(&ssh.user, Arc::new(key))
                .await
                .map_err(|e| TransferError::Ssh(e.to_string()))?
        } else if let Some(password) = &ssh.password {
            session
                .authenticate_password(&ssh.user, password)
                .await
                .map_err(|e| TransferError::Ssh(e.to_string()))?
        } else {
            return Err(TransferError::Ssh("ssh: password or key_file required".into()));
        };
        if !authed {
            return Err(TransferError::Ssh("ssh authentication failed".into()));
        }

        let session = Arc::new(session);
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let local_port = listener.local_addr()?.port();

        let session_for_loop = session.clone();
        let remote_host = remote_host.to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    break;
                };
                let session = session_for_loop.clone();
                let remote_host = remote_host.clone();
                tokio::spawn(async move {
                    let channel = match session
                        .channel_open_direct_tcpip(remote_host, remote_port as u32, "127.0.0.1", 0)
                        .await
                    {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    let mut channel_stream = channel.into_stream();
                    let _ =
                        tokio::io::copy_bidirectional(&mut inbound, &mut channel_stream).await;
                });
            }
        });

        Ok(Tunnel {
            local_port,
            _session: session,
        })
    }

    pub fn local_addr(&self) -> (String, u16) {
        ("127.0.0.1".to_string(), self.local_port)
    }
}

/// Open a tunnel for a connection when SSH is enabled.
pub async fn open_tunnel(cfg: &ConnectionConfig) -> Result<Option<Tunnel>> {
    if !cfg.ssh.enabled {
        return Ok(None);
    }
    Ok(Some(Tunnel::open(&cfg.ssh, &cfg.host, cfg.port).await?))
}
```

Add to `crates/transfer-core/src/lib.rs`:

```rust
pub mod ssh_tunnel;
pub use ssh_tunnel::{open_tunnel, Tunnel};
```

- [ ] **Step 4: Route `test_connection` through the tunnel**

In `crates/transfer-core/src/connection.rs`, replace `try_select_one` with the tunnel-aware version (imports `open_tunnel`):

```rust
async fn try_select_one(cfg: &ConnectionConfig) -> Result<()> {
    let tunnel = crate::ssh_tunnel::open_tunnel(cfg).await?;
    let local_addr = tunnel.as_ref().map(|t| t.local_addr());
    let mut conn = connect(cfg, local_addr).await?;
    let _: Option<i64> = conn.query_first("SELECT 1").await?;
    conn.disconnect().await.ok();
    Ok(())
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p transfer-core --test ssh_it`
Expected: PASS — `open_tunnel_returns_none_when_disabled`; `1 ignored`.

Run: `cargo build`
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: russh SSH tunnel forwarder"
```

---

### Task 11: Incremental sync (delta + upsert)

**Files:**
- Modify: `crates/transfer-core/src/data.rs` (extend `transfer_table`)
- Modify: `crates/transfer-core/tests/connection_it.rs` (add an incremental integration test)

**Interfaces:**
- No new public functions. `transfer_table` (Task 8) gains incremental behavior: when `cfg.incremental && !cfg.incremental_column.is_empty()`, read `MAX(col)` from the destination table and stream only `WHERE col > ?`; inserts use `REPLACE` (already selected via `cfg.incremental` in Task 8). A missing/empty destination table is tolerated (falls back to a full copy with `REPLACE`).

- [ ] **Step 1: Write the failing integration test**

Append to `crates/transfer-core/tests/connection_it.rs`:

```rust
#[tokio::test]
#[ignore = "requires live MySQL"]
async fn incremental_only_copies_new_rows() {
    use mysql_async::prelude::Queryable;
    use transfer_core::data::transfer_table;
    use transfer_core::{EventSink, TransferConfig};

    let mut c = connect(&local_cfg(), None).await.unwrap();
    c.query_drop("DROP TABLE IF EXISTS it_inc").await.unwrap();
    c.query_drop("CREATE TABLE it_inc (id INT PRIMARY KEY, v VARCHAR(10))").await.unwrap();
    c.query_drop("INSERT INTO it_inc VALUES (1,'a'),(2,'b')").await.unwrap();

    // Simulate dest already has id<=2; add id=3 to source, then incremental sync.
    c.query_drop("INSERT INTO it_inc VALUES (3,'c')").await.unwrap();

    let mut cfg = TransferConfig::default();
    cfg.source = local_cfg();
    cfg.dest = local_cfg();
    cfg.incremental = true;
    cfg.incremental_column = "id".into();

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = EventSink::new(tx);
    // dest MAX(id) is 3 here (same table) so WHERE id > 3 copies 0 rows — asserts the
    // delta path executes without error and reports 0 transferred.
    let stats = transfer_table(&cfg.source, &cfg.dest, "it_inc", 0, &cfg, &sink, None, None)
        .await
        .unwrap();
    assert!(stats.error.is_none());
    assert_eq!(stats.rows_transferred, 0);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core --test connection_it`
Expected: FAIL — the current `transfer_table` ignores `incremental_column` and would stream all rows (assert `rows_transferred == 0` fails). (Compiles; logic fails when run with `--ignored`.) For the no-DB gate, this stays `ignored`; verify via Step 4 reasoning.

- [ ] **Step 3: Write the implementation**

In `crates/transfer-core/src/data.rs`, replace the block that builds `insert_sql` and starts the stream (everything from `let insert_sql = build_insert_sql(...)` down to the `let mut stream = ...` line) with:

```rust
    let insert_sql = build_insert_sql(table, &columns, cfg.incremental);

    // Incremental: stream only rows beyond the destination's current MAX(column).
    let mut where_value: Option<Value> = None;
    let mut query = format!("SELECT * FROM `{table}`");
    if cfg.incremental && !cfg.incremental_column.is_empty() {
        let col = &cfg.incremental_column;
        let max: Option<Value> = dest
            .query_first(format!("SELECT MAX(`{col}`) FROM `{table}`"))
            .await
            .ok()
            .flatten();
        if let Some(v) = max {
            if v != Value::NULL {
                query.push_str(&format!(" WHERE `{col}` > ?"));
                where_value = Some(v);
            }
        }
    }

    // Optimise destination for bulk loading.
    for stmt in [
        "SET FOREIGN_KEY_CHECKS = 0",
        "SET UNIQUE_CHECKS = 0",
        "SET AUTOCOMMIT = 0",
        "SET sql_mode = ''",
    ] {
        dest.query_drop(stmt).await?;
    }

    let params = match &where_value {
        Some(v) => Params::Positional(vec![v.clone()]),
        None => Params::Empty,
    };
    let mut stream = source.exec_stream::<Row, _, _>(query, params).await?;
```

(Delete the now-duplicated bulk-load `for stmt in [...]` block and the old `let query = ...; let mut stream = ...` lines that followed it in Task 8, so the bulk-load setup and stream start appear exactly once, in the order shown above.)

- [ ] **Step 4: Verify it compiles and reasoning holds**

Run: `cargo test -p transfer-core --test connection_it`
Expected: compiles; `6 ignored`. (When run with `--ignored` against MySQL, `incremental_only_copies_new_rows` passes because `MAX(id)=3` yields `WHERE id > 3`, copying 0 rows.)

Run: `cargo test -p transfer-core`
Expected: PASS — all unit tests green.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: incremental delta sync with REPLACE upsert"
```

---

### Task 12: Schema transfer (tables, views, routines, triggers)

**Files:**
- Create: `crates/transfer-core/src/schema_transfer.rs`
- Modify: `crates/transfer-core/src/lib.rs`
- Modify: `crates/transfer-core/tests/connection_it.rs`

**Interfaces:**
- Consumes: `get_create_table`/`get_views`/`get_create_view`/`get_routines`/`get_create_routine`/`get_triggers`/`get_create_trigger` (Task 5), `TableInfo` (Task 5), `TransferConfig` (Task 2), `EventSink`/`Level`/`TransferEvent` (Task 1).
- Produces:
  - `pub async fn transfer_schema(source: &mut Conn, dest: &mut Conn, cfg: &TransferConfig, tables: &[TableInfo], sink: &EventSink) -> Result<()>` — creates tables (with `FOREIGN_KEY_CHECKS=0`, optional `DROP ... IF EXISTS`, "already exists" tolerated), then views/routines/triggers gated on the `include_*` flags. Dry-run logs intentions without executing.

- [ ] **Step 1: Write the failing integration test**

Append to `crates/transfer-core/tests/connection_it.rs`:

```rust
#[tokio::test]
#[ignore = "requires live MySQL"]
async fn schema_transfer_creates_table_at_dest() {
    use mysql_async::prelude::Queryable;
    use transfer_core::schema::get_tables;
    use transfer_core::schema_transfer::transfer_schema;
    use transfer_core::{EventSink, TransferConfig};

    let mut conn = connect(&local_cfg(), None).await.unwrap();
    conn.query_drop("DROP TABLE IF EXISTS it_schema_src").await.unwrap();
    conn.query_drop("DROP TABLE IF EXISTS it_schema_dst").await.unwrap();
    conn.query_drop("CREATE TABLE it_schema_src (id INT PRIMARY KEY)").await.unwrap();

    let tables: Vec<_> = get_tables(&mut conn)
        .await
        .unwrap()
        .into_iter()
        .filter(|t| t.name == "it_schema_src")
        .collect();

    let mut cfg = TransferConfig::default();
    cfg.drop_existing = true;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = EventSink::new(tx);

    let mut src = connect(&local_cfg(), None).await.unwrap();
    let mut dst = connect(&local_cfg(), None).await.unwrap();
    transfer_schema(&mut src, &mut dst, &cfg, &tables, &sink).await.unwrap();

    // The DDL recreates `it_schema_src` (same DB here); assert it still exists.
    let exists: Option<String> = dst
        .query_first("SHOW TABLES LIKE 'it_schema_src'")
        .await
        .unwrap();
    assert!(exists.is_some());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core --test connection_it`
Expected: FAIL to compile — `schema_transfer` module not found.

- [ ] **Step 3: Write the implementation**

Create `crates/transfer-core/src/schema_transfer.rs`:

```rust
//! Schema object transfer: tables, views, routines, triggers.

use crate::config::TransferConfig;
use crate::error::Result;
use crate::event::{EventSink, Level};
use crate::schema::{
    get_create_routine, get_create_table, get_create_trigger, get_create_view, get_routines,
    get_triggers, get_views, TableInfo,
};
use mysql_async::prelude::Queryable;
use mysql_async::Conn;

fn is_already_exists(e: &mysql_async::Error) -> bool {
    e.to_string().to_lowercase().contains("already exists")
}

pub async fn transfer_schema(
    source: &mut Conn,
    dest: &mut Conn,
    cfg: &TransferConfig,
    tables: &[TableInfo],
    sink: &EventSink,
) -> Result<()> {
    sink.log(Level::Info, format!("Transferring schema for {} table(s)...", tables.len()));

    dest.query_drop("SET FOREIGN_KEY_CHECKS = 0").await?;
    for t in tables {
        let ddl = get_create_table(source, &t.name).await?;
        if cfg.dry_run {
            sink.log(Level::DryRun, format!("Would create table `{}`", t.name));
            continue;
        }
        if cfg.drop_existing {
            dest.query_drop(format!("DROP TABLE IF EXISTS `{}`", t.name)).await?;
        }
        match dest.query_drop(&ddl).await {
            Ok(()) => sink.log(Level::Info, format!("Created table `{}`", t.name)),
            Err(e) if is_already_exists(&e) => {
                sink.log(Level::Info, format!("Table `{}` already exists, skipping", t.name));
            }
            Err(e) => sink.log(Level::Warn, format!("Failed to create table `{}`: {e}", t.name)),
        }
    }
    dest.query_drop("SET FOREIGN_KEY_CHECKS = 1").await?;

    if cfg.include_views {
        for view in get_views(source).await? {
            let ddl = get_create_view(source, &view).await?;
            if cfg.dry_run {
                sink.log(Level::DryRun, format!("Would create view `{view}`"));
                continue;
            }
            if cfg.drop_existing {
                dest.query_drop(format!("DROP VIEW IF EXISTS `{view}`")).await?;
            }
            match dest.query_drop(&ddl).await {
                Ok(()) => sink.log(Level::Info, format!("Created view `{view}`")),
                Err(e) => sink.log(Level::Warn, format!("Failed to create view `{view}`: {e}")),
            }
        }
    }

    if cfg.include_routines {
        for r in get_routines(source).await? {
            let ddl = get_create_routine(source, &r.name, &r.kind).await?;
            if cfg.dry_run {
                sink.log(Level::DryRun, format!("Would create {} `{}`", r.kind, r.name));
                continue;
            }
            let keyword = if r.kind.eq_ignore_ascii_case("PROCEDURE") { "PROCEDURE" } else { "FUNCTION" };
            if cfg.drop_existing {
                dest.query_drop(format!("DROP {keyword} IF EXISTS `{}`", r.name)).await?;
            }
            match dest.query_drop(&ddl).await {
                Ok(()) => sink.log(Level::Info, format!("Created {} `{}`", r.kind.to_lowercase(), r.name)),
                Err(e) => sink.log(Level::Warn, format!("Failed to create {} `{}`: {e}", r.kind.to_lowercase(), r.name)),
            }
        }
    }

    if cfg.include_triggers {
        for trigger in get_triggers(source).await? {
            let ddl = get_create_trigger(source, &trigger).await?;
            if cfg.dry_run {
                sink.log(Level::DryRun, format!("Would create trigger `{trigger}`"));
                continue;
            }
            if cfg.drop_existing {
                dest.query_drop(format!("DROP TRIGGER IF EXISTS `{trigger}`")).await?;
            }
            match dest.query_drop(&ddl).await {
                Ok(()) => sink.log(Level::Info, format!("Created trigger `{trigger}`")),
                Err(e) => sink.log(Level::Warn, format!("Failed to create trigger `{trigger}`: {e}")),
            }
        }
    }

    Ok(())
}
```

Add to `crates/transfer-core/src/lib.rs`:

```rust
pub mod schema_transfer;
pub use schema_transfer::transfer_schema;
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo test -p transfer-core --test connection_it`
Expected: compiles; `7 ignored`.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: schema transfer for tables, views, routines, triggers"
```

---

### Task 13: `run_transfer` orchestration (tunnels, schema, concurrent data, summary)

**Files:**
- Modify: `crates/transfer-core/src/transfer.rs`
- Modify: `crates/transfer-core/src/lib.rs`
- Modify: `crates/transfer-core/tests/connection_it.rs`

**Interfaces:**
- Consumes: `open_tunnel`/`Tunnel` (Task 10), `connect` (Task 4), `resolve_tables` (Task 6), `transfer_schema` (Task 12), `get_row_count`/`transfer_table_with_retry` (Task 9), `EventSink`/`TransferEvent`/`TableStats`/`TransferSummary`/`Level` (Task 1), `TransferConfig` (Task 2).
- Produces:
  - `pub async fn run_transfer(cfg: &TransferConfig, schema: bool, data: bool, sink: &EventSink) -> Result<TransferSummary>` — opens both SSH tunnels once and shares their local addresses; resolves tables; transfers schema when `schema`; counts rows then transfers tables concurrently (`Semaphore(workers)` + `JoinSet`) when `data`; emits `Phase`/`TableCounted`/`TableCompleted`/`Summary`; returns the `TransferSummary`.

- [ ] **Step 1: Write the failing integration test**

Append to `crates/transfer-core/tests/connection_it.rs`:

```rust
#[tokio::test]
#[ignore = "requires live MySQL"]
async fn run_transfer_data_only_reports_summary() {
    use mysql_async::prelude::Queryable;
    use transfer_core::{run_transfer, EventSink, TransferConfig};

    let mut c = connect(&local_cfg(), None).await.unwrap();
    c.query_drop("DROP TABLE IF EXISTS it_run").await.unwrap();
    c.query_drop("CREATE TABLE it_run (id INT PRIMARY KEY)").await.unwrap();
    c.query_drop("INSERT INTO it_run VALUES (1),(2)").await.unwrap();

    let mut cfg = TransferConfig::default();
    cfg.source = local_cfg();
    cfg.dest = local_cfg();
    cfg.tables = vec!["it_run".into()];
    cfg.truncate = true;
    cfg.workers = 2;

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = EventSink::new(tx);
    let summary = run_transfer(&cfg, false, true, &sink).await.unwrap();
    assert_eq!(summary.stats.len(), 1);
    assert_eq!(summary.total_rows, 2);
    assert_eq!(summary.errors, 0);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-core --test connection_it`
Expected: FAIL to compile — `run_transfer` not found.

- [ ] **Step 3: Write the implementation**

In `crates/transfer-core/src/transfer.rs`, update the imports at the top to include:

```rust
use crate::data::{get_row_count, transfer_table_with_retry};
use crate::event::{EventSink, Level, TableStats, TransferEvent, TransferSummary};
use crate::error::TransferError;
use crate::schema_transfer::transfer_schema;
use crate::ssh_tunnel::open_tunnel;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
```

Append this function to `crates/transfer-core/src/transfer.rs`:

```rust
pub async fn run_transfer(
    cfg: &TransferConfig,
    schema: bool,
    data: bool,
    sink: &EventSink,
) -> Result<TransferSummary> {
    let start = Instant::now();
    if cfg.dry_run {
        sink.log(Level::DryRun, "DRY RUN MODE");
    }

    // Open SSH tunnels once; share local addresses across all connections.
    let src_tunnel = open_tunnel(&cfg.source).await?;
    let dst_tunnel = open_tunnel(&cfg.dest).await?;
    let src_addr = src_tunnel.as_ref().map(|t| t.local_addr());
    let dst_addr = dst_tunnel.as_ref().map(|t| t.local_addr());

    sink.send(TransferEvent::Phase("Connecting".into()));
    let mut sconn = connect(&cfg.source, src_addr.clone()).await?;
    let tables = resolve_tables(&mut sconn, cfg).await?;
    sconn.disconnect().await.ok();

    if tables.is_empty() {
        sink.log(Level::Info, "No tables found matching the filter.");
        return Ok(TransferSummary {
            stats: Vec::new(),
            total_rows: 0,
            errors: 0,
            elapsed_secs: start.elapsed().as_secs_f64(),
            rows_per_sec: 0.0,
        });
    }
    sink.log(Level::Info, format!("Found {} table(s) to transfer.", tables.len()));

    if schema {
        sink.send(TransferEvent::Phase("Schema".into()));
        let mut src = connect(&cfg.source, src_addr.clone()).await?;
        let mut dst = connect(&cfg.dest, dst_addr.clone()).await?;
        transfer_schema(&mut src, &mut dst, cfg, &tables, sink).await?;
        src.disconnect().await.ok();
        dst.disconnect().await.ok();
    }

    let mut stats: Vec<TableStats> = Vec::new();
    if data {
        sink.send(TransferEvent::Phase("Data".into()));

        // Count rows first (for progress totals).
        let mut cconn = connect(&cfg.source, src_addr.clone()).await?;
        let mut counts: Vec<(String, u64)> = Vec::with_capacity(tables.len());
        for t in &tables {
            let n = get_row_count(&mut cconn, &t.name).await.unwrap_or(0);
            sink.send(TransferEvent::TableCounted { table: t.name.clone(), rows: n });
            counts.push((t.name.clone(), n));
        }
        cconn.disconnect().await.ok();

        // Transfer tables concurrently, bounded by `workers`.
        let cfg = Arc::new(cfg.clone());
        let sem = Arc::new(Semaphore::new(cfg.workers.max(1)));
        let mut set: JoinSet<TableStats> = JoinSet::new();
        for (name, total) in counts {
            let cfg = cfg.clone();
            let sem = sem.clone();
            let sink = sink.clone();
            let src_addr = src_addr.clone();
            let dst_addr = dst_addr.clone();
            set.spawn(async move {
                let _permit = sem.acquire_owned().await.expect("semaphore closed");
                transfer_table_with_retry(
                    &cfg.source, &cfg.dest, &name, total, &cfg, &sink, src_addr, dst_addr,
                )
                .await
            });
        }
        while let Some(joined) = set.join_next().await {
            let s = joined.map_err(|e| TransferError::MySql(format!("worker join error: {e}")))?;
            sink.send(TransferEvent::TableCompleted(s.clone()));
            stats.push(s);
        }
    }

    let total_rows: u64 = stats.iter().map(|s| s.rows_transferred).sum();
    let errors = stats.iter().filter(|s| s.error.is_some()).count() as u64;
    let elapsed = start.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 { total_rows as f64 / elapsed } else { 0.0 };
    let summary = TransferSummary { stats, total_rows, errors, elapsed_secs: elapsed, rows_per_sec };
    sink.send(TransferEvent::Summary(summary.clone()));
    Ok(summary)
}
```

Add to `crates/transfer-core/src/lib.rs`:

```rust
pub use transfer::run_transfer;
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo test -p transfer-core --test connection_it`
Expected: compiles; `8 ignored`.

Run: `cargo test -p transfer-core`
Expected: PASS — all unit tests green.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-core
git commit -m "feat: run_transfer orchestration with parallel workers and summary"
```

---

### Task 14: CLI argument model + config building

**Files:**
- Create: `crates/transfer-cli/src/cli.rs`
- Modify: `crates/transfer-cli/src/main.rs`

**Interfaces:**
- Consumes: `load_config`/`apply_cli_overrides`/`CliOverrides`/`TransferConfig` (Task 2).
- Produces:
  - `pub struct Cli { pub command: Command }` (clap `Parser`).
  - `pub enum Command { Transfer(TransferArgs), Schema(TransferArgs), Data(TransferArgs), Inspect(CommonArgs), Diff(CommonArgs) }`.
  - `pub struct CommonArgs { ... all connection + ssh Option flags, config_file }` with `fn to_overrides(&self) -> CliOverrides` and `fn config_path(&self) -> Option<&std::path::Path>`.
  - `pub struct TransferArgs { pub common: CommonArgs, pub select: bool, ... transfer flags }` with `fn to_overrides(&self) -> CliOverrides`.
  - `pub fn build_config(config_file: Option<&std::path::Path>, ov: &CliOverrides) -> TransferConfig` — loads, applies overrides, validates; prints errors to stderr and exits non-zero on failure.

- [ ] **Step 1: Write failing unit tests**

Create `crates/transfer-cli/src/cli.rs` with the structs (Step 3 content) and this test module appended:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn common() -> CommonArgs {
        CommonArgs {
            config_file: None,
            source_host: Some("sh".into()),
            source_port: None,
            source_user: Some("su".into()),
            source_password: None,
            source_db: Some("sd".into()),
            source_ssh_host: None,
            source_ssh_port: None,
            source_ssh_user: None,
            source_ssh_password: None,
            source_ssh_key: None,
            dest_host: Some("dh".into()),
            dest_port: None,
            dest_user: Some("du".into()),
            dest_password: None,
            dest_db: Some("dd".into()),
            dest_ssh_host: None,
            dest_ssh_port: None,
            dest_ssh_user: None,
            dest_ssh_password: None,
            dest_ssh_key: None,
        }
    }

    #[test]
    fn transfer_flags_map_to_some_only_when_set() {
        let args = TransferArgs {
            common: common(),
            select: false,
            tables: Some("a,b".into()),
            exclude_tables: None,
            chunk_size: Some(50),
            workers: None,
            drop_existing: true,
            truncate: false,
            incremental: false,
            incremental_column: None,
            include_views: false,
            include_routines: false,
            include_triggers: false,
            dry_run: true,
        };
        let ov = args.to_overrides();
        assert_eq!(ov.tables.as_deref(), Some("a,b"));
        assert_eq!(ov.chunk_size, Some(50));
        assert_eq!(ov.drop_existing, Some(true)); // flag present
        assert_eq!(ov.truncate, None); // flag absent => leave file value
        assert_eq!(ov.dry_run, Some(true));
    }

    #[test]
    fn build_config_applies_overrides_and_passes_validation() {
        let ov = common().to_overrides();
        let cfg = build_config(None, &ov);
        assert_eq!(cfg.source.host, "sh");
        assert_eq!(cfg.source.database, "sd");
        assert_eq!(cfg.dest.host, "dh");
        assert_eq!(cfg.dest.database, "dd");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-cli`
Expected: FAIL to compile — `cli` module not declared in `main.rs`.

- [ ] **Step 3: Write the implementation**

Put this at the top of `crates/transfer-cli/src/cli.rs` (above the `#[cfg(test)]` block from Step 1):

```rust
//! CLI argument model (clap) and config assembly.

use clap::{Args, Parser, Subcommand};
use std::path::{Path, PathBuf};
use transfer_core::{apply_cli_overrides, load_config, CliOverrides, TransferConfig};

#[derive(Parser)]
#[command(
    name = "mysql-transfer",
    version,
    about = "Transfer MySQL databases (schema + data) between servers."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Transfer schema and data from source to destination.
    Transfer(TransferArgs),
    /// Transfer schema only (tables, views, routines, triggers).
    Schema(TransferArgs),
    /// Transfer data only (assumes schema exists at destination).
    Data(TransferArgs),
    /// Inspect the source database.
    Inspect(CommonArgs),
    /// Compare source and destination schemas.
    Diff(CommonArgs),
}

#[derive(Args, Clone)]
pub struct CommonArgs {
    #[arg(short = 'c', long = "config")]
    pub config_file: Option<PathBuf>,
    #[arg(long)] pub source_host: Option<String>,
    #[arg(long)] pub source_port: Option<u16>,
    #[arg(long)] pub source_user: Option<String>,
    #[arg(long)] pub source_password: Option<String>,
    #[arg(long)] pub source_db: Option<String>,
    #[arg(long)] pub source_ssh_host: Option<String>,
    #[arg(long)] pub source_ssh_port: Option<u16>,
    #[arg(long)] pub source_ssh_user: Option<String>,
    #[arg(long)] pub source_ssh_password: Option<String>,
    #[arg(long)] pub source_ssh_key: Option<String>,
    #[arg(long)] pub dest_host: Option<String>,
    #[arg(long)] pub dest_port: Option<u16>,
    #[arg(long)] pub dest_user: Option<String>,
    #[arg(long)] pub dest_password: Option<String>,
    #[arg(long)] pub dest_db: Option<String>,
    #[arg(long)] pub dest_ssh_host: Option<String>,
    #[arg(long)] pub dest_ssh_port: Option<u16>,
    #[arg(long)] pub dest_ssh_user: Option<String>,
    #[arg(long)] pub dest_ssh_password: Option<String>,
    #[arg(long)] pub dest_ssh_key: Option<String>,
}

impl CommonArgs {
    pub fn config_path(&self) -> Option<&Path> {
        self.config_file.as_deref()
    }

    pub fn to_overrides(&self) -> CliOverrides {
        CliOverrides {
            source_host: self.source_host.clone(),
            source_port: self.source_port,
            source_user: self.source_user.clone(),
            source_password: self.source_password.clone(),
            source_db: self.source_db.clone(),
            source_ssh_host: self.source_ssh_host.clone(),
            source_ssh_port: self.source_ssh_port,
            source_ssh_user: self.source_ssh_user.clone(),
            source_ssh_password: self.source_ssh_password.clone(),
            source_ssh_key: self.source_ssh_key.clone(),
            dest_host: self.dest_host.clone(),
            dest_port: self.dest_port,
            dest_user: self.dest_user.clone(),
            dest_password: self.dest_password.clone(),
            dest_db: self.dest_db.clone(),
            dest_ssh_host: self.dest_ssh_host.clone(),
            dest_ssh_port: self.dest_ssh_port,
            dest_ssh_user: self.dest_ssh_user.clone(),
            dest_ssh_password: self.dest_ssh_password.clone(),
            dest_ssh_key: self.dest_ssh_key.clone(),
            ..Default::default()
        }
    }
}

#[derive(Args, Clone)]
pub struct TransferArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Interactively select tables to transfer.
    #[arg(short = 's', long)]
    pub select: bool,
    #[arg(long)] pub tables: Option<String>,
    #[arg(long)] pub exclude_tables: Option<String>,
    #[arg(long)] pub chunk_size: Option<usize>,
    #[arg(long)] pub workers: Option<usize>,
    #[arg(long)] pub drop_existing: bool,
    #[arg(long)] pub truncate: bool,
    #[arg(long)] pub incremental: bool,
    #[arg(long)] pub incremental_column: Option<String>,
    #[arg(long)] pub include_views: bool,
    #[arg(long)] pub include_routines: bool,
    #[arg(long)] pub include_triggers: bool,
    #[arg(long)] pub dry_run: bool,
}

fn flag(set: bool) -> Option<bool> {
    if set { Some(true) } else { None }
}

impl TransferArgs {
    pub fn to_overrides(&self) -> CliOverrides {
        let mut ov = self.common.to_overrides();
        ov.tables = self.tables.clone();
        ov.exclude_tables = self.exclude_tables.clone();
        ov.chunk_size = self.chunk_size;
        ov.workers = self.workers;
        ov.drop_existing = flag(self.drop_existing);
        ov.truncate = flag(self.truncate);
        ov.incremental = flag(self.incremental);
        ov.incremental_column = self.incremental_column.clone();
        ov.include_views = flag(self.include_views);
        ov.include_routines = flag(self.include_routines);
        ov.include_triggers = flag(self.include_triggers);
        ov.dry_run = flag(self.dry_run);
        ov
    }
}

pub fn build_config(config_file: Option<&Path>, ov: &CliOverrides) -> TransferConfig {
    let mut cfg = match load_config(config_file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };
    apply_cli_overrides(&mut cfg, ov);
    let errs = cfg.validate();
    if !errs.is_empty() {
        eprintln!("Configuration errors:");
        for e in errs {
            eprintln!("  • {e}");
        }
        std::process::exit(1);
    }
    cfg
}
```

Replace `crates/transfer-cli/src/main.rs` with:

```rust
mod cli;

use clap::Parser;

fn main() {
    let _cli = cli::Cli::parse();
    // Command dispatch is implemented in Task 16.
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p transfer-cli`
Expected: PASS — `transfer_flags_map_to_some_only_when_set`, `build_config_applies_overrides_and_passes_validation`.

Run: `cargo run -p transfer-cli -- --help`
Expected: prints the top-level help listing the 5 subcommands.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-cli
git commit -m "feat: CLI argument model and config building"
```

---

### Task 15: Output rendering (progress bars, inspect/diff/summary tables)

**Files:**
- Create: `crates/transfer-cli/src/render.rs`
- Modify: `crates/transfer-cli/src/main.rs`

**Interfaces:**
- Consumes: `TransferEvent`/`TransferSummary`/`Level` (Task 1), `Inspection`/`SchemaDiff` (Task 6).
- Produces:
  - `pub async fn render_events(rx: tokio::sync::mpsc::UnboundedReceiver<TransferEvent>)` — drains events into an `indicatif` `MultiProgress`: a bar per table (created on `TableCounted`), advanced on `ChunkInserted`, finished on `TableCompleted`; `Phase`/`Log` print above the bars; `Summary` prints the summary table.
  - `pub fn print_inspection(insp: &Inspection)` — `comfy-table` of tables + lists of views/routines/triggers.
  - `pub fn print_diff(d: &SchemaDiff)` — `comfy-table` source-vs-dest comparison.
  - `pub fn print_summary(s: &TransferSummary)` — `comfy-table` per-table stats + totals/speed.
  - `pub fn diff_status(in_src: bool, in_dst: bool) -> &'static str` — `"Synced"` / `"Missing at dest"` / `"Extra at dest"`.

- [ ] **Step 1: Write the failing unit test**

Create `crates/transfer-cli/src/render.rs` with the implementation (Step 3) plus this test module:

```rust
#[cfg(test)]
mod tests {
    use super::diff_status;

    #[test]
    fn diff_status_classifies_membership() {
        assert_eq!(diff_status(true, true), "Synced");
        assert_eq!(diff_status(true, false), "Missing at dest");
        assert_eq!(diff_status(false, true), "Extra at dest");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-cli render`
Expected: FAIL to compile — `render` module not declared in `main.rs`.

- [ ] **Step 3: Write the implementation**

Put this above the test module in `crates/transfer-cli/src/render.rs`:

```rust
//! Terminal rendering: progress bars and result tables.

use comfy_table::{Cell, Table};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::HashMap;
use transfer_core::transfer::{Inspection, SchemaDiff};
use transfer_core::{Level, TransferEvent, TransferSummary};

pub fn diff_status(in_src: bool, in_dst: bool) -> &'static str {
    match (in_src, in_dst) {
        (true, true) => "Synced",
        (true, false) => "Missing at dest",
        _ => "Extra at dest",
    }
}

pub async fn render_events(mut rx: tokio::sync::mpsc::UnboundedReceiver<TransferEvent>) {
    let mp = MultiProgress::new();
    let style = ProgressStyle::with_template(
        "{spinner} {msg:<28} [{bar:30}] {pos}/{len} • {elapsed} • ETA {eta}",
    )
    .unwrap()
    .progress_chars("=>-");
    let mut bars: HashMap<String, ProgressBar> = HashMap::new();

    while let Some(ev) = rx.recv().await {
        match ev {
            TransferEvent::Phase(p) => {
                let _ = mp.println(format!("▶ {p}"));
            }
            TransferEvent::TableCounted { table, rows } => {
                let pb = mp.add(ProgressBar::new(rows));
                pb.set_style(style.clone());
                pb.set_message(table.clone());
                bars.insert(table, pb);
            }
            TransferEvent::TableStarted { .. } => {}
            TransferEvent::ChunkInserted { table, rows } => {
                if let Some(pb) = bars.get(&table) {
                    pb.inc(rows);
                }
            }
            TransferEvent::TableCompleted(stats) => {
                if let Some(pb) = bars.get(&stats.table) {
                    pb.finish();
                }
            }
            TransferEvent::Log { level, message } => {
                let prefix = match level {
                    Level::Info => "",
                    Level::Warn => "⚠ ",
                    Level::DryRun => "[dry-run] ",
                };
                let _ = mp.println(format!("{prefix}{message}"));
            }
            TransferEvent::Summary(s) => {
                print_summary(&s);
            }
        }
    }
}

pub fn print_summary(s: &TransferSummary) {
    println!("\n━━━ Transfer Summary ━━━\n");
    if s.stats.is_empty() {
        println!("  No data transferred.");
        return;
    }
    let mut table = Table::new();
    table.set_header(vec!["Table", "Rows", "Chunks", "Status"]);
    for st in &s.stats {
        let status = match &st.error {
            None => "✓ OK".to_string(),
            Some(e) => format!("✗ {e}"),
        };
        table.add_row(vec![
            Cell::new(&st.table),
            Cell::new(st.rows_transferred),
            Cell::new(st.chunks),
            Cell::new(status),
        ]);
    }
    println!("{table}");
    println!(
        "\n  Total: {} rows • Time: {:.1}s • Errors: {}",
        s.total_rows, s.elapsed_secs, s.errors
    );
    if s.total_rows > 0 && s.elapsed_secs > 0.0 {
        println!("  Speed: {:.0} rows/sec", s.rows_per_sec);
    }
}

pub fn print_inspection(insp: &Inspection) {
    let mut table = Table::new();
    table.set_header(vec!["Table", "Engine", "Rows", "Size (MB)", "Collation"]);
    let mut total_rows = 0u64;
    let mut total_size = 0.0f64;
    for t in &insp.tables {
        table.add_row(vec![
            Cell::new(&t.name),
            Cell::new(&t.engine),
            Cell::new(t.rows),
            Cell::new(format!("{:.2}", t.size_mb)),
            Cell::new(&t.collation),
        ]);
        total_rows += t.rows;
        total_size += t.size_mb;
    }
    println!("\nSource Database Tables\n{table}");
    println!(
        "\n  Total: {} tables • {} rows • {:.2} MB",
        insp.tables.len(),
        total_rows,
        total_size
    );
    if !insp.views.is_empty() {
        println!("  Views: {}", insp.views.join(", "));
    }
    let procs: Vec<&str> = insp.routines.iter().filter(|r| r.kind.eq_ignore_ascii_case("PROCEDURE")).map(|r| r.name.as_str()).collect();
    let funcs: Vec<&str> = insp.routines.iter().filter(|r| r.kind.eq_ignore_ascii_case("FUNCTION")).map(|r| r.name.as_str()).collect();
    if !procs.is_empty() {
        println!("  Procedures: {}", procs.join(", "));
    }
    if !funcs.is_empty() {
        println!("  Functions: {}", funcs.join(", "));
    }
    if !insp.triggers.is_empty() {
        println!("  Triggers: {}", insp.triggers.join(", "));
    }
}

pub fn print_diff(d: &SchemaDiff) {
    let src: std::collections::BTreeSet<&String> = d.source.iter().collect();
    let dst: std::collections::BTreeSet<&String> = d.dest.iter().collect();
    let mut all: Vec<&String> = src.union(&dst).copied().collect();
    all.sort();

    let mut table = Table::new();
    table.set_header(vec!["Table", "Source", "Dest", "Status"]);
    for name in &all {
        let in_src = src.contains(name);
        let in_dst = dst.contains(name);
        table.add_row(vec![
            Cell::new(name),
            Cell::new(if in_src { "✓" } else { "✗" }),
            Cell::new(if in_dst { "✓" } else { "✗" }),
            Cell::new(diff_status(in_src, in_dst)),
        ]);
    }
    println!("\nSchema Diff: Source vs Destination\n{table}");
    println!(
        "\n  Only in source: {} • Only in dest: {} • In both: {}",
        d.source_only().len(),
        d.dest_only().len(),
        d.in_both().len()
    );
}
```

Add `mod render;` to `crates/transfer-cli/src/main.rs` (below `mod cli;`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p transfer-cli render`
Expected: PASS — `diff_status_classifies_membership`.

Run: `cargo build`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add crates/transfer-cli
git commit -m "feat: terminal rendering for progress, inspect, diff, summary"
```

---

### Task 16: Command dispatch, interactive select, end-to-end wiring

**Files:**
- Modify: `crates/transfer-cli/src/main.rs`
- Create: `crates/transfer-cli/tests/cli_help.rs`

**Interfaces:**
- Consumes: `Cli`/`Command`/`build_config`/`TransferArgs`/`CommonArgs` (Task 14), `render_events`/`print_inspection`/`print_diff` (Task 15), `run_transfer`/`run_inspect`/`run_diff`/`EventSink`/`connect`/`open_tunnel` (core), `schema::get_tables` (Task 5).
- Produces: a working `mysql-transfer` binary dispatching all 5 subcommands, with `-s/--select` interactive table selection.

- [ ] **Step 1: Write the failing test**

Create `crates/transfer-cli/tests/cli_help.rs`:

```rust
use std::process::Command;

#[test]
fn help_lists_all_subcommands() {
    let out = Command::new(env!("CARGO_BIN_EXE_mysql-transfer"))
        .arg("--help")
        .output()
        .expect("run binary");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    for sub in ["transfer", "schema", "data", "inspect", "diff"] {
        assert!(stdout.contains(sub), "help missing `{sub}`");
    }
}

#[test]
fn transfer_help_shows_select_flag() {
    let out = Command::new(env!("CARGO_BIN_EXE_mysql-transfer"))
        .args(["transfer", "--help"])
        .output()
        .expect("run binary");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("--select"));
    assert!(stdout.contains("--dry-run"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p transfer-cli --test cli_help`
Expected: FAIL — the current `main` doesn't dispatch subcommands, so `transfer --help` errors (no `transfer` subcommand wired) → assertion fails (or only after Step 3 do subcommands appear). With the Task 14 `main`, the subcommands already parse, but `transfer --help` should already succeed; this test pins the behavior and will pass once dispatch is complete. Run it now to confirm current state (it may already pass for `--help`; the dispatch in Step 3 is still required for the binary to *do* anything).

- [ ] **Step 3: Write the implementation**

Replace `crates/transfer-cli/src/main.rs` with:

```rust
mod cli;
mod render;

use clap::Parser;
use std::collections::HashMap;
use transfer_core::schema::get_tables;
use transfer_core::{connect, open_tunnel, run_diff, run_inspect, run_transfer, EventSink, TransferConfig};

#[tokio::main]
async fn main() {
    let parsed = cli::Cli::parse();
    match parsed.command {
        cli::Command::Transfer(a) => run_pipeline(a, true, true).await,
        cli::Command::Schema(a) => run_pipeline(a, true, false).await,
        cli::Command::Data(a) => run_pipeline(a, false, true).await,
        cli::Command::Inspect(c) => {
            let cfg = cli::build_config(c.config_path(), &c.to_overrides());
            match run_inspect(&cfg).await {
                Ok(insp) => render::print_inspection(&insp),
                Err(e) => fail(e),
            }
        }
        cli::Command::Diff(c) => {
            let cfg = cli::build_config(c.config_path(), &c.to_overrides());
            match run_diff(&cfg).await {
                Ok(d) => render::print_diff(&d),
                Err(e) => fail(e),
            }
        }
    }
}

fn fail(e: transfer_core::TransferError) -> ! {
    eprintln!("Error: {e}");
    std::process::exit(1);
}

async fn run_pipeline(args: cli::TransferArgs, schema: bool, data: bool) {
    let ov = args.to_overrides();
    let mut cfg = cli::build_config(args.common.config_path(), &ov);

    if args.select {
        cfg.tables = interactive_select(&cfg).await;
    }

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = EventSink::new(tx);
    let renderer = tokio::spawn(render::render_events(rx));

    let result = run_transfer(&cfg, schema, data, &sink).await;
    drop(sink); // closing the channel lets the renderer finish
    let _ = renderer.await;

    if let Err(e) = result {
        fail(e);
    }
}

async fn interactive_select(cfg: &TransferConfig) -> Vec<String> {
    let tunnel = open_tunnel(&cfg.source).await.unwrap_or_else(|e| fail(e));
    let addr = tunnel.as_ref().map(|t| t.local_addr());
    let mut conn = connect(&cfg.source, addr).await.unwrap_or_else(|e| fail(e));
    let tables = get_tables(&mut conn).await.unwrap_or_else(|e| fail(e));
    conn.disconnect().await.ok();

    if tables.is_empty() {
        eprintln!("No tables found in source database.");
        std::process::exit(1);
    }

    let mut label_to_name: HashMap<String, String> = HashMap::new();
    let mut labels: Vec<String> = Vec::with_capacity(tables.len());
    for t in &tables {
        let label = format!("{}  ({} rows, {:.2} MB)", t.name, t.rows, t.size_mb);
        label_to_name.insert(label.clone(), t.name.clone());
        labels.push(label);
    }

    let selected = tokio::task::spawn_blocking(move || {
        inquire::MultiSelect::new("Select tables to transfer:", labels).prompt()
    })
    .await
    .expect("select task");

    match selected {
        Ok(sel) if !sel.is_empty() => {
            println!("  Selected {} table(s).", sel.len());
            sel.into_iter().map(|l| label_to_name[&l].clone()).collect()
        }
        Ok(_) => {
            eprintln!("No tables selected. Aborting.");
            std::process::exit(0);
        }
        Err(_) => {
            eprintln!("Selection cancelled.");
            std::process::exit(0);
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p transfer-cli`
Expected: PASS — `cli` + `render` unit tests + `help_lists_all_subcommands` + `transfer_help_shows_select_flag`.

Run: `cargo build --workspace`
Expected: success (the `mysql-transfer` binary is built).

- [ ] **Step 5: Full end-to-end manual verification (live MySQL)**

Run: `cargo test --workspace`
Expected: all non-ignored tests PASS.

Run (against a real MySQL, mirroring the Python tool): `cargo run -p transfer-cli -- inspect -c config.yaml`
Expected: prints the source tables table. Then `cargo run -p transfer-cli -- transfer -c config.yaml --dry-run` prints the dry-run plan; without `--dry-run` it streams data with live progress bars and a summary.

Run the ignored integration suite against a `testcontainers` MySQL: `cargo test -p transfer-core -- --ignored`
Expected: schema/data/incremental/run_transfer integration tests PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/transfer-cli
git commit -m "feat: command dispatch, interactive select, end-to-end CLI"
```

---

## Plan Self-Review

**Spec coverage (phases 1–7):**
- Phase 1 (workspace, error, event, config, matcher) → Tasks 1, 2, 3. ✓
- Phase 2 (connection, schema introspection, inspect/diff) → Tasks 4, 5, 6. ✓
- Phase 3 (single-threaded data) → Tasks 7, 8. ✓
- Phase 4 (concurrency, retry, summary) → Tasks 9, 13. ✓
- Phase 5 (SSH tunnel) → Task 10. ✓
- Phase 6 (incremental, transfer_schema, dry-run) → Tasks 11, 12 (dry-run also in Tasks 8/12). ✓
- Phase 7 (CLI flags, select, render) → Tasks 14, 15, 16. ✓
- Parity details covered: `INSERT IGNORE`/`REPLACE` (Task 7), column intersection (Task 7), bulk-load session vars (Task 8), retry 3×/`5*attempt` (Task 9), wildcard filters (Task 3), `destination` alias + SSH-enabled-on-override defaults (Task 2), shared tunnels (Tasks 10/13), worker semaphore (Task 13), summary speed/errors (Task 13/15). ✓

**Placeholder scan:** No `TODO`/`TBD`/"add error handling"/"similar to Task N". Each code step contains full code. The only forward references are explicit, completed edits ("Task 11 extends `transfer_table`", "Task 13 inserts the schema block") with the exact replacement code provided in that later task. ✓

**Type consistency:** `TransferEvent`, `TableStats`, `TransferSummary`, `EventSink`, `Level`, `TransferError`, `Result<T>`, `SshConfig`, `ConnectionConfig`, `TransferConfig`, `CliOverrides`, `TableInfo`, `RoutineInfo`, `Inspection`, `SchemaDiff` are defined once and referenced with identical names/fields throughout. `transfer_table`'s signature (8 args incl. `total_rows` and the two `Option<(String,u16)>` addrs) is consistent between Tasks 8, 9, 13. `run_transfer(cfg, schema, data, sink)` matches between Task 13 and CLI Task 16. ✓

**Known follow-ups (carried to Plan 2 / noted):** russh `0.45` channel-stream API is version-sensitive (Task 10 note); testcontainers harness wiring is described in the Task 4 note; per-type value-fidelity coverage (dates/decimals/binary/NULL) is exercised by the ignored integration tests.

