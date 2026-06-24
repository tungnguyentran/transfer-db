# Design: Port `mysql-transfer` to Rust with a SwiftUI macOS frontend

- **Date:** 2026-06-24
- **Status:** Approved (brainstorming)
- **Owner:** Nguyen Tran Tung
- **Supersedes:** N/A (new product line, separate repo)

## 1. Summary

Re-implement the existing Python `mysql-transfer` CLI as a native **Rust core** with **full feature parity**, and build a native **SwiftUI macOS app** on top of it. The Rust engine is exposed to Swift through a UniFFI binding layer packaged as an XCFramework. The same core also backs a Rust CLI, so there is exactly one engine with two frontends (CLI + GUI).

This work lives in a **new, separate git repository** (`mysql-transfer-rs`), defaulting to a sibling of the current project at `/Users/tungnt1/Projects/TungXoan/mysql-transfer-rs`. The existing Python project is left untouched as a reference implementation. This design document is committed in the *current* (Python) repo because the new repo does not exist yet.

### Goals

- Full parity with the Python tool: 5 commands (transfer, schema, data, inspect, diff), SSH tunnels, incremental sync, views/routines/triggers, dry-run, wildcard table filters, parallel transfer, chunked streaming.
- A single self-contained binary (no Python/uv runtime).
- Real async concurrency (no GIL); lower memory; faster per-row marshalling.
- A reusable, UI-agnostic core that a native SwiftUI app consumes via FFI.

### Non-goals (v1)

- No features beyond Python parity (no resume/checkpoint, no config encryption, no schema migration/diff-apply).
- No code-signing, notarization, or App Store distribution — a local unsigned `.app` is acceptable.
- macOS-only — no iOS, no Windows, no Linux GUI (the CLI is naturally cross-platform but only macOS is a target for v1 validation).
- No auto-update.

## 2. Motivations (from brainstorming)

1. **Native UI foundation** — a Rust core wrapped by a native SwiftUI desktop app.
2. **Performance / memory** — true parallelism without the GIL, lower memory, faster row handling.
3. **Learning / maintainability** — a statically-typed, compiled codebase.

## 3. Technology choices

| Concern | Choice | Rationale |
|---|---|---|
| Async runtime | `tokio` | DB transfer is I/O-bound; async gives natural concurrency for N parallel tables and row streaming, and is the foundation an async UI/FFI expects. |
| MySQL driver | `mysql_async` | Purpose-built MySQL driver; native streaming of result sets (the `SSDictCursor` analog); runs arbitrary DDL/DML (what a transfer tool needs). `sqlx` targets compile-time-checked static queries, which fights a dynamic transfer tool. |
| SSH tunnel | `russh` (pure Rust) | Replicates `sshtunnel`: one SSH session, local TCP forward to remote MySQL. Pure-Rust keeps the single-binary goal (no libssh2/C dependency). |
| CLI parsing | `clap` (derive) | Direct analog of `click`; derive macros keep 5 commands + shared options clean. |
| Config | `serde` + `serde_yaml` | YAML → typed structs, 1:1 with the Python dataclasses. |
| CLI progress/output | `indicatif` + `comfy-table` + `console` | Multi-bar progress + summary/inspect/diff tables (the `rich` analog). CLI crate only. |
| Interactive table select | `inquire` | The `InquirerPy` fuzzy multi-select analog for `-s/--select`. |
| Error types | `thiserror` (core), `anyhow` (CLI) | Library returns typed errors; binary adds context + exit codes. |
| Swift↔Rust FFI | **UniFFI** | Mature (Firefox); auto-generates idiomatic Swift for records, enums, errors, async functions, and callback interfaces. (`swift-bridge` is the fallback alternative.) |
| Swift YAML | `Yams` | Lets the SwiftUI app read/write the same `config.yaml` schema as the CLI. |

Rejected alternative: a **sync** stack (`mysql` + `rayon` + `ssh2`/shell-out). Simpler mental model and closer to `ThreadPoolExecutor`, but blocking, needs a C library for SSH (hurts single-binary), and is a worse base for an async UI/FFI. Not chosen given the performance + UI-foundation goals.

## 4. Repository & workspace structure

```
mysql-transfer-rs/                 # new git repo (monorepo: Rust + Swift)
  Cargo.toml                       # [workspace]
  config.example.yaml              # same schema as today
  build-xcframework.sh             # universal static lib -> MySQLTransfer.xcframework
  crates/
    transfer-core/                 # library — the engine, UI-agnostic, never prints
      src/
        lib.rs
        config.rs
        error.rs
        event.rs
        connection.rs
        ssh_tunnel.rs
        schema.rs
        data.rs
        transfer.rs                # orchestration (transfer/inspect/diff)
    transfer-cli/                  # binary — clap + indicatif/comfy-table
      src/
        main.rs
        cli.rs
        render.rs                  # drains core events into indicatif/comfy-table
    transfer-ffi/                  # cdylib/staticlib — UniFFI bindings over transfer-core
      src/lib.rs
      uniffi.toml
  bindings/swift/                  # generated Swift glue + module map (build output)
  app/                             # SwiftUI macOS app (Xcode project)
    MySQLTransfer.xcodeproj
    Sources/
      App/                         # @main App, windows
      Models/                      # config view models, engine service
      Views/                       # Connections, Options, Tables, Transfer, Diff
      Services/EngineService.swift # wraps the FFI, marshals events to main actor
    Frameworks/MySQLTransfer.xcframework  # built artifact
```

Both frontends link the **same `transfer-core`**: the CLI as a direct crate dependency, Swift through `transfer-ffi` → XCFramework.

## 5. Core architecture: the event seam

The core **never prints**. It emits progress as structured events through a sink; each frontend renders them.

```rust
// event.rs
pub enum Level { Info, Warn, DryRun }

pub enum TransferEvent {
    Phase(String),                                   // "Connecting", "Schema", "Data"
    TableCounted   { table: String, rows: u64 },
    TableStarted   { table: String, total_rows: u64 },
    ChunkInserted  { table: String, rows: u64 },     // drives a progress bar
    TableCompleted(TableStats),
    Log            { level: Level, message: String },
    Summary(TransferSummary),
}

pub struct TableStats {
    pub table: String,
    pub rows_transferred: u64,
    pub chunks: u64,
    pub error: Option<String>,                       // per-table failure, run continues
}

pub struct TransferSummary {
    pub stats: Vec<TableStats>,
    pub total_rows: u64,
    pub errors: u64,
    pub elapsed_secs: f64,
    pub rows_per_sec: f64,
}
```

- In Rust, `EventSink` wraps a `tokio::sync::mpsc::Sender<TransferEvent>`.
- The **CLI** spawns a render task draining the channel into `indicatif` multi-progress and logging.
- The **FFI/Swift** path turns the sink into calls on a `TransferListener` callback interface; SwiftUI updates views from those events.

`inspect` and `diff` need no streaming — they return plain result structs the frontends format directly.

```rust
// transfer.rs — orchestration entry points (async)
pub async fn run_transfer(cfg: &TransferConfig, schema: bool, data: bool, sink: &EventSink)
    -> Result<TransferSummary, TransferError>;
pub async fn run_inspect(cfg: &TransferConfig) -> Result<Inspection, TransferError>;
pub async fn run_diff(cfg: &TransferConfig)    -> Result<SchemaDiff, TransferError>;

// Inspection mirrors run_inspect's data: tables (name/rows/size/engine/collation),
// views, routines (name/type), triggers.
// SchemaDiff: source-only, dest-only, in-both table name sets.
```

## 6. Module-by-module parity

### 6.1 `config.rs`

- `SshConfig`, `ConnectionConfig`, `TransferConfig` as `serde` structs with the **same fields and defaults** as the Python dataclasses (see `config.example.yaml`).
- Accept both `dest` and `destination` keys (Python does).
- `load_config(path) -> Result<TransferConfig>` via `serde_yaml`.
- `apply_cli_overrides(cfg, overrides)` — only non-`None` CLI values override; setting any SSH override implies `ssh.enabled = true` (matches Python).
- `validate(&self) -> Vec<String>` — same rules: source/dest require host+database+user; SSH requires host+user+(password|key_file); `incremental` requires `incremental_column`; `chunk_size >= 1`; `workers >= 1`.

### 6.2 `connection.rs` + `ssh_tunnel.rs`

- `russh` opens **one** SSH session per endpoint (source, dest), shared via `Arc`.
- A local TCP forwarder task listens on `127.0.0.1:0`; each accepted socket opens a fresh `direct-tcpip` channel to the remote MySQL `host:port` — mirroring `sshtunnel`. `mysql_async` connects to `127.0.0.1:<local_port>`.
- Connection options: charset from config; `connect_timeout`. After connect, run `SET SESSION net_read_timeout=600`, `net_write_timeout=600`, `wait_timeout=28800` (best-effort, ignore failures — Python does).
- Streaming reads use `mysql_async`'s native result streaming (no separate cursor class).
- `test_connection(cfg) -> (bool, String)` — runs `SELECT 1`; message notes "(via SSH tunnel)" when SSH is enabled.

### 6.3 `schema.rs`

- Introspection, queries ported verbatim:
  - `get_tables` → name, `TABLE_ROWS`, size MB `(DATA_LENGTH+INDEX_LENGTH)/1024/1024`, engine, collation, `BASE TABLE` only.
  - `get_views`, `get_routines` (name + `ROUTINE_TYPE`), `get_triggers`.
- DDL: `SHOW CREATE TABLE|VIEW|PROCEDURE|FUNCTION|TRIGGER`. Handle MySQL's column-name variance for routine DDL (Python scans keys starting with "create").
- `resolve_tables(cfg)` — wildcard `*`/`?` matching (case-insensitive) for include/exclude lists; exact match otherwise. Implement a small matcher (or `wildmatch`/`globset`) equivalent to `fnmatch`.
- `transfer_schema` — tables pass with `SET FOREIGN_KEY_CHECKS=0`, optional `DROP TABLE IF EXISTS` when `drop_existing`, tolerate "already exists", dry-run logs only; then views / routines / triggers passes gated on the `include_*` flags, each with `drop_existing` handling and dry-run.

### 6.4 `data.rs`

Per table:
- `SHOW COLUMNS` on source **and** dest; use the **intersection preserving source order** (schema-drift safety). If dest has no columns yet, use all source columns. Empty intersection → record error, skip.
- Build `INSERT IGNORE INTO ...` normally, `REPLACE INTO ...` when `incremental`.
- Incremental: `SELECT MAX(\`col\`)` on dest; if present, append `WHERE \`col\` > ?` to the source query and bind the value. Tolerate dest table not existing yet.
- `TRUNCATE TABLE` when `truncate` and not `incremental` (and not dry-run).
- Dry-run: count source rows only, emit a dry-run log, return stats.
- Bulk-load session vars on dest: `FOREIGN_KEY_CHECKS=0`, `UNIQUE_CHECKS=0`, `AUTOCOMMIT=0`, `sql_mode=''`; restore checks afterward.
- Stream source rows; collect each row's selected columns as `Vec<mysql_async::Value>` (preserves types better than the Python dict round-trip); batch to `chunk_size`; `exec_batch` the insert into dest; commit per chunk; emit `ChunkInserted { table, rows }`. Flush the remainder.
- **Retry:** 3 attempts; on failure sleep `5 * attempt` seconds (`tokio::time::sleep`) and retry; after the last attempt record the error into `TableStats`.

### 6.5 `transfer.rs` (orchestration)

- Open both SSH tunnels once (shared); resolve tables; if none match, emit a message and stop.
- Schema pass (when requested).
- Data pass: count rows per table first (emit `TableCounted`), then transfer tables **concurrently** with a `tokio::sync::Semaphore(workers)` + `JoinSet` (the `ThreadPoolExecutor(max_workers)` analog). Each task creates its own `mysql_async` connections through the shared tunnel. Collect `TableStats`; emit `Summary`.
- `run_inspect` / `run_diff` mirror the Python versions and return `Inspection` / `SchemaDiff`.

## 7. CLI crate (`transfer-cli`)

- `clap` derive: top-level command + 5 subcommands (`transfer`, `schema`, `data`, `inspect`, `diff`).
- Shared options matching today's names: `-c/--config`; `--source-host/-port/-user/-password/-db`; `--source-ssh-host/-port/-user/-password/-key`; the `--dest-*` equivalents.
- Transfer options: `--tables`, `--exclude-tables`, `--chunk-size`, `--workers`, `--drop-existing`, `--truncate`, `--incremental`, `--incremental-column`, `--include-views`, `--include-routines`, `--include-triggers`, `--dry-run`, and `-s/--select`.
- `_build_config`: load file → apply overrides → validate → print errors and exit non-zero on failure.
- `-s/--select`: connect to source, list tables (`name (rows, size MB)`), `inquire` fuzzy multi-select, set `cfg.tables`.
- `render.rs`: drain `TransferEvent`s into `indicatif` multi-progress; render inspect/diff/summary via `comfy-table`. Dry-run banner.

## 8. FFI crate (`transfer-ffi`, UniFFI)

- Declares the interface (proc-macro or UDL): config records (`SshConfig`/`ConnectionConfig`/`TransferConfig`), the `TransferEvent`/`TableStats`/`TransferSummary`/`Inspection`/`SchemaDiff` types, async functions `run_transfer` / `run_inspect` / `run_diff` / `test_connection`, a **`TransferListener` callback interface** (`on_event(event: TransferEvent)`), and `TransferError` mapped to a Swift `throws`.
- Owns a bundled `tokio` runtime (e.g. `uniffi`'s async support backed by a process-wide runtime) so Swift simply `await`s.
- The sink passed into `transfer-core` forwards each event to the Swift listener.
- Build: `cargo build` static libs for `aarch64-apple-darwin` + `x86_64-apple-darwin`, `lipo` into a universal lib, `uniffi-bindgen` generates Swift + module map, `xcodebuild -create-xcframework` produces `MySQLTransfer.xcframework`. Captured in `build-xcframework.sh`.
- Includes a tiny Swift smoke test that links the framework and calls `test_connection`.

## 9. SwiftUI macOS app (`app/`)

- MVVM with `@Observable` view models; an `EngineService` calls the FFI off the main thread and republishes events on the `@MainActor`.
- `TransferListener` is implemented in Swift; `on_event` hops to the main actor to update the UI.
- Screens:
  - **Connections** — source + dest forms (host, port, user, password, database, charset, SSH toggle + ssh host/port/user/password/key) with **Test Connection** buttons calling `test_connection`.
  - **Options** — chunk size, workers, drop/truncate, incremental(+column), include views/routines/triggers, dry-run.
  - **Tables** — runs `inspect`, lists tables (name, rows, size MB), multi-select (the `-s/--select` analog) feeding `cfg.tables`.
  - **Transfer** — per-table progress bars (driven by `ChunkInserted`/`TableStarted`/`TableCompleted`), a live log (from `Log`), and a final summary table (from `Summary`). Run buttons for transfer / schema-only / data-only.
  - **Diff** — source-vs-dest table comparison from `run_diff`.
  - **Config I/O** — load/save `config.yaml` via `Yams`, interoperable with the CLI.
- Networking entitlement (outgoing client) if sandboxed; otherwise a plain `.app`.

## 10. Error handling

- `transfer-core` returns `Result<T, TransferError>` (`thiserror`) with variants like `Config`, `Ssh`, `MySql`, `Io`. No `exit`/printing in the library.
- Per-table failures are captured into `TableStats.error` so one table failing does not abort the run (matches Python).
- CLI maps errors to messages + exit codes; FFI maps `TransferError` to a Swift `throws` error; SwiftUI surfaces it in an alert.

## 11. Testing strategy

- **Unit (no DB):** config load/merge/validate, wildcard matcher, column-intersection, INSERT/REPLACE SQL builder, event/summary reduction.
- **Integration (DB):** `testcontainers` MySQL for schema + data round-trips, incremental sync, dry-run; gated behind a feature/`#[ignore]` so `cargo test` runs without Docker.
- **SSH:** a localhost `russh` server fixture (or `#[ignore]`) exercising the tunnel forwarder.
- **FFI/Swift:** a smoke test linking the XCFramework and calling `test_connection`; a manual checklist for the SwiftUI flows in v1.

## 12. Phasing (drives the implementation plan)

Rust core:
1. Workspace skeleton + `config` + `error` + `event` types (+ unit tests).
2. `connection` (no SSH) + `schema` introspection + `inspect`/`diff` end-to-end.
3. `data` transfer (chunked streaming, column intersect, bulk-load) — single-threaded.
4. Concurrency (`Semaphore`/`JoinSet`) + retry + summary.
5. `ssh_tunnel` forwarder.
6. Incremental sync + views/routines/triggers + dry-run polish.
7. CLI flags parity + interactive select + progress rendering.

FFI + UI:
8. `transfer-ffi` (UniFFI interface, bundled runtime, callback events, error mapping) + `build-xcframework.sh` + Swift smoke test.
9. SwiftUI: Connections + Options + Test Connection.
10. SwiftUI: Inspect + table selection.
11. SwiftUI: Transfer (live progress + summary) + Diff + config load/save.

## 13. Risks & mitigations

- **`russh` local-forward plumbing** is the least boilerplate-y part — isolate it in `ssh_tunnel.rs` behind a small `Tunnel` API and test against a localhost SSH fixture.
- **UniFFI async + callbacks across the FFI boundary** — validate the seam early with the Phase 8 smoke test before building UI on top of it.
- **Two-language scope is large** — the phasing keeps the Rust core fully usable (and shippable via CLI) before any Swift work begins, so value lands incrementally.
- **`mysql_async` value fidelity** (dates, decimals, binary, NULL) — cover with integration round-trips on representative column types.
