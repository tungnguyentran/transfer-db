# Easier-to-Use DX Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `doctor` diagnostic command, wrap connection errors with friendly messages, and shorten the invocation path (alias `mt` + config auto-discovery) so the existing user spends less time typing and less time debugging connection failures.

**Architecture:** Three independent, additive changes. New code lives in existing modules (`connection.py`, `config.py`, `cli.py`). The doctor logic builds on the existing `test_connection()` helper. Error wrapping is centralized in a `_diagnose_error()` helper at the connection boundary, raising a typed `MySQLTransferConnectionError` that the CLI catches for a clean exit. Config auto-discovery extends `load_config()` with a search-order fallback. No breaking changes; everything is opt-out via existing or new flags.

**Tech Stack:** Python 3.10+, Click 8, pymysql, sshtunnel/paramiko, Rich, PyYAML. Project uses `uv` for dependency management.

**Testing note:** This project has no existing test suite. Pure-function code (config path resolution, exception → message mapping) is unit-tested with pytest. Code that needs a real MySQL+SSH (the doctor command end-to-end, pre-flight integration) is manually verified against the user's existing working config per the steps below. pytest is added as a dev dependency.

---

## File Structure

Files modified or created:

- **Modify** `src/mysql_transfer/connection.py` — add `MySQLTransferConnectionError`, `_diagnose_error()`, wrap `pymysql.connect()` and `tunnel.start()`, extend `test_connection()` into a richer `check_connection()` returning per-step results.
- **Modify** `src/mysql_transfer/config.py` — add `find_config_file()`; have `load_config(None)` use it; raise a clear "no config found" error.
- **Modify** `src/mysql_transfer/cli.py` — add `doctor` subcommand, add `--skip-doctor` to `transfer`/`schema`/`data`, run doctor as pre-flight, catch `MySQLTransferConnectionError` at the top level for clean exit.
- **Create** `tests/__init__.py`, `tests/test_config.py`, `tests/test_connection_errors.py` — unit tests for pure logic.
- **Modify** `pyproject.toml` — add `mt` console script, add pytest as dev dependency.
- **Modify** `README.md` — update install/quick-start, document `doctor`, document config auto-discovery, document `mt` alias.
- **Modify** `CLAUDE.md` — keep Quick Start in sync with README.

---

## Task 1: Set up pytest

**Files:**
- Modify: `pyproject.toml`
- Create: `tests/__init__.py`

- [ ] **Step 1: Add pytest as a dev dependency**

Edit `pyproject.toml`. After the `[project]` section (just before `[project.scripts]`), add:

```toml
[dependency-groups]
dev = [
    "pytest>=8.0",
]
```

- [ ] **Step 2: Create empty tests package**

Create `tests/__init__.py` with empty contents (so pytest discovery works regardless of conftest layout).

```python
```

- [ ] **Step 3: Sync dependencies**

Run: `uv sync --all-groups`
Expected: completes successfully; pytest is now available.

- [ ] **Step 4: Verify pytest works**

Run: `uv run pytest --version`
Expected: prints a version number, exit 0.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml tests/__init__.py uv.lock
git commit -m "$(cat <<'EOF'
chore: add pytest as a dev dependency

Prep work for adding unit tests for the upcoming DX polish changes.
EOF
)"
```

---

## Task 2: Config auto-discovery — failing tests

**Files:**
- Create: `tests/test_config.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_config.py`:

```python
"""Tests for config loading and auto-discovery."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from mysql_transfer.config import find_config_file, load_config


def test_find_config_file_returns_none_when_no_config_exists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    # Override HOME so the user's real ~/.config/mysql-transfer/config.yaml can't leak in.
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    assert find_config_file() is None


def test_find_config_file_picks_cwd_config_yaml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "config.yaml").write_text("source: {}\n")
    assert find_config_file() == tmp_path / "config.yaml"


def test_find_config_file_picks_cwd_mysql_transfer_yaml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "mysql-transfer.yaml").write_text("source: {}\n")
    assert find_config_file() == tmp_path / "mysql-transfer.yaml"


def test_find_config_file_prefers_config_yaml_over_mysql_transfer_yaml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "config.yaml").write_text("source: {}\n")
    (tmp_path / "mysql-transfer.yaml").write_text("source: {}\n")
    assert find_config_file() == tmp_path / "config.yaml"


def test_find_config_file_falls_back_to_xdg(tmp_path, monkeypatch):
    cwd = tmp_path / "work"
    cwd.mkdir()
    xdg = tmp_path / "xdg"
    (xdg / "mysql-transfer").mkdir(parents=True)
    (xdg / "mysql-transfer" / "config.yaml").write_text("source: {}\n")
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    assert find_config_file() == xdg / "mysql-transfer" / "config.yaml"


def test_find_config_file_default_xdg_under_home(tmp_path, monkeypatch):
    cwd = tmp_path / "work"
    cwd.mkdir()
    home = tmp_path / "home"
    (home / ".config" / "mysql-transfer").mkdir(parents=True)
    (home / ".config" / "mysql-transfer" / "config.yaml").write_text("source: {}\n")
    monkeypatch.chdir(cwd)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setenv("HOME", str(home))
    assert find_config_file() == home / ".config" / "mysql-transfer" / "config.yaml"


def test_load_config_with_none_returns_empty_when_no_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    cfg = load_config(None)
    # No file means defaults — validation will catch missing fields elsewhere.
    assert cfg.source.host == "localhost"
    assert cfg.source.database == ""


def test_load_config_with_none_picks_up_discovered_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "config.yaml").write_text(
        "source:\n  host: src.example\n  database: srcdb\n"
        "destination:\n  host: dst.example\n  database: dstdb\n"
    )
    cfg = load_config(None)
    assert cfg.source.host == "src.example"
    assert cfg.dest.host == "dst.example"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL — `ImportError: cannot import name 'find_config_file' from 'mysql_transfer.config'`.

---

## Task 3: Config auto-discovery — implementation

**Files:**
- Modify: `src/mysql_transfer/config.py`

- [ ] **Step 1: Add `find_config_file()` and update `load_config()`**

In `src/mysql_transfer/config.py`, add the following near the top of the file (after the existing `import yaml` line):

```python
import os
```

Then add this function above `load_config`:

```python
def find_config_file() -> Path | None:
    """Search for a config file in standard locations.

    Search order (first hit wins):
      1. ./config.yaml
      2. ./mysql-transfer.yaml
      3. $XDG_CONFIG_HOME/mysql-transfer/config.yaml
         (default $HOME/.config/mysql-transfer/config.yaml)
    """
    candidates: list[Path] = [
        Path.cwd() / "config.yaml",
        Path.cwd() / "mysql-transfer.yaml",
    ]

    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        candidates.append(Path(xdg) / "mysql-transfer" / "config.yaml")
    else:
        home = os.environ.get("HOME")
        if home:
            candidates.append(Path(home) / ".config" / "mysql-transfer" / "config.yaml")

    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None
```

Modify the existing `load_config` function. Replace:

```python
def load_config(config_path: str | Path | None = None) -> TransferConfig:
    """Load configuration from a YAML file."""
    if config_path is None:
        return TransferConfig()

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
```

With:

```python
def load_config(config_path: str | Path | None = None) -> TransferConfig:
    """Load configuration from a YAML file.

    If config_path is None, search standard locations via find_config_file().
    If nothing is found, return a default (empty) TransferConfig — the caller
    is expected to either apply CLI overrides or fail validation.
    """
    if config_path is None:
        discovered = find_config_file()
        if discovered is None:
            return TransferConfig()
        path = discovered
    else:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS — all 8 tests green.

- [ ] **Step 3: Commit**

```bash
git add src/mysql_transfer/config.py tests/test_config.py
git commit -m "$(cat <<'EOF'
feat(config): auto-discover config.yaml from cwd and XDG path

Lets users run `mysql-transfer transfer` without `-c config.yaml` when
a config exists in the current directory or in
$XDG_CONFIG_HOME/mysql-transfer/config.yaml.
EOF
)"
```

---

## Task 4: Wire auto-discovery into the CLI with a friendly "no config" message

**Files:**
- Modify: `src/mysql_transfer/cli.py`

- [ ] **Step 1: Update `_build_config` to surface the discovered path and a friendly error**

In `src/mysql_transfer/cli.py`, replace the existing `_build_config` function with:

```python
def _build_config(config_file, **kwargs) -> TransferConfig:
    """Load config from file (auto-discovering if needed) and apply CLI overrides."""
    # Pop --select before passing to config (it's not a config field)
    kwargs.pop("select", None)

    from mysql_transfer.config import find_config_file

    if config_file is None:
        discovered = find_config_file()
        if discovered is not None:
            console.print(f"[dim]Using config: {discovered}[/dim]")
            config_file = str(discovered)

    try:
        cfg = load_config(config_file)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    cfg = apply_cli_overrides(cfg, **kwargs)

    errors = cfg.validate()
    if errors:
        # If the user passed nothing — no -c, no overrides — and validation
        # is failing because nothing was set, hint at the config file.
        if config_file is None and not any(
            kwargs.get(k) for k in ("source_host", "dest_host", "source_db", "dest_db")
        ):
            console.print(
                "[red]Error:[/red] No config found. "
                "Pass [bold]-c <path>[/bold] or create [bold]./config.yaml[/bold] "
                "(see config.example.yaml)."
            )
            sys.exit(1)
        console.print("[red]Configuration errors:[/red]")
        for err in errors:
            console.print(f"  • {err}")
        sys.exit(1)

    return cfg
```

- [ ] **Step 2: Manual verification — auto-discovered config**

From the project root (where `config.yaml` exists), run:

```bash
uv run mysql-transfer --help
```

Expected: prints help, exit 0 (this only exercises Click setup, not config loading).

From the project root, run:

```bash
uv run mysql-transfer inspect
```

Expected: prints `Using config: /Users/tungnt1/Projects/TungXoan/transfer-db/config.yaml` (or similar), then proceeds with inspect. If your config.yaml is currently broken, you'll see the existing inspect output — that's fine; the point is that the config was auto-discovered.

- [ ] **Step 3: Manual verification — no config found**

From a temporary empty dir:

```bash
cd /tmp && mkdir -p mt-test-empty && cd mt-test-empty
HOME=/tmp/mt-test-empty XDG_CONFIG_HOME=/tmp/mt-test-empty/xdg \
  uv run --project /Users/tungnt1/Projects/TungXoan/transfer-db mysql-transfer inspect
```

Expected: `Error: No config found. Pass -c <path> or create ./config.yaml (see config.example.yaml).` Exit 1.

Return to project root: `cd /Users/tungnt1/Projects/TungXoan/transfer-db`.

- [ ] **Step 4: Commit**

```bash
git add src/mysql_transfer/cli.py
git commit -m "$(cat <<'EOF'
feat(cli): wire config auto-discovery into all commands

When -c is omitted, prints the discovered config path or a friendly
'No config found' message instead of a generic validation error.
EOF
)"
```

---

## Task 5: Error wrapping — failing tests

**Files:**
- Create: `tests/test_connection_errors.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_connection_errors.py`:

```python
"""Tests for the connection error diagnosis layer."""

from __future__ import annotations

import paramiko
import pymysql
import pytest

from mysql_transfer.config import ConnectionConfig, SSHConfig
from mysql_transfer.connection import (
    MySQLTransferConnectionError,
    _diagnose_error,
)


def _cfg(host="db.example.com", port=3306, user="alice", database="prod", ssh=False):
    return ConnectionConfig(
        host=host,
        port=port,
        user=user,
        password="x",
        database=database,
        ssh=SSHConfig(enabled=ssh, host="bastion.example.com", port=22, user="alice"),
    )


def test_diagnose_paramiko_auth_failure():
    err = paramiko.ssh_exception.AuthenticationException("bad key")
    out = _diagnose_error(err, _cfg(ssh=True), step="ssh")
    assert isinstance(out, MySQLTransferConnectionError)
    msg = str(out)
    assert "SSH auth failed" in msg
    assert "alice@bastion.example.com:22" in msg


def test_diagnose_paramiko_no_valid_connections():
    err = paramiko.ssh_exception.NoValidConnectionsError({("bastion.example.com", 22): OSError("nope")})
    out = _diagnose_error(err, _cfg(ssh=True), step="ssh")
    assert "Could not reach SSH host" in str(out)
    assert "bastion.example.com:22" in str(out)


def test_diagnose_mysql_access_denied():
    err = pymysql.err.OperationalError(1045, "Access denied for user 'alice'@'1.2.3.4'")
    out = _diagnose_error(err, _cfg(), step="mysql")
    msg = str(out)
    assert "MySQL auth failed" in msg
    assert "alice@db.example.com:3306" in msg


def test_diagnose_mysql_unknown_database():
    err = pymysql.err.OperationalError(1049, "Unknown database 'prod'")
    out = _diagnose_error(err, _cfg(), step="mysql")
    msg = str(out)
    assert "Database 'prod' does not exist" in msg
    assert "db.example.com:3306" in msg


def test_diagnose_mysql_cant_connect():
    err = pymysql.err.OperationalError(2003, "Can't connect to MySQL server on 'db.example.com' (61)")
    out = _diagnose_error(err, _cfg(), step="mysql")
    msg = str(out)
    assert "Could not reach MySQL at db.example.com:3306" in msg
    assert "SSH" in msg  # hints that SSH might be needed


def test_diagnose_mysql_lost_connection():
    err = pymysql.err.OperationalError(2013, "Lost connection during query")
    out = _diagnose_error(err, _cfg(), step="mysql")
    msg = str(out)
    assert "Lost connection" in msg
    assert "db.example.com:3306" in msg
    assert "chunk_size" in msg or "workers" in msg


def test_diagnose_passes_through_unknown_errors():
    err = RuntimeError("something weird")
    out = _diagnose_error(err, _cfg(), step="mysql")
    msg = str(out)
    assert "db.example.com:3306" in msg
    assert "something weird" in msg
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_connection_errors.py -v`
Expected: FAIL — `ImportError: cannot import name 'MySQLTransferConnectionError'`.

---

## Task 6: Error wrapping — implementation

**Files:**
- Modify: `src/mysql_transfer/connection.py`

- [ ] **Step 1: Add the error class and diagnose helper**

At the top of `src/mysql_transfer/connection.py`, add to the imports (the file already imports `pymysql`, `sshtunnel`, etc.):

```python
import paramiko
```

Then, immediately after the existing `warnings.filterwarnings(...)` calls (before the `if TYPE_CHECKING:` block), add:

```python
class MySQLTransferConnectionError(Exception):
    """Raised when a connection to source/destination cannot be established.

    Carries a user-facing diagnostic message; the CLI catches this for a
    clean (no-traceback) exit.
    """


def _diagnose_error(exc: BaseException, cfg, step: str) -> MySQLTransferConnectionError:
    """Translate a low-level connection exception into a friendly message.

    Args:
        exc: The original exception.
        cfg: The ConnectionConfig in play (used for host/port/user/db context).
        step: "ssh" or "mysql" — which phase failed.
    """
    ssh = cfg.ssh
    db_loc = f"{cfg.host}:{cfg.port}"

    # --- SSH-layer errors ---
    if isinstance(exc, paramiko.ssh_exception.AuthenticationException):
        return MySQLTransferConnectionError(
            f"SSH auth failed for {ssh.user}@{ssh.host}:{ssh.port}. "
            f"Check your SSH key/password."
        )
    if isinstance(exc, paramiko.ssh_exception.NoValidConnectionsError):
        return MySQLTransferConnectionError(
            f"Could not reach SSH host {ssh.host}:{ssh.port}. "
            f"Is it up? Is the port right?"
        )

    # --- MySQL-layer errors ---
    if isinstance(exc, pymysql.err.OperationalError):
        code = exc.args[0] if exc.args else None
        if code == 1045:
            return MySQLTransferConnectionError(
                f"MySQL auth failed for {cfg.user}@{db_loc}. "
                f"Check user/password."
            )
        if code == 1049:
            return MySQLTransferConnectionError(
                f"Database '{cfg.database}' does not exist on {db_loc}."
            )
        if code == 2003:
            hint = "" if ssh.enabled else " Did you mean to use SSH?"
            return MySQLTransferConnectionError(
                f"Could not reach MySQL at {db_loc}. "
                f"Is it running? Is the port right?{hint}"
            )
        if code == 2013:
            return MySQLTransferConnectionError(
                f"Lost connection to MySQL at {db_loc} during {step}. "
                f"Consider increasing chunk_size or reducing workers."
            )

    # --- Fallback: keep the original message but prepend context ---
    return MySQLTransferConnectionError(f"{db_loc} {step}: {exc}")
```

- [ ] **Step 2: Wrap the SSH `tunnel.start()` call**

In `_create_ssh_tunnel`, replace the existing try/except block:

```python
    tunnel = SSHTunnelForwarder(**ssh_kwargs)
    try:
        tunnel.start()
    except Exception as e:
        from rich.console import Console
        Console().print(f"[red]Error:[/red] Could not connect to SSH gateway [bold]{cfg.ssh.host}:{cfg.ssh.port}[/bold] — {e}")
        import sys
        sys.exit(1)
    return tunnel
```

With:

```python
    tunnel = SSHTunnelForwarder(**ssh_kwargs)
    try:
        tunnel.start()
    except Exception as e:
        raise _diagnose_error(e, cfg, step="ssh") from e
    return tunnel
```

- [ ] **Step 3: Wrap the `pymysql.connect()` call**

In `create_connection`, replace the existing connect call:

```python
    conn = pymysql.connect(
        host=host,
        port=port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset=cfg.charset,
        cursorclass=cursor_class,
        autocommit=False,
        connect_timeout=30,
        read_timeout=600,
        write_timeout=300,
    )
```

With:

```python
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=cfg.user,
            password=cfg.password,
            database=cfg.database,
            charset=cfg.charset,
            cursorclass=cursor_class,
            autocommit=False,
            connect_timeout=30,
            read_timeout=600,
            write_timeout=300,
        )
    except Exception as e:
        raise _diagnose_error(e, cfg, step="mysql") from e
```

- [ ] **Step 4: Run unit tests to verify pass**

Run: `uv run pytest tests/test_connection_errors.py -v`
Expected: PASS — all 7 tests green.

- [ ] **Step 5: Catch the typed error in the CLI top-level**

In `src/mysql_transfer/cli.py`, add an import at the top with the other `from mysql_transfer.*` lines:

```python
from mysql_transfer.connection import MySQLTransferConnectionError
```

Wrap the bottom-of-file `cli()` invocation. Replace:

```python
if __name__ == "__main__":
    cli()
```

With:

```python
def main() -> None:
    try:
        cli()
    except MySQLTransferConnectionError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

Also update `pyproject.toml` to point the console scripts at `main` instead of `cli`, so the wrapper runs in production too. Find the existing `[project.scripts]` section:

```toml
[project.scripts]
mysql-transfer = "mysql_transfer.cli:cli"
```

Replace with:

```toml
[project.scripts]
mysql-transfer = "mysql_transfer.cli:main"
```

(The `mt` alias is added in Task 8.)

- [ ] **Step 6: Run unit tests again (full file)**

Run: `uv run pytest -v`
Expected: PASS — all tests across both files green.

- [ ] **Step 7: Manual verification — induce a bad password**

Temporarily edit your `config.yaml` to use a wrong source password (back up the real one in your head first). Run:

```bash
uv run mysql-transfer inspect
```

Expected: a single red `Error: MySQL auth failed for <user>@<host>:<port>. Check user/password.` line, exit 1. No traceback.

Restore the real password.

- [ ] **Step 8: Commit**

```bash
git add src/mysql_transfer/connection.py src/mysql_transfer/cli.py pyproject.toml tests/test_connection_errors.py
git commit -m "$(cat <<'EOF'
feat(connection): wrap connect errors with friendly diagnostics

Translates common pymysql/paramiko errors into one-line messages that
name the host, the step, and the most likely fix. Routes
MySQLTransferConnectionError through the CLI for a clean (no-traceback)
exit.
EOF
)"
```

---

## Task 7: `doctor` command and pre-flight integration

**Files:**
- Modify: `src/mysql_transfer/connection.py` — add `check_connection()`.
- Modify: `src/mysql_transfer/transfer.py` — add `run_doctor()`.
- Modify: `src/mysql_transfer/cli.py` — register `doctor` subcommand, add `--skip-doctor`, run pre-flight.

This task is verified manually because it needs a real MySQL.

- [ ] **Step 1: Add `check_connection()` to `connection.py`**

In `src/mysql_transfer/connection.py`, replace the existing `test_connection` function with:

```python
def test_connection(cfg: ConnectionConfig) -> tuple[bool, str]:
    """Legacy: test if a connection can be established. Returns (success, message)."""
    try:
        with managed_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        tunnel_info = " (via SSH tunnel)" if cfg.ssh.enabled else ""
        return True, f"Connection successful{tunnel_info}"
    except Exception as e:
        return False, str(e)


def check_connection(
    cfg: ConnectionConfig,
    *,
    require_write: bool = False,
    require_drop: bool = False,
) -> list[tuple[str, bool, str]]:
    """Run diagnostic checks against a connection config.

    Returns a list of (check_name, ok, detail) tuples in the order they ran.
    `detail` is empty on success, an error/fix message on failure.

    Checks:
      - SSH tunnel (if cfg.ssh.enabled)
      - MySQL authentication
      - Database accessibility
      - SELECT privilege (always)
      - INSERT/CREATE/ALTER privileges (if require_write)
      - DROP privilege (if require_drop)
    """
    results: list[tuple[str, bool, str]] = []
    tunnel = None
    conn = None
    try:
        # 1. SSH tunnel
        if cfg.ssh.enabled:
            try:
                tunnel = _create_ssh_tunnel(cfg)
                results.append(("SSH tunnel established", True, ""))
            except MySQLTransferConnectionError as e:
                results.append(("SSH tunnel", False, str(e)))
                return results
            except Exception as e:
                results.append(("SSH tunnel", False, str(e)))
                return results

        # 2. MySQL auth
        try:
            conn = create_connection(cfg, tunnel=tunnel)
            results.append(("MySQL authentication successful", True, ""))
        except MySQLTransferConnectionError as e:
            results.append(("MySQL authentication", False, str(e)))
            return results
        except Exception as e:
            results.append(("MySQL authentication", False, str(e)))
            return results

        # 3. Database accessibility (already implied by connect with database=, but
        # SELECT DATABASE() makes it explicit and works if the user later switches).
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT DATABASE()")
                row = cur.fetchone()
            actual_db = next(iter(row.values())) if row else None
            if actual_db == cfg.database:
                results.append((f"Database '{cfg.database}' accessible", True, ""))
            else:
                results.append(
                    (f"Database '{cfg.database}' accessible",
                     False,
                     f"Connected, but current database is '{actual_db}'."),
                )
                return results
        except Exception as e:
            results.append((f"Database '{cfg.database}' accessible", False, str(e)))
            return results

        # 4. Privileges
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW GRANTS FOR CURRENT_USER()")
                grants_rows = cur.fetchall()
        except Exception as e:
            results.append(("Could not read grants", False, str(e)))
            return results

        grants_text = "\n".join(
            next(iter(row.values())).upper() if row else ""
            for row in grants_rows
        )

        def _has_priv(priv: str) -> bool:
            # Crude but workable: ALL PRIVILEGES, or the named privilege,
            # scoped to either *.* or `db`.* — we don't try to parse fully.
            up = priv.upper()
            return (
                "ALL PRIVILEGES" in grants_text
                or up in grants_text
            )

        required = [("SELECT", True)]
        if require_write:
            required += [("INSERT", True), ("CREATE", True), ("ALTER", True)]
        if require_drop:
            required += [("DROP", True)]

        for priv, _ in required:
            if _has_priv(priv):
                results.append((f"User has {priv} privilege", True, ""))
            else:
                results.append(
                    (f"User has {priv} privilege",
                     False,
                     f"Fix: GRANT {priv} ON `{cfg.database}`.* TO CURRENT_USER();"),
                )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if tunnel is not None:
            try:
                tunnel.stop()
            except Exception:
                pass

    return results
```

- [ ] **Step 2: Add `run_doctor()` to `transfer.py`**

In `src/mysql_transfer/transfer.py`, add this function at the bottom of the file:

```python
def run_doctor(cfg: TransferConfig, *, check_dest: bool = True) -> bool:
    """Run connection diagnostics for source and (optionally) destination.

    Returns True if all checks passed, False otherwise.
    """
    from mysql_transfer.connection import check_connection

    def _print_section(label: str, cfg_section, *, require_write: bool, require_drop: bool) -> bool:
        ssh_part = f" via SSH {cfg_section.ssh.host}" if cfg_section.ssh.enabled else ""
        console.print(
            f"[bold]{label}[/bold] ({cfg_section.host}:{cfg_section.port}{ssh_part}, db='{cfg_section.database}')"
        )
        results = check_connection(
            cfg_section,
            require_write=require_write,
            require_drop=require_drop,
        )
        ok = True
        for name, passed, detail in results:
            if passed:
                console.print(f"  [green]✓[/green] {name}")
            else:
                ok = False
                console.print(f"  [red]✗[/red] {name}")
                if detail:
                    console.print(f"    [dim]{detail}[/dim]")
        console.print()
        return ok

    src_ok = _print_section(
        "Source",
        cfg.source,
        require_write=False,
        require_drop=False,
    )
    if not check_dest:
        return src_ok

    dest_ok = _print_section(
        "Destination",
        cfg.dest,
        require_write=True,
        require_drop=cfg.drop_existing,
    )

    if src_ok and dest_ok:
        console.print("[bold green]All checks passed.[/bold green]")
    else:
        console.print("[bold red]Connection problems detected.[/bold red]")
    return src_ok and dest_ok
```

- [ ] **Step 3: Add the `doctor` subcommand and `--skip-doctor` flag to `cli.py`**

In `src/mysql_transfer/cli.py`, add this command at the bottom of the file (just before the `def main()` block):

```python
# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------

@cli.command()
@common_options
@click.option("--skip-dest", is_flag=True, default=False, help="Only check the source.")
def doctor(config_file, skip_dest, **kwargs):
    """Diagnose connection / privilege problems for source and destination."""
    cfg = _build_config(config_file, **kwargs)
    from mysql_transfer.transfer import run_doctor
    ok = run_doctor(cfg, check_dest=not skip_dest)
    sys.exit(0 if ok else 1)
```

Add `--skip-doctor` to `transfer_options`. Find the existing `transfer_options` decorator's `opts` list and add this line just before `click.option("--dry-run", ...)`:

```python
        click.option("--skip-doctor", is_flag=True, default=False, help="Skip pre-flight connection checks."),
```

Then wire pre-flight into `transfer`, `schema`, and `data`. Replace the existing `transfer` command:

```python
@cli.command()
@common_options
@transfer_options
def transfer(config_file, **kwargs):
    """Transfer schema and data from source to destination."""
    select = kwargs.get("select", False)
    skip_doctor = kwargs.pop("skip_doctor", False)
    cfg = _build_config(config_file, **kwargs)

    if not skip_doctor:
        from mysql_transfer.transfer import run_doctor
        if not run_doctor(cfg, check_dest=True):
            console.print("[yellow]Pre-flight checks failed. Pass --skip-doctor to override.[/yellow]")
            sys.exit(1)

    if select:
        cfg.tables = _interactive_table_select(cfg)

    from mysql_transfer.transfer import run_transfer
    run_transfer(cfg, schema=True, data=True)
```

Replace the existing `schema` command:

```python
@cli.command()
@common_options
@transfer_options
def schema(config_file, **kwargs):
    """Transfer schema only (tables, views, routines, triggers)."""
    select = kwargs.get("select", False)
    skip_doctor = kwargs.pop("skip_doctor", False)
    cfg = _build_config(config_file, **kwargs)

    if not skip_doctor:
        from mysql_transfer.transfer import run_doctor
        if not run_doctor(cfg, check_dest=True):
            console.print("[yellow]Pre-flight checks failed. Pass --skip-doctor to override.[/yellow]")
            sys.exit(1)

    if select:
        cfg.tables = _interactive_table_select(cfg)

    from mysql_transfer.transfer import run_transfer
    run_transfer(cfg, schema=True, data=False)
```

Replace the existing `data` command:

```python
@cli.command()
@common_options
@transfer_options
def data(config_file, **kwargs):
    """Transfer data only (assumes schema exists at destination)."""
    select = kwargs.get("select", False)
    skip_doctor = kwargs.pop("skip_doctor", False)
    cfg = _build_config(config_file, **kwargs)

    if not skip_doctor:
        from mysql_transfer.transfer import run_doctor
        if not run_doctor(cfg, check_dest=True):
            console.print("[yellow]Pre-flight checks failed. Pass --skip-doctor to override.[/yellow]")
            sys.exit(1)

    if select:
        cfg.tables = _interactive_table_select(cfg)

    from mysql_transfer.transfer import run_transfer
    run_transfer(cfg, schema=False, data=True)
```

- [ ] **Step 4: Manual verification — doctor happy path**

From the project root with the real (working) config.yaml:

```bash
uv run mysql-transfer doctor
```

Expected: Two sections (Source, Destination), each with ✓ marks for SSH (if applicable), authentication, database accessibility, and required privileges. Final line: `All checks passed.` Exit 0.

- [ ] **Step 5: Manual verification — doctor with bad password**

Temporarily edit `config.yaml` and replace the source password with garbage. Run:

```bash
uv run mysql-transfer doctor
```

Expected: Source section shows ✗ on MySQL authentication with the friendly message. `Connection problems detected.` Exit 1.

Restore the password.

- [ ] **Step 6: Manual verification — pre-flight aborts a real transfer**

With a wrong source password set, run:

```bash
uv run mysql-transfer transfer --dry-run
```

Expected: doctor output, ✗ on source auth, `Pre-flight checks failed. Pass --skip-doctor to override.` No transfer attempted. Exit 1.

Restore the password.

- [ ] **Step 7: Manual verification — `--skip-doctor` bypasses the check**

Run a dry-run with `--skip-doctor` (config valid):

```bash
uv run mysql-transfer transfer --skip-doctor --dry-run
```

Expected: Goes straight into the transfer flow without the doctor banner.

- [ ] **Step 8: Commit**

```bash
git add src/mysql_transfer/connection.py src/mysql_transfer/transfer.py src/mysql_transfer/cli.py
git commit -m "$(cat <<'EOF'
feat: add doctor command with pre-flight checks

`mt doctor` runs SSH/MySQL/db/privilege checks for both sides with
clear ✓/✗ output and fix hints. transfer/schema/data run the same
checks as pre-flight by default; --skip-doctor opts out.
EOF
)"
```

---

## Task 8: `mt` alias

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add the alias**

Find the `[project.scripts]` section in `pyproject.toml`:

```toml
[project.scripts]
mysql-transfer = "mysql_transfer.cli:main"
```

Replace with:

```toml
[project.scripts]
mysql-transfer = "mysql_transfer.cli:main"
mt = "mysql_transfer.cli:main"
```

- [ ] **Step 2: Re-sync so the new entry point is registered**

Run: `uv sync --all-groups`
Expected: completes; `mt` is now available via `uv run mt`.

- [ ] **Step 3: Manual verification**

Run:

```bash
uv run mt --help
```

Expected: same help output as `uv run mysql-transfer --help`, exit 0.

```bash
uv run mt doctor
```

Expected: same as `uv run mysql-transfer doctor`.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "$(cat <<'EOF'
feat: add `mt` alias as a short console-script name

`uv tool install .` exposes both `mysql-transfer` and `mt`.
EOF
)"
```

---

## Task 9: Update README and CLAUDE.md

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update README — install section**

In `README.md`, replace the existing `## Installation` and `## Quick Start` sections (lines 15–42 of the current file) with:

```markdown
## Installation

Install once and use globally:

```bash
uv tool install .
```

This puts `mysql-transfer` (and a short alias `mt`) on your PATH.

For development, `uv sync` + `uv run mt ...` also works.

## Quick Start

```bash
# 1. Copy and edit the config file
cp config.example.yaml config.yaml

# 2. Verify your connection settings work
mt doctor

# 3. Inspect the source database
mt inspect

# 4. Full transfer (schema + data)
mt transfer

# 5. Schema only / Data only
mt schema
mt data

# 6. Compare source vs destination
mt diff

# 7. Dry run
mt transfer --dry-run
```

`mt` auto-discovers `./config.yaml` (or `./mysql-transfer.yaml`, or `$XDG_CONFIG_HOME/mysql-transfer/config.yaml`). Pass `-c <path>` to use a different file.

By default, `transfer` / `schema` / `data` run a pre-flight `doctor` check before doing any work. Pass `--skip-doctor` to skip it.
```

In the CLI options table, add this row (just below the existing rows, before `## License`):

```markdown
| `--skip-doctor` | false | Skip pre-flight connection checks |
```

- [ ] **Step 2: Update CLAUDE.md Quick Start**

In `CLAUDE.md`, replace the `## Quick Start` section's bash block with:

````markdown
**Installation & Setup:**
```bash
# Install once, get `mysql-transfer` and `mt` on your PATH
uv tool install .

# Copy and customize the config
cp config.example.yaml config.yaml
```

**Running the CLI:**
```bash
# Diagnose connection problems
mt doctor

# Run the main transfer (runs doctor first; --skip-doctor to skip)
mt transfer

# Other commands
mt inspect
mt schema
mt data
mt diff
```

`mt` auto-discovers `./config.yaml`. For development, `uv run mt ...` also works.
````

- [ ] **Step 3: Manual verification**

Run: `grep -n "uv run mysql-transfer" README.md CLAUDE.md`
Expected: no results in the main install/quick-start sections (legacy mentions elsewhere are OK to leave).

- [ ] **Step 4: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "$(cat <<'EOF'
docs: document doctor, mt alias, and config auto-discovery

Updates README and CLAUDE.md so the primary install path is
`uv tool install .` and the documented commands use `mt`.
EOF
)"
```

---

## Task 10: End-to-end smoke test against a real transfer

This is a final sanity check that none of the changes broke the happy path. Verified manually with the user's real config.

- [ ] **Step 1: Run a dry-run transfer**

```bash
mt transfer --dry-run
```

Expected: doctor banner shows ✓ across the board, then the existing dry-run output (list of tables it would create/transfer).

- [ ] **Step 2: Run inspect**

```bash
mt inspect
```

Expected: existing inspect output unchanged.

- [ ] **Step 3: Run diff**

```bash
mt diff
```

Expected: existing diff output unchanged.

- [ ] **Step 4: Verify the unit tests still pass**

```bash
uv run pytest -v
```

Expected: all tests green.

- [ ] **Step 5: Final commit (only if there are accumulated cache/pyc changes worth tracking)**

If `git status` shows nothing relevant, skip. Otherwise:

```bash
git status
# If meaningful changes exist:
git add -p
git commit -m "chore: final cleanup after DX polish"
```

---

## Self-Review

**Spec coverage:**
- "doctor command" → Tasks 5, 6, 7
- "Pre-flight integration with --skip-doctor" → Task 7
- "Friendly error wrapping" → Tasks 5, 6
- "uv tool install . install path" → Task 9
- "mt alias" → Task 8
- "Config auto-discovery" → Tasks 2, 3, 4
- "No-config friendly error" → Task 4
- Privilege checks (SELECT on source; CREATE/INSERT/ALTER + optional DROP on dest) → Task 7
- Testing strategy (unit tests for pure logic, manual verification for live MySQL) → Tasks 2, 5 (unit); Tasks 4, 6, 7, 8, 10 (manual)

All spec requirements have at least one task. No gaps.

**Placeholder scan:** No TBDs, no "implement later", no "similar to Task N", no missing code blocks. The `_print_section` function in Task 7 step 2 has an unused `n_fail` accumulator block — left in deliberately because removing it cleanly is a Task-7-only change and the body is harmless dead code; can be cleaned up if a reviewer cares. (Self-correction: trimming it now.)

Going back to fix: the `n_fail` block in Task 7 step 2 is dead and confusing. Replace it with just the `[bold red]Connection problems detected.[/bold red]` line.

(Plan edited inline.)

**Type/name consistency:**
- `MySQLTransferConnectionError` used in connection.py and cli.py — matches.
- `find_config_file()` defined in Task 3, called in Task 4 — matches.
- `check_connection()` defined in Task 7 step 1, called from `run_doctor` in Task 7 step 2 — matches.
- `run_doctor(cfg, check_dest=...)` signature matches between transfer.py and cli.py.
- `--skip-doctor` flag — set in `transfer_options`, popped in each command via `kwargs.pop("skip_doctor", False)` — consistent.
- Entry point `mysql_transfer.cli:main` — consistent across both `mysql-transfer` and `mt` console scripts.

All consistent. Plan is ready.
