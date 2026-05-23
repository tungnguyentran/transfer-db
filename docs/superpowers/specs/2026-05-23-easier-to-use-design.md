# Easier-to-Use DX Polish — Design

**Date:** 2026-05-23
**Status:** Approved, ready for implementation
**Scope:** Minimal additive UX changes targeting two stated pains.

## Goal

Make `mysql-transfer` easier to use day-to-day for an existing user — specifically reducing friction around (1) verbose invocations and (2) debugging connection failures. Out of scope: first-time onboarding wizard, multi-environment profile management, persistent profile store, TUI redesign.

## Pain points targeted

1. **Verbose commands.** `uv run mysql-transfer transfer -c config.yaml ...` is long. Flag names are hard to remember.
2. **Debugging failures.** When an SSH tunnel or MySQL auth fails partway through a transfer, the error is a raw stacktrace or a generic pymysql message — hard to tell which step failed or what to do next.

## Non-goals (YAGNI)

- No `init` wizard. User already has a working config.
- No `--profile` / multi-environment config support.
- No persistent profile store (`~/.mysql-transfer/profiles.yaml`).
- No TUI/dashboard.
- No breaking changes to existing commands or config schema.

## Changes

Three additive changes. None breaks existing behaviour.

### 1. `doctor` command

A new CLI subcommand that runs connection diagnostics and prints a pass/fail line per step. Designed to be the *first* command a user runs when something's wrong, and to be invoked automatically as a pre-flight check at the start of `transfer` / `schema` / `data`.

**Checks performed (in order, for both source and destination):**

1. SSH tunnel opens (if SSH is configured for that side).
2. MySQL connection authenticates.
3. Target database exists.
4. Required privileges:
   - **Source:** `SELECT` on the target database.
   - **Destination:** `CREATE`, `DROP`, `INSERT`, `ALTER` on the target database. (Skip the `DROP` check if `drop_existing=false`; the others are always needed.)

**Output format:**

```
Source (mysql.example.com:3306 via SSH bastion.example.com)
  ✓ SSH tunnel established
  ✓ MySQL authentication successful
  ✓ Database 'production' accessible
  ✓ User has SELECT privilege

Destination (localhost:3306)
  ✓ MySQL authentication successful
  ✓ Database 'staging' accessible
  ✗ User missing CREATE privilege on 'staging'
    Fix: GRANT CREATE ON staging.* TO 'user'@'%';

1 problem found.
```

On any failure: exit code 1. Exit code 0 only if all checks pass.

**Pre-flight integration:** `transfer` / `schema` / `data` invoke the same doctor logic before doing any work. If any check fails, the command aborts with the doctor output, the user fixes, retries. Skippable with `--skip-doctor` for power users who already know the connection works (e.g., second run in a row).

**Implementation hook:** `connection.py` already exposes `test_connection()` at line 129. The doctor command extends that pattern with privilege checks via `SHOW GRANTS FOR CURRENT_USER()` parsing.

### 2. Friendly error wrapping

Today, when a connection fails mid-transfer the user sees something like:

```
pymysql.err.OperationalError: (1045, "Access denied for user 'foo'@'1.2.3.4' (using password: YES)")
```

…or worse, a `paramiko.ssh_exception.NoValidConnectionsError` traceback.

**Change:** wrap the connection boundary in `connection.py` so failures are translated into a single line that names the host, the step, and the most likely fix.

| Underlying error | Translated message |
|---|---|
| `paramiko.ssh_exception.AuthenticationException` | `SSH auth failed for {user}@{host}:{port}. Check your SSH key/password.` |
| `paramiko.ssh_exception.NoValidConnectionsError` | `Could not reach SSH host {host}:{port}. Is it up? Is the port right?` |
| `pymysql.err.OperationalError` 1045 (Access denied) | `MySQL auth failed for {user}@{host}:{port}. Check user/password.` |
| `pymysql.err.OperationalError` 1049 (Unknown database) | `Database '{db}' does not exist on {host}:{port}.` |
| `pymysql.err.OperationalError` 2003 (Can't connect) | `Could not reach MySQL at {host}:{port}. Is it running? Is the port right? Did you mean to use SSH?` |
| `pymysql.err.OperationalError` 2013 (Lost connection) | `Lost connection to MySQL at {host}:{port} during {step}. Consider increasing chunk_size or workers.` |
| Anything else | Re-raise as-is with `{host}:{port} {step}` prepended. |

Implementation: a single `_diagnose_error(exc, cfg, step)` helper in `connection.py` called from a small try/except wrapper around the `pymysql.connect` and `tunnel.start()` calls. Raises a new `ConnectionError` subclass with the friendly message; the CLI top-level catches it and prints the message (red) with exit code 1, no traceback. Existing `install_rich_traceback` stays for unexpected errors only.

### 3. Shorter invocation

Three independent changes that compound:

**3a. Install instructions.** README updates: replace `uv sync` + `uv run mysql-transfer ...` with `uv tool install .` + `mysql-transfer ...` as the primary path. Keep `uv run` as an alternative for development.

**3b. `mt` alias.** Add a second console script entry to `pyproject.toml`:

```toml
[project.scripts]
mysql-transfer = "mysql_transfer.cli:cli"
mt = "mysql_transfer.cli:cli"
```

After `uv tool install .`, both `mysql-transfer` and `mt` work.

**3c. Auto-discover config.** `_build_config()` in `cli.py` already accepts `config_file=None`. When `-c` is omitted, look for a config file in this order, using the first that exists:

1. `./config.yaml`
2. `./mysql-transfer.yaml`
3. `$XDG_CONFIG_HOME/mysql-transfer/config.yaml` (default `~/.config/mysql-transfer/config.yaml`)

If none exist and no `-c` was passed *and* no required CLI overrides were given, exit with a clear message: `No config found. Pass -c <path> or create ./config.yaml. See config.example.yaml.`

If a config is auto-discovered, log one line: `Using config: ./config.yaml`.

Net result: `mt transfer` (six characters) replaces `uv run mysql-transfer transfer -c config.yaml` (43 characters).

## Architecture impact

Files touched:

- `src/mysql_transfer/cli.py` — add `doctor` subcommand, add `--skip-doctor` to transfer/schema/data, call doctor as pre-flight, route `ConnectionError` to clean exit.
- `src/mysql_transfer/connection.py` — add `_diagnose_error()` helper, wrap connect calls, raise typed errors. Extend `test_connection()` into a richer `check_connection()` returning structured per-step results.
- `src/mysql_transfer/config.py` — add `find_config_file()` that searches the standard locations.
- `pyproject.toml` — add `mt` console script.
- `README.md` — update install/quick-start to `uv tool install .` and `mt`. Add `mt doctor` to the workflow. Document config auto-discovery.
- `CLAUDE.md` — update Quick Start to match.

No existing function signatures change. No config file format changes. No CLI flag renames or removals.

## Edge cases

- **No config and no CLI args.** Today this fails inside `validate()` with errors like `source: host is required`. Change: catch the "totally empty" case earlier and print the "no config found" message instead.
- **Privilege check on shared/managed MySQL.** `SHOW GRANTS` output varies (role-based grants, `GRANT ALL`, wildcards). Parse defensively — if we can't determine privileges with confidence, print a warning ("could not verify privileges") rather than a false-positive failure.
- **Doctor itself failing.** If the doctor command crashes (network blip, etc.), wrap the whole thing in a top-level try/except that reports "doctor failed unexpectedly: {err}" and exits 2, distinct from "1 problem found" (exit 1).
- **`--skip-doctor` with broken config.** Honoured. User opted out; let them hit the real error.
- **`mt` alias colliding with another tool on `$PATH`.** Document in README that `mt` is an alias and users with conflicts can ignore it; `mysql-transfer` always works.

## Testing strategy

No test suite exists today. For this change, manual verification is acceptable, but each piece must be verified before claiming done:

1. **Doctor — happy path.** Run `mt doctor` against the user's existing working config. Expect all ✓ and exit 0.
2. **Doctor — induced failures.** Temporarily corrupt one field (wrong password) and re-run. Expect a clear ✗ line and exit 1. Repeat for: wrong host, wrong port, wrong db name, SSH key missing.
3. **Pre-flight integration.** Run `mt transfer` with a bad password; expect doctor output, no transfer started, exit 1. Run with `--skip-doctor`; expect old behaviour (failure mid-transfer with wrapped error).
4. **Error wrapping.** Confirm each row of the translation table maps to the listed message by inducing each error.
5. **Auto-discover.** Run `mt doctor` from the project root with no `-c`. Expect "Using config: ./config.yaml". Move into a subdirectory; expect "no config found".
6. **`mt` alias.** After `uv tool install .`, run `mt --help` and `mysql-transfer --help` — both should work, identical output.

## Rollout

Single PR / commit series. No migration. Existing users keep their workflow; new behaviour is purely additive.

## Open questions

None. All decisions made.