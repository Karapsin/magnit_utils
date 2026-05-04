# AGENTS.md

## Scope

These instructions apply to the whole repository.

## Project Overview

`analytics_toolkit` is a Python 3.11+ utility package with five public areas:

- `analytics_toolkit.ab_utils`: AB-test metric comparison helpers.
- `analytics_toolkit.sql`: SQL read/execute/load/transfer helpers for Greenplum, Trino, and ClickHouse.
- `analytics_toolkit.excel`: long-format dataframe to Excel report helpers.
- `analytics_toolkit.dates`: date and period helpers.
- `analytics_toolkit.general`: shared logging and file path helpers.

Keep public APIs stable unless the user explicitly asks for a breaking change. Many tests import underscore helpers through package re-export modules, so treat exported internals as compatibility surface too.

## Development Commands

Use a temporary bytecode cache when running Python commands from this sandboxed workspace:

```bash
PYTHONPYCACHEPREFIX=/tmp/utils_dev_pycache python -m compileall analytics_toolkit tests
PYTHONPYCACHEPREFIX=/tmp/utils_dev_pycache pytest -q
```

Focused test files:

```bash
PYTHONPYCACHEPREFIX=/tmp/utils_dev_pycache pytest -q tests/test_ab_utils_metrics.py
PYTHONPYCACHEPREFIX=/tmp/utils_dev_pycache pytest -q tests/test_excel_long_format.py
PYTHONPYCACHEPREFIX=/tmp/utils_dev_pycache pytest -q tests/test_general_read_file.py
PYTHONPYCACHEPREFIX=/tmp/utils_dev_pycache pytest -q tests/test_sql_connection_config.py tests/test_sql_retries.py tests/test_sql_load_table.py
```

Do not run tests against real databases. Unit tests should use fake connections, monkeypatching, and the autouse env fixture in `tests/conftest.py`.

## General Rules

- Prefer small, local changes that follow existing module patterns.
- Do not bump package versions, alter packaging metadata, or rewrite README/manual docs unless the task requires it.
- When changing public behavior, update the relevant module README and focused tests.
- Keep `.connections` out of the repo. Tests should create a temporary `.connections` and chdir into that temp project.
- Use existing structured parsers for SQL/table names (`sqlparse`, `sqlglot`) instead of ad hoc parsing where those modules already do the job.

## SQL Module Contracts

- Public SQL APIs accept connection keys/aliases from `.connections`; callers should not need to pass backend names separately.
- Each `.connections` value must include `type` as `gp`, `trino`, or `ch`. Backend dispatch comes from this `type`, while reconnect/retry/log messages keep using the alias key.
- Env-based SQL config such as `SQL_CONNECTIONS`, `GP_HOST`, `TRINO_HOST`, `CH_HOST`, `TRINO_INSERT_CHUNK_SIZE`, and config-file override env vars is intentionally unsupported. Do not restore fallback support.
- Keep public names such as `connection_type`, `from_db`, and `to_db` compatible even when they now represent aliases.
- A Trino target may define `insert_chunk_size` in its connection config. Explicit function arguments override config; config overrides the internal default.
- `read_sql`, `execute_sql`, `load_df`, and `transfer_table` retry the whole public operation with fresh connections. Preserve Greenplum rollback behavior on errors.
- `transfer_table` and `load_df` separate key and backend in option models. Same-backend aliases are valid as long as the alias keys differ.
- ClickHouse load/transfer creates and manages a shard table plus a `Distributed` table. Preserve local and cluster DDL/drop/truncate behavior.
- Key validation uses normalized unique key lists and null-safe joins for staged-vs-target overlap checks.
- Trino table metadata helpers need the alias key so unqualified names can use that connection's catalog/schema.

## SQL Layout Notes

- `connection/config.py`: finds `.connections`, parses it as JSON, normalizes aliases to lowercase, validates fields, and resolves alias to backend.
- `connection/get_sql_connection.py`: opens backend clients and handles optional Trino keychain certificate bundles under `MAGNIT_UTILS_HOME` or the env-file directory.
- `ddl/create_sql_table.py`: infers dataframe column types, quotes identifiers per backend, and builds ClickHouse distributed DDL.
- `dml/io`: read/execute helpers using `sqlparse`; `read_sql` accepts exactly one statement.
- `dml/load`: dataframe loading, stage table creation, batch insertion, Trino chunking, and backend-specific scalar normalization.
- `dml/table`: shared table existence, analyze, drop, vacuum, stage finalization, and validation helpers.
- `dml/transfer`: staged transfer flow, source streaming, full retry/restart behavior, and connection replacement helpers.

## AB Utilities Contracts

- `compute_test_metrics` expects one row per user, a non-null unique user id, a non-null group column, and at least one mean or ratio metric.
- Output column order is part of the API; preserve placement of `metric_type`, group columns, `p-value CUPED`, and `bootstrap_adj_p`.
- `analytics_toolkit.ab_utils.metrics` re-exports many underscore helpers. Tests may import those names directly.
- Ratio metrics support only `level="agg"` or `level="user"` and `invalid_denominator="ignore"`.
- Missing metric values are ignored per metric/group; non-numeric metric values should raise.
- CUPED failures should warn and return `NaN`, not abort the whole metric computation when validation has passed.
- Bootstrap multiple-comparison adjustment should remain deterministic when `bootstrap_random_state` is set and should fall back from process pools to threads when process pools are unavailable.

## Excel Contracts

- `pivot_and_break_table` and `break_table` accept either one dataframe or a sequence of dataframes.
- Preserve sheet grouping order, table order, side-by-side placement for multiple dataframes, and blank spacing between table blocks.
- Preserve sheet-name sanitization, 31-character truncation, and deduplication for append mode.
- Decimal values are coerced to floats before writing to Excel.
- `enforce_same_row_order=True` aligns later dataframe tables to the first dataframe and rejects extra row labels.

## Dates Contracts

- Date helpers accept ISO strings, `date`, or `datetime` values.
- The default return type is an ISO string; `output_string=False` returns midnight `datetime` values.
- Weekly and monthly sequences truncate start/end dates to the period start and emit warnings when truncation happens.
- `add_weeks` and `add_months` operate from the week/month start, not from the exact input day.

## General Module Contracts

- `time_print` prints timestamped messages and is re-exported through `analytics_toolkit.general` and `analytics_toolkit.sql`.
- `here()` prefers the caller's `__main__.__file__` directory, then falls back to the current working directory and unique cwd matches.
- `read_file()` raises `InvalidSqlInputError` for missing files and applies `str.format(**params_dict)` only when params are provided.
- Preserve the `analytics_toolkit.general.read_file.inspect` compatibility assignment; tests monkeypatch through that dotted path.
