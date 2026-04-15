# analytics_toolkit

`analytics_toolkit` is a small utility package for:

- AB-test related helpers
- SQL I/O and table-loading helpers for Trino, Greenplum, and ClickHouse
- date helpers for common period calculations
- Excel helpers for writing pivoted tables from long-format data

## Install

From the repository root:

```bash
pip install git+https://github.com/Karapsin/analytics_toolkit.git
```

## Quick Start

```python
from analytics_toolkit.ab_utils import compute_test_metrics
from analytics_toolkit import sql
from analytics_toolkit.dates.dates import first_day
from analytics_toolkit.excel import break_table, pivot_and_break_table
```

## Configuration

Connection settings are read from environment variables. By default the package looks
for a `.env` file starting from the current working directory and walking up through
its parents.

If `TRINO_USE_KEYCHAIN_CERTS=true`, the generated Trino CA bundle is written to:

- `<project_root>/certs/trino-keychain-ca.pem` when a `.env` file is found
- `<current_working_directory>/certs/trino-keychain-ca.pem` otherwise

You can override the env file path with `MAGNIT_UTILS_ENV_FILE` and the state/output
directory with `MAGNIT_UTILS_HOME`.

## Package Layout

- `analytics_toolkit/ab_utils`: AB-test metric comparison helpers, including `compute_test_metrics`
- `analytics_toolkit/dates`: date and period helpers
- `analytics_toolkit/excel`: Excel formatting helpers
- `analytics_toolkit/sql`: SQL execution, loading, and transfer helpers
