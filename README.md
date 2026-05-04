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

SQL connection settings are read from `.connections`. The package searches
from the current working directory upward through parent directories. Each key is
the public connection alias used by `analytics_toolkit.sql`; each value must
include `type` as one of `gp`, `trino`, or `ch`.

```json
{
  "gp": {
    "type": "gp",
    "host": "gp.example",
    "port": 5432,
    "user": "user",
    "password": "password",
    "database": "db"
  },
  "gp_sandbox": {
    "type": "gp",
    "host": "gp-sandbox.example",
    "user": "user",
    "password": "password",
    "database": "sandbox"
  }
}
```

Legacy variables such as `GP_HOST`, `TRINO_HOST`, `CH_HOST`, `SQL_CONNECTIONS`,
and `TRINO_INSERT_CHUNK_SIZE` are not read. Move connection settings into
`.connections`; Trino insert chunk sizing is the Trino connection field
`insert_chunk_size`.

If a Trino connection sets `use_keychain_certs=true`, the generated CA bundle is
written to:

- `<connections_file_directory>/certs/trino-<connection-key>-keychain-ca.pem`

You can override the state/output directory with `MAGNIT_UTILS_HOME`.

## Package Layout

- `analytics_toolkit/ab_utils`: AB-test metric comparison helpers, including `compute_test_metrics`
- `analytics_toolkit/dates`: date and period helpers
- `analytics_toolkit/excel`: Excel formatting helpers
- `analytics_toolkit/sql`: SQL execution, loading, and transfer helpers
