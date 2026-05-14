# analytics_toolkit.sql

SQL utilities for reading, executing, loading, and transferring data across:

- Trino
- Greenplum
- ClickHouse

## Public API

```python
from analytics_toolkit import sql

sql.read(..., retry_cnt=5, timeout_increment=5)
sql.execute(..., retry_cnt=5, timeout_increment=5)
sql.execute_read(..., retry_cnt=5, timeout_increment=5)
sql.async_sql(
    ...,
    concurrency=5,
    fail_fast=True,
    soft_concurrency_cap=None,
    hard_concurrency_cap=10,
    progress=True,
)
sql.gp_vacuum(...)
sql.create_sql_table(...)
sql.ch_create_table_as(...)
sql.create_table_from_sql(...)
sql.load_df(..., retry_cnt=5, timeout_increment=5)
sql.transfer(..., trino_insert_chunk_size=1000)
sql.ch_full_table_move(...)
sql.get_sql_connection(...)
```

## Main Entry Points

- `read_sql` / `read`: run a query and return a dataframe
- `execute_sql` / `execute`: run SQL statements without returning a dataframe
- `execute_read`: run setup SQL statements and return the final statement as a
  dataframe
- `async_sql`: run a named batch of independent SQL tasks or custom pipelines
  concurrently through the existing sync APIs
- `gp_vacuum`: run Greenplum `VACUUM` outside a transaction block
- `create_sql_table`: build and execute `CREATE TABLE` statements
- `ch_create_table_as`: recreate a ClickHouse distributed/shard table pair
  from a query result
- `create_table_from_sql`: create a table from a source query's native column
  metadata, optionally inserting the query result
- `load_df`: load a pandas dataframe into a SQL table
- `transfer_table` / `transfer`: move data between supported backends
- `ch_full_table_move`: recreate a ClickHouse distributed/shard table pair
  from source DDL, copy all rows, and drop the source pair
- `get_sql_connection`: open a backend connection directly
- `with_sql_connection`: decorate a function with managed connection lifecycle

`read_sql`, `execute_sql`, `execute_read`, `load_df`, and `transfer_table` all
support `retry_cnt` and `timeout_increment`. Retries restart the whole public
operation from the beginning with a fresh connection.

`execute_read` accepts the same execution options as `execute_sql`. It splits
the provided SQL into statements, executes every statement except the last, then
reads the last statement into a pandas dataframe on the same connection.

`async_sql` is a synchronous public function: call it directly and it returns a
result dictionary. It accepts a non-empty sequence of task specs. Each spec
declares a `type` (`read`, `execute`, `execute_read`, `load_df`, `transfer`, or
`custom_sql_pipeline`). SQL task specs pass the same keyword arguments as the
matching sync function. Add an optional `name` field to control the result key;
unnamed tasks are keyed as `task_0`, `task_1`, and so on. It uses
`asyncio.to_thread` internally, so sync work runs concurrently with fresh
operations; it does not parallelize an individual SQL statement internally.
Result keys follow the input task order. With `fail_fast=True`, the first raised
task exception is raised and pending tasks are cancelled; already-running sync
work can continue until that function exits. Successful task results are
preserved, except `None` results are reported as `"success"`. With
`fail_fast=False`, failed tasks are reported under their task names as the error
text. A `tqdm` progress bar is shown by default; pass `progress=False` to
disable it.

`soft_concurrency_cap` limits actual sync worker execution. When omitted, it
defaults to `concurrency`. `hard_concurrency_cap` defaults to `10` and rejects
calls only when actual possible worker execution after soft throttling would
exceed the hard cap. Lowering `soft_concurrency_cap` is therefore a valid way to
run a large requested batch without exceeding the hard cap.

```python
import pandas as pd
from analytics_toolkit import sql

tasks = [
    {
        "name": "users",
        "type": "read",
        "connection_type": "gp",
        "query": "select user_id, segment from sandbox.users",
        "print_queries": False,
    },
    {
        "name": "refresh_summary",
        "type": "execute",
        "connection_type": "gp",
        "query": "truncate table sandbox.summary",
        "gp_break_query": True,
    },
    {
        "name": "load_scores",
        "type": "load_df",
        "connection_type": "ch",
        "destination_table": "sandbox.scores",
        "df": pd.DataFrame({"user_id": [1], "score": [10]}),
        "append": False,
        "ch_order_by": ["user_id"],
    },
    {
        "name": "copy_events",
        "type": "transfer",
        "from_db": "trino",
        "to_db": "gp_sandbox",
        "from_sql": "select * from iceberg.events.daily",
        "to_table": "sandbox.events_daily",
        "batch_size": 50_000,
    },
]

result = sql.async_sql(tasks, concurrency=3, progress=True)

users_df = result["users"]
refresh_status = result["refresh_summary"]  # "success"
loaded_rows = result["load_scores"]
transferred_rows = result["copy_events"]
```

Use `custom_sql_pipeline` for ordered Python steps that should run sequentially
inside one task while other tasks continue under the outer concurrency limit.
Each step is called as `step(context)`. The context exposes `task_name`,
`step_index`, `results`, and `last_result`. Sync steps run in a worker thread;
async steps are awaited directly. A pipeline returns the final step result.

```python
def read_row_count(context):
    return sql.read(
        "gp",
        "select count(*) as row_count from sandbox.source_table",
        print_queries=False,
    )


def transfer_if_not_empty(context):
    row_count = int(context.last_result["row_count"].iloc[0])
    if row_count == 0:
        return 0

    return sql.transfer(
        from_db="gp",
        to_db="ch",
        from_sql="select * from sandbox.source_table",
        to_table="sandbox.source_table_copy",
        ch_order_by=["id"],
    )


result = sql.async_sql(
    [
        {
            "name": "source_copy",
            "type": "custom_sql_pipeline",
            "steps": [read_row_count, transfer_if_not_empty],
        }
    ],
    concurrency=3,
)
```

Pipeline steps can launch nested batches. The nested call below requests
two-way concurrency:

```python
def load_parts_in_parallel(context):
    return sql.async_sql(
        [
            {
                "name": "load_a",
                "type": "load_df",
                "connection_type": "gp",
                "destination_table": "sandbox.part_a",
                "df": df_a,
            },
            {
                "name": "load_b",
                "type": "load_df",
                "connection_type": "gp",
                "destination_table": "sandbox.part_b",
                "df": df_b,
            },
        ],
        concurrency=2,
    )


def finalize_parts(context):
    return sql.execute(
        "gp",
        """
        create table sandbox.final_parts as
        select * from sandbox.part_a
        union all
        select * from sandbox.part_b
        """,
        print_queries=False,
    )
```

For a single large top-level batch, set an explicit soft cap below the hard cap:

```python
result = sql.async_sql(
    many_load_tasks,
    concurrency=20,
    soft_concurrency_cap=5,
    hard_concurrency_cap=10,
)
```

For Trino targets, `load_df` and `transfer_table` also accept
`trino_insert_chunk_size` to control how many rows are sent in each
parameterized multi-row insert statement. If omitted, the package falls back to
the target Trino connection's `insert_chunk_size`, then to the internal default.

## Opt-In Write Controls

Write-heavy helpers keep their existing defaults. `load_df(append=False)` still
replaces the target, and `load_df(append=True)` still appends. New callers can
use `write_mode` for explicit behavior:

- `append`: insert rows into the target.
- `replace`: recreate or clear the target using the helper's historical replace
  behavior.
- `truncate_insert`: clear existing table data and then insert rows when the
  target exists.
- `upsert`: reserved and currently rejected for all backends.

`load_df`, `transfer_table`, `create_table_from_sql`, `create_sql_table`, and
`ch_create_table_as` accept `dry_run=True` or `return_sql=True` to return a
`SqlPlan` without mutating a database. Plans contain ordered SQL statements,
aliases/backends, target metadata, and notable options. `load_df`,
`transfer_table`, and `create_table_from_sql` also accept
`return_metadata=True`; the returned `SqlOperationResult` includes row counts
that can be collected without changing the historical default return values.

Use `query_label` to add a safe SQL comment to generated statements and logs:

```python
plan = sql.load_df(
    "gp",
    "sandbox.scores",
    scores_df,
    write_mode="truncate_insert",
    dry_run=True,
    query_label="daily_score_refresh",
)

loaded = sql.load_df(
    "gp",
    "sandbox.scores",
    scores_df,
    return_metadata=True,
    query_label="daily_score_refresh",
)
loaded.rows
loaded.metadata.final_target_rows
```

`transfer_table` reads native source query column types before loading data.
Stage and newly created target tables use the closest matching target backend
types instead of pandas-inferred batch types. Final stage-to-target inserts use
explicit column lists and cast staged columns to the target types. When
`replace_target_table=False` and the target already exists, the existing target
column types are used for those final casts.

`create_table_from_sql` uses the same native metadata mapping to create an
empty target table by default. Pass `insert_data=True` to insert the source
query result after creation. Existing targets are preserved by default; pass
`drop_target_if_exists=True` to drop the target first. When `table_db` is
omitted, the table is created on `source_db`. Cross-backend inserts delegate to
`transfer_table` with `replace_target_table=False` after the target is created.
Decimal precision and scale from source metadata are only preserved when valid
for the target backend; unbounded or out-of-range numerics fall back to the
backend's safe default decimal type. Binary source columns are preserved as
`BYTEA` on Greenplum targets and `VARBINARY` on Trino targets; ClickHouse uses
`String` for binary payloads.

For ClickHouse targets, `load_df`, `transfer_table`, and
`create_table_from_sql` create a local `<target>_shard` table first and then
create the requested target as a `Distributed` table. Use `ch_partition_by`,
`ch_order_by`, `ch_engine`, `ch_cluster`, and `sharding_key` to control the
shard DDL and distributed sharding expression. The default `ch_cluster` is the
ClickHouse `{cluster}` macro so created distributed/shard table pairs are
visible across the full cluster on Yandex Managed ClickHouse.

`ch_create_table_as` is ClickHouse-only. It drops any existing target
distributed/shard table pair, creates a new `<target>_shard` table from the
provided query schema, creates the target `Distributed` table, and inserts the
query result into the distributed target. It accepts the same ClickHouse DDL
options as `load_df`: `ch_partition_by`, `ch_order_by`, `ch_engine`,
`ch_cluster`, and `sharding_key`. Its default `ch_cluster` is the ClickHouse
`{cluster}` macro so the created tables are visible across the full cluster on
Yandex Managed ClickHouse.

`ch_full_table_move` is ClickHouse-only. It reads `SHOW CREATE TABLE` for
`move_table`, extracts the source shard table from its `Distributed` engine, and
then reads that shard DDL. It creates the destination shard/distributed pair
with the same columns, types, engine clauses, settings, and sharding expression.
By default the destination uses the ClickHouse `{cluster}` macro; pass
`ch_cluster=None` to reuse the cluster extracted from the source DDL. It copies
rows with `INSERT INTO <to_table> SELECT * FROM <move_table>`, then drops the
source pair.

## Greenplum Maintenance

Use `gp_vacuum` for Greenplum vacuum operations that must run outside a transaction
block.

```python
from analytics_toolkit import sql

sql.gp_vacuum("cvm_sbx.some_table")
sql.gp_vacuum("cvm_sbx.some_table", analyze=True)
sql.gp_vacuum("cvm_sbx.some_table", full=True, verbose=True)
```

## Configuration

Connection settings are read from `.connections`. The package searches from
the current working directory upward through parent directories. Public SQL
functions accept a key from that file; backend behavior is selected from the
key's `type`.

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
  },
  "trino": {
    "type": "trino",
    "host": "trino.example",
    "port": 8080,
    "user": "user",
    "password": "password",
    "catalog": "iceberg",
    "schema": "sandbox",
    "insert_chunk_size": 1000
  },
  "ch": {
    "type": "ch",
    "host": "ch.example",
    "port": 8123,
    "user": "user",
    "password": "password",
    "database": "default"
  }
}
```

Trino supports optional `auth_mode`, `http_scheme`, `verify`,
`use_keychain_certs`, `keychain_cert_names`, and `insert_chunk_size` fields.

Validate connection files from Python or the CLI:

```python
from analytics_toolkit import sql

for result in sql.validate_connections(["gp", "trino"]):
    print(result.connection_key, result.valid, result.error)
```

```bash
analytics-toolkit sql validate
analytics-toolkit sql validate gp trino --connect
analytics-toolkit sql support-matrix
```

## SQL Support Matrix

| Backend | Parser dialect | Transactions | Analyze | Distributed DDL | Write modes |
| --- | --- | --- | --- | --- | --- |
| `gp` | `postgres` | yes | yes | no | `append`, `replace`, `truncate_insert` |
| `trino` | `trino` | no | yes | no | `append`, `replace`, `truncate_insert` |
| `ch` | `clickhouse` | no | no | yes | `append`, `replace`, `truncate_insert` |

## Migration From Env Vars

Previous env-based configuration is no longer read. Move values into
`.connections`:

- `GP_HOST`, `GP_PORT`, `GP_USER`, `GP_PASSWORD`, `GP_DATABASE` -> a connection
  with `"type": "gp"` and fields `host`, `port`, `user`, `password`, `database`
- `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_PASSWORD`, `TRINO_CATALOG`,
  `TRINO_SCHEMA`, `TRINO_AUTH_MODE`, `TRINO_HTTP_SCHEME`, `TRINO_VERIFY`,
  `TRINO_USE_KEYCHAIN_CERTS`, `TRINO_KEYCHAIN_CERT_NAMES` -> a connection with
  `"type": "trino"` and matching lower-case fields
- `TRINO_INSERT_CHUNK_SIZE` -> Trino connection field `insert_chunk_size`
- `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE`, `CH_SECURE` ->
  a connection with `"type": "ch"` and matching lower-case fields
- `SQL_CONNECTIONS` -> the complete `.connections` file content

## Internal Layout

- `connection/`: connection config and backend connection creation
- `ddl/`: table-creation helpers
- `dml/io/`: read and execute operations
- `dml/load/`: dataframe loading and staging helpers
- `dml/transfer/`: table transfer flow and runtime models
