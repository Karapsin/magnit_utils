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
sql.gp_vacuum(...)
sql.create_sql_table(...)
sql.load_df(..., retry_cnt=5, timeout_increment=5)
sql.transfer(..., trino_insert_chunk_size=1000)
sql.get_sql_connection(...)
```

## Main Entry Points

- `read_sql` / `read`: run a query and return a dataframe
- `execute_sql` / `execute`: run SQL statements without returning a dataframe
- `gp_vacuum`: run Greenplum `VACUUM` outside a transaction block
- `create_sql_table`: build and execute `CREATE TABLE` statements
- `load_df`: load a pandas dataframe into a SQL table
- `transfer_table` / `transfer`: move data between supported backends
- `get_sql_connection`: open a backend connection directly
- `with_sql_connection`: decorate a function with managed connection lifecycle

`read_sql`, `execute_sql`, `load_df`, and `transfer_table` all support
`retry_cnt` and `timeout_increment`. Retries restart the whole public operation
from the beginning with a fresh connection.

For Trino targets, `load_df` and `transfer_table` also accept
`trino_insert_chunk_size` to control how many rows are sent in each
parameterized multi-row insert statement. If omitted, the package falls back to
the target Trino connection's `insert_chunk_size`, then to the internal default.

For ClickHouse targets, `load_df` and `transfer_table` create a local
`<target>_shard` table first and then create the requested target as a
`Distributed` table. Use `ch_partition_by`, `ch_order_by`, `ch_engine`,
`ch_cluster`, and `sharding_key` to control the shard DDL and distributed
sharding expression.

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
