# analytics_toolkit.sql

SQL utilities for reading, executing, loading, and transferring data across:

- Trino
- Greenplum
- ClickHouse

## Public API

```python
from analytics_toolkit import sql

sql.read(...)
sql.execute(...)
sql.create_sql_table(...)
sql.load_df(...)
sql.transfer(...)
sql.get_sql_connection(...)
```

## Main Entry Points

- `read_sql` / `read`: run a query and return a dataframe
- `execute_sql` / `execute`: run SQL statements without returning a dataframe
- `create_sql_table`: build and execute `CREATE TABLE` statements
- `load_df`: load a pandas dataframe into a SQL table
- `transfer_table` / `transfer`: move data between supported backends
- `get_sql_connection`: open a backend connection directly
- `with_sql_connection`: decorate a function with managed connection lifecycle

## Configuration

Connection settings are read from environment variables, typically through a `.env`
file in the working project.

Common variables include:

- `TRINO_HOST`
- `TRINO_PORT`
- `TRINO_USER`
- `TRINO_PASSWORD`
- `TRINO_USE_KEYCHAIN_CERTS`
- `TRINO_KEYCHAIN_CERT_NAMES`
- `GP_HOST`
- `GP_PORT`
- `GP_USER`
- `GP_PASSWORD`
- `GP_DATABASE`
- `CH_HOST`
- `CH_PORT`
- `CH_USER`
- `CH_PASSWORD`
- `CH_DATABASE`

If keychain-backed Trino certificates are enabled, the package can export a CA bundle
into a `certs/` directory under the active project root. This behavior is optional
and only relevant in environments that rely on local keychain-managed certificates.

## Internal Layout

- `connection/`: connection config and backend connection creation
- `ddl/`: table-creation helpers
- `dml/io/`: read and execute operations
- `dml/load/`: dataframe loading and staging helpers
- `dml/transfer/`: table transfer flow and runtime models
