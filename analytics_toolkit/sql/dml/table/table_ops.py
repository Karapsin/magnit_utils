from __future__ import annotations

from typing import Any

import pandas as pd

from ...connection.config import (
    TrinoConfig,
    get_connection_config,
    resolve_connection_backend,
)
from ...connection.get_sql_connection import get_sql_connection
from ...ddl.create_sql_table import build_ch_shard_table_name, create_sql_table
from ...ddl.create_sql_table import quote_identifier
from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from analytics_toolkit.general import time_print


def table_exists(
    connection_type: str,
    connection: Any,
    table_name: str,
    connection_key: str | None = None,
) -> bool:
    backend = resolve_connection_backend(connection_type)
    if backend == "gp":
        return _gp_table_exists(connection, table_name)
    if backend == "trino":
        return _trino_table_exists(
            connection,
            table_name,
            connection_key or connection_type,
        )
    if backend == "ch":
        return _ch_table_exists(connection, table_name)

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def clear_target_table(connection_type: str, connection: Any, table_name: str) -> None:
    time_print(f"Clearing target table {table_name} on {connection_type}")
    backend = resolve_connection_backend(connection_type)

    if backend == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            connection.commit()
            return
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if backend == "trino":
        cursor = connection.cursor()
        try:
            cursor.execute(f"DELETE FROM {table_name}")
            return
        finally:
            cursor.close()

    if backend == "ch":
        _truncate_ch_table(connection, table_name)
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def finalize_stage_table(
    connection_type: str,
    connection: Any,
    stage_table: str,
    target_table: str,
    replace_target_table: bool,
    target_exists: bool,
    sample_batch: pd.DataFrame,
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: list[str] | str | None = None,
    ch_order_by: list[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "core",
    ch_sharding_key: str = "rand()",
) -> None:
    time_print(
        f"Finalizing staged transfer from {stage_table} into {target_table} on {connection_type}"
    )
    backend = resolve_connection_backend(connection_type)

    if backend == "ch":
        if replace_target_table:
            drop_ch_distributed_table_pair(
                connection,
                target_table,
                ch_cluster=ch_cluster,
            )
            target_exists = False
        if not target_exists:
            create_sql_table(
                connection_type,
                connection,
                target_table,
                sample_batch,
                gp_distributed_by_key=gp_distributed_by_key,
                ch_partition_by=ch_partition_by,
                ch_order_by=ch_order_by,
                ch_engine=ch_engine,
                ch_cluster=ch_cluster,
                ch_sharding_key=ch_sharding_key,
                ch_distributed_table=True,
            )
        insert_from_table(backend, connection, target_table, stage_table)
        return

    if not target_exists:
        create_sql_table(
            backend,
            connection,
            target_table,
            sample_batch,
            gp_distributed_by_key=gp_distributed_by_key,
        )
    elif replace_target_table:
        clear_target_table(backend, connection, target_table)

    insert_from_table(backend, connection, target_table, stage_table)


def analyze_table(
    connection_type: str,
    connection: Any,
    table_name: str,
) -> None:
    backend = resolve_connection_backend(connection_type)
    if backend == "ch":
        return

    time_print(f"Analyzing target table {table_name} on {connection_type}")
    sql = f"ANALYZE {table_name}"

    if backend == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            connection.commit()
            return
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if backend == "trino":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return
        finally:
            cursor.close()

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def gp_vacuum(
    table_name: str,
    analyze: bool = False,
    full: bool = False,
    verbose: bool = True,
    connection_key: str = "gp",
) -> None:
    config = get_connection_config(connection_key)
    if config.backend != "gp":
        raise UnsupportedConnectionTypeError(
            f"gp_vacuum requires a gp connection, got '{config.backend}'."
        )

    conn = get_sql_connection(config.connection_key)
    qualified_table_name = quote_qualified_table_name(table_name, "gp")
    options: list[str] = []
    if full:
        options.append("FULL")
    if verbose:
        options.append("VERBOSE")
    if analyze:
        options.append("ANALYZE")

    options_sql = f" ({', '.join(options)})" if options else ""
    sql = f"VACUUM{options_sql} {qualified_table_name}"

    time_print(f"Vacuuming table {qualified_table_name} on gp")
    try:
        previous_autocommit = conn.autocommit
        cursor = conn.cursor()
        try:
            conn.autocommit = True
            cursor.execute(sql)
        finally:
            cursor.close()
            conn.autocommit = previous_autocommit
    finally:
        time_print(f"Closing {config.connection_key} connection")
        conn.close()


def drop_table_with_retry(
    connection_backend: str,
    connection_key: str,
    connection_ref: dict[str, Any],
    table_name: str,
    retry_fn: Any,
    retry_cnt: int,
    timeout_increment: int | float,
    rollback_fn: Any,
    replace_connection_fn: Any,
) -> None:
    backend = resolve_connection_backend(connection_backend)

    def operation(attempt: int) -> None:
        connection = connection_ref["connection"]
        try:
            drop_table(backend, connection, table_name)
            return None
        except Exception:
            if backend == "gp":
                rollback_fn(connection)
            replace_connection_fn(connection_key, connection_ref)
            raise

    retry_fn(
        operation_name=f"dropping stage table {table_name} on {connection_key}",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )


def drop_table(
    connection_type: str,
    connection: Any,
    table_name: str,
    ch_cluster: str | None = None,
) -> None:
    backend = resolve_connection_backend(connection_type)
    if backend == "gp":
        sql = f"DROP TABLE IF EXISTS {table_name}"
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            connection.commit()
            return
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if backend == "trino":
        sql = f"DROP TABLE IF EXISTS {table_name}"
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return
        finally:
            cursor.close()

    if backend == "ch":
        sql = f"DROP TABLE IF EXISTS {table_name}{_ch_cluster_clause(ch_cluster)}"
        _execute_ch_command(connection, sql)
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def drop_ch_distributed_table_pair(
    connection: Any,
    table_name: str,
    ch_cluster: str = "core",
) -> None:
    shard_table = build_ch_shard_table_name(table_name)
    drop_table("ch", connection, table_name)
    drop_table("ch", connection, shard_table)
    drop_table("ch", connection, table_name, ch_cluster=ch_cluster)
    drop_table(
        "ch",
        connection,
        shard_table,
        ch_cluster=ch_cluster,
    )


def clear_ch_distributed_table_data(
    connection: Any,
    table_name: str,
    ch_cluster: str = "core",
) -> None:
    shard_table = build_ch_shard_table_name(table_name)
    _truncate_ch_table(connection, shard_table, ch_cluster=ch_cluster)
    _truncate_ch_table(connection, table_name)


def get_trino_table_column_types(
    connection: Any,
    table_name: str,
    connection_key: str = "trino",
) -> dict[str, str]:
    catalog, schema_name, relation_name = split_trino_table_name(
        table_name,
        connection_key=connection_key,
    )

    cursor = connection.cursor()
    try:
        cursor.execute(
            f"""
            SELECT column_name, data_type
            FROM {catalog}.information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
            ORDER BY ordinal_position
            """.strip(),
            (schema_name, relation_name),
        )
        return {
            str(column_name): str(data_type)
            for column_name, data_type in cursor.fetchall()
        }
    finally:
        cursor.close()


def insert_from_table(
    connection_type: str,
    connection: Any,
    target_table: str,
    source_table: str,
) -> None:
    sql = f"INSERT INTO {target_table} SELECT * FROM {source_table}"
    backend = resolve_connection_backend(connection_type)

    if backend == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            connection.commit()
            return
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if backend == "trino":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return
        finally:
            cursor.close()

    if backend == "ch":
        connection.command(sql)
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def split_trino_table_name(
    table_name: str,
    connection_key: str = "trino",
) -> tuple[str, str, str]:
    parts = [part.strip() for part in table_name.split(".") if part.strip()]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]

    config = get_connection_config(connection_key)
    if not isinstance(config, TrinoConfig):
        raise ValueError("Invalid Trino configuration.")

    if len(parts) == 2:
        if not config.catalog:
            raise ValueError(
                f"Trino table operations for schema-qualified names require "
                f".connections['{config.connection_key}'].catalog."
            )
        return config.catalog, parts[0], parts[1]
    if len(parts) == 1:
        if not config.catalog or not config.schema:
            raise ValueError(
                f"Trino table operations for unqualified names require "
                f".connections['{config.connection_key}'].catalog and schema."
            )
        return config.catalog, config.schema, parts[0]
    raise ValueError(f"Invalid table name: {table_name}")


def quote_qualified_table_name(table_name: str, connection_type: str) -> str:
    parts = [part.strip() for part in table_name.split(".")]
    if not parts or any(not part for part in parts):
        raise InvalidSqlInputError("Table name must be a non-empty identifier.")
    if len(parts) > 3:
        raise InvalidSqlInputError(
            "Table name must be unqualified or dot-qualified up to three parts."
        )
    return ".".join(quote_identifier(part, connection_type) for part in parts)


def _truncate_ch_table(
    connection: Any,
    table_name: str,
    ch_cluster: str | None = None,
) -> None:
    _execute_ch_command(
        connection,
        f"TRUNCATE TABLE IF EXISTS {table_name}{_ch_cluster_clause(ch_cluster)}",
    )


def _ch_cluster_clause(ch_cluster: str | None) -> str:
    if ch_cluster is None:
        return ""
    normalized = ch_cluster.strip()
    if not normalized:
        raise ValueError("ch_cluster must not be empty.")
    return f" ON CLUSTER {normalized}"


def _execute_ch_command(connection: Any, sql: str) -> None:
    if "ON CLUSTER" not in sql:
        connection.command(sql)
        return

    try:
        connection.command(
            sql,
            settings={
                "distributed_ddl_task_timeout": 300,
            },
        )
    except TypeError:
        connection.command(sql)


def _gp_table_exists(connection: Any, table_name: str) -> bool:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT to_regclass(%s)", (table_name,))
        row = cursor.fetchone()
        return bool(row and row[0])
    finally:
        cursor.close()


def _trino_table_exists(
    connection: Any,
    table_name: str,
    connection_key: str,
) -> bool:
    catalog, schema_name, relation_name = split_trino_table_name(
        table_name,
        connection_key=connection_key,
    )
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"""
            SELECT 1
            FROM {catalog}.information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            """.strip(),
            (schema_name, relation_name),
        )
        return cursor.fetchone() is not None
    finally:
        cursor.close()


def _ch_table_exists(client: Any, table_name: str) -> bool:
    result = client.query(f"EXISTS TABLE {table_name}")
    return bool(result.result_rows and result.result_rows[0][0])
