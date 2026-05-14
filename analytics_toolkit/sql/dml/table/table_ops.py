from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

from ...connection.config import (
    TrinoConfig,
    get_connection_config,
    resolve_connection_backend,
)
from ...connection.get_sql_connection import get_sql_connection
from ...ddl.create_sql_table import (
    build_ch_shard_table_name,
    column_list_sql,
    create_sql_table,
)
from ...ddl.create_sql_table import quote_identifier
from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from ...labels import apply_query_label
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


def build_clear_table_sqls(
    connection_type: str,
    table_name: str,
    query_label: str | None = None,
) -> list[str]:
    backend = resolve_connection_backend(connection_type)
    if backend == "gp":
        return [apply_query_label(f"TRUNCATE TABLE {table_name}", query_label)]
    if backend == "trino":
        return [apply_query_label(f"DELETE FROM {table_name}", query_label)]
    if backend == "ch":
        return [
            apply_query_label(
                f"TRUNCATE TABLE IF EXISTS {table_name}",
                query_label,
            )
        ]
    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def build_drop_table_sql(
    connection_type: str,
    table_name: str,
    ch_cluster: str | None = None,
    query_label: str | None = None,
) -> str:
    backend = resolve_connection_backend(connection_type)
    if backend in {"gp", "trino"}:
        return apply_query_label(f"DROP TABLE IF EXISTS {table_name}", query_label)
    if backend == "ch":
        return apply_query_label(
            f"DROP TABLE IF EXISTS {table_name}{_ch_cluster_clause(ch_cluster)}",
            query_label,
        )
    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def build_drop_ch_distributed_table_pair_sqls(
    table_name: str,
    ch_cluster: str = "{cluster}",
    query_label: str | None = None,
) -> list[str]:
    shard_table = build_ch_shard_table_name(table_name)
    return [
        build_drop_table_sql("ch", table_name, query_label=query_label),
        build_drop_table_sql("ch", shard_table, query_label=query_label),
        build_drop_table_sql(
            "ch",
            table_name,
            ch_cluster=ch_cluster,
            query_label=query_label,
        ),
        build_drop_table_sql(
            "ch",
            shard_table,
            ch_cluster=ch_cluster,
            query_label=query_label,
        ),
    ]


def build_analyze_table_sql(
    connection_type: str,
    table_name: str,
    query_label: str | None = None,
) -> str:
    backend = resolve_connection_backend(connection_type)
    if backend == "ch":
        raise UnsupportedConnectionTypeError("ClickHouse does not support ANALYZE here.")
    if backend in {"gp", "trino"}:
        return apply_query_label(f"ANALYZE {table_name}", query_label)
    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def clear_target_table(
    connection_type: str,
    connection: Any,
    table_name: str,
    query_label: str | None = None,
) -> None:
    time_print(f"Clearing target table {table_name} on {connection_type}")
    backend = resolve_connection_backend(connection_type)
    sqls = build_clear_table_sqls(backend, table_name, query_label=query_label)

    if backend == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(sqls[0])
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
            cursor.execute(sqls[0])
            return
        finally:
            cursor.close()

    if backend == "ch":
        _truncate_ch_table(connection, table_name, query_label=query_label)
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
    target_column_types: Mapping[str, str] | None = None,
    insert_column_types: Mapping[str, str] | None = None,
    write_mode: str = "replace",
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: list[str] | str | None = None,
    ch_order_by: list[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    query_label: str | None = None,
) -> None:
    time_print(
        f"Finalizing staged transfer from {stage_table} into {target_table} on {connection_type}"
    )
    backend = resolve_connection_backend(connection_type)

    if backend == "ch":
        if replace_target_table:
            if write_mode == "truncate_insert" and target_exists:
                clear_ch_distributed_table_data(
                    connection,
                    target_table,
                    ch_cluster=ch_cluster,
                    query_label=query_label,
                )
            else:
                drop_ch_distributed_table_pair(
                    connection,
                    target_table,
                    ch_cluster=ch_cluster,
                    query_label=query_label,
                )
                target_exists = False
        if not target_exists:
            create_sql_table(
                connection_type,
                connection,
                target_table,
                sample_batch,
                column_types=target_column_types,
                gp_distributed_by_key=gp_distributed_by_key,
                ch_partition_by=ch_partition_by,
                ch_order_by=ch_order_by,
                ch_engine=ch_engine,
                ch_cluster=ch_cluster,
                ch_sharding_key=ch_sharding_key,
                ch_distributed_table=True,
                query_label=query_label,
            )
        insert_from_table(
            backend,
            connection,
            target_table,
            stage_table,
            column_types=insert_column_types,
            query_label=query_label,
        )
        return

    if not target_exists:
        create_sql_table(
            backend,
            connection,
            target_table,
            sample_batch,
            column_types=target_column_types,
            gp_distributed_by_key=gp_distributed_by_key,
            query_label=query_label,
        )
    elif replace_target_table:
        clear_target_table(backend, connection, target_table, query_label=query_label)

    insert_from_table(
        backend,
        connection,
        target_table,
        stage_table,
        column_types=insert_column_types,
        query_label=query_label,
    )


def analyze_table(
    connection_type: str,
    connection: Any,
    table_name: str,
    query_label: str | None = None,
) -> None:
    backend = resolve_connection_backend(connection_type)
    if backend == "ch":
        return

    time_print(f"Analyzing target table {table_name} on {connection_type}")
    sql = build_analyze_table_sql(backend, table_name, query_label=query_label)

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
    query_label: str | None = None,
) -> None:
    backend = resolve_connection_backend(connection_backend)

    def operation(attempt: int) -> None:
        connection = connection_ref["connection"]
        try:
            drop_table(backend, connection, table_name, query_label=query_label)
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
    query_label: str | None = None,
) -> None:
    backend = resolve_connection_backend(connection_type)
    sql = build_drop_table_sql(
        backend,
        table_name,
        ch_cluster=ch_cluster,
        query_label=query_label,
    )
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
        _execute_ch_command(connection, sql)
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def drop_ch_distributed_table_pair(
    connection: Any,
    table_name: str,
    ch_cluster: str = "{cluster}",
    query_label: str | None = None,
) -> None:
    shard_table = build_ch_shard_table_name(table_name)
    drop_table("ch", connection, table_name, query_label=query_label)
    drop_table("ch", connection, shard_table, query_label=query_label)
    drop_table("ch", connection, table_name, ch_cluster=ch_cluster, query_label=query_label)
    drop_table(
        "ch",
        connection,
        shard_table,
        ch_cluster=ch_cluster,
        query_label=query_label,
    )


def clear_ch_distributed_table_data(
    connection: Any,
    table_name: str,
    ch_cluster: str = "{cluster}",
    query_label: str | None = None,
) -> None:
    shard_table = build_ch_shard_table_name(table_name)
    _truncate_ch_table(
        connection,
        shard_table,
        ch_cluster=ch_cluster,
        query_label=query_label,
    )
    _truncate_ch_table(connection, table_name, query_label=query_label)


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


def get_table_column_types(
    connection_type: str,
    connection: Any,
    table_name: str,
    connection_key: str | None = None,
) -> dict[str, str]:
    backend = resolve_connection_backend(connection_type)
    if backend == "gp":
        return _get_gp_table_column_types(connection, table_name)
    if backend == "trino":
        return get_trino_table_column_types(
            connection,
            table_name,
            connection_key=connection_key or connection_type,
        )
    if backend == "ch":
        return _get_ch_table_column_types(connection, table_name)
    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def _get_gp_table_column_types(connection: Any, table_name: str) -> dict[str, str]:
    schema_name, relation_name = _split_gp_table_name(table_name)
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """.strip(),
            (schema_name, relation_name),
        )
        return {
            str(column_name): _format_gp_information_schema_type(
                str(data_type),
                udt_name,
                numeric_precision,
                numeric_scale,
            )
            for (
                column_name,
                data_type,
                udt_name,
                numeric_precision,
                numeric_scale,
            ) in cursor.fetchall()
        }
    finally:
        cursor.close()


def _split_gp_table_name(table_name: str) -> tuple[str, str]:
    parts = [part.strip().strip('"') for part in table_name.split(".") if part.strip()]
    if len(parts) == 1:
        return "public", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"Invalid Greenplum table name: {table_name}")


def _format_gp_information_schema_type(
    data_type: str,
    udt_name: Any,
    numeric_precision: Any,
    numeric_scale: Any,
) -> str:
    normalized = data_type.lower()
    if normalized == "numeric" and numeric_precision is not None:
        if numeric_scale is None:
            return f"NUMERIC({numeric_precision})"
        return f"NUMERIC({numeric_precision}, {numeric_scale})"
    if normalized == "character varying":
        return "VARCHAR"
    if normalized == "timestamp without time zone":
        return "TIMESTAMP"
    if normalized == "timestamp with time zone":
        return "TIMESTAMP WITH TIME ZONE"
    if normalized == "integer":
        return "INTEGER"
    if normalized == "bigint":
        return "BIGINT"
    if normalized == "smallint":
        return "SMALLINT"
    if normalized == "boolean":
        return "BOOLEAN"
    if normalized == "date":
        return "DATE"
    if normalized == "text":
        return "TEXT"
    return str(udt_name or data_type).upper()


def _get_ch_table_column_types(connection: Any, table_name: str) -> dict[str, str]:
    result = connection.query(f"DESCRIBE TABLE {table_name}")
    rows = getattr(result, "result_rows", None) or []
    return {
        str(row[0]): str(row[1])
        for row in rows
        if len(row) >= 2
    }


def insert_from_table(
    connection_type: str,
    connection: Any,
    target_table: str,
    source_table: str,
    column_types: Mapping[str, str] | None = None,
    query_label: str | None = None,
) -> None:
    backend = resolve_connection_backend(connection_type)
    sql = apply_query_label(
        _build_insert_from_table_sql(
            backend,
            target_table,
            source_table,
            column_types,
        ),
        query_label,
    )

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


def insert_from_query(
    connection_type: str,
    connection: Any,
    target_table: str,
    source_sql: str,
    column_types: Mapping[str, str],
    query_label: str | None = None,
) -> int:
    backend = resolve_connection_backend(connection_type)
    sql = build_insert_from_query_sql(
        backend,
        target_table,
        source_sql,
        column_types,
        query_label=query_label,
    )

    if backend == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            row_count = _extract_row_count(cursor)
            connection.commit()
            return row_count
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if backend == "trino":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return _extract_row_count(cursor)
        finally:
            cursor.close()

    if backend == "ch":
        result = connection.command(sql)
        return _extract_row_count(result)

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def build_insert_from_query_sql(
    connection_type: str,
    target_table: str,
    source_sql: str,
    column_types: Mapping[str, str],
    query_label: str | None = None,
) -> str:
    backend = resolve_connection_backend(connection_type)
    query_sql = source_sql.strip().removesuffix(";").strip()
    return apply_query_label(
        _build_typed_insert_select_sql(
            backend,
            target_table,
            f"FROM ({query_sql}) AS source_query",
            column_types,
        ),
        query_label,
    )


def build_insert_from_table_sql(
    connection_type: str,
    target_table: str,
    source_table: str,
    column_types: Mapping[str, str] | None = None,
    query_label: str | None = None,
) -> str:
    backend = resolve_connection_backend(connection_type)
    return apply_query_label(
        _build_insert_from_table_sql(
            backend,
            target_table,
            source_table,
            column_types,
        ),
        query_label,
    )


def count_table_rows(
    connection_type: str,
    connection: Any,
    table_name: str,
    query_label: str | None = None,
) -> int:
    backend = resolve_connection_backend(connection_type)
    sql = build_count_table_rows_sql(backend, table_name, query_label=query_label)

    if backend in {"gp", "trino"}:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        finally:
            cursor.close()

    if backend == "ch":
        result = connection.query(sql)
        rows = getattr(result, "result_rows", None) or []
        return int(rows[0][0]) if rows else 0

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def build_count_table_rows_sql(
    connection_type: str,
    table_name: str,
    query_label: str | None = None,
) -> str:
    backend = resolve_connection_backend(connection_type)
    if backend == "ch":
        sql = f"SELECT count() FROM {table_name}"
    elif backend in {"gp", "trino"}:
        sql = f"SELECT COUNT(*) FROM {table_name}"
    else:
        raise UnsupportedConnectionTypeError(
            "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
        )
    return apply_query_label(sql, query_label)


def _build_insert_from_table_sql(
    connection_type: str,
    target_table: str,
    source_table: str,
    column_types: Mapping[str, str] | None,
) -> str:
    if not column_types:
        return f"INSERT INTO {target_table} SELECT * FROM {source_table}"

    return _build_typed_insert_select_sql(
        connection_type,
        target_table,
        f"FROM {source_table}",
        column_types,
    )


def _build_typed_insert_select_sql(
    connection_type: str,
    target_table: str,
    from_sql: str,
    column_types: Mapping[str, str],
) -> str:
    columns = list(column_types)
    target_columns = column_list_sql(columns, connection_type)
    select_columns = ", ".join(
        _cast_select_expression(connection_type, column_name, target_type)
        for column_name, target_type in column_types.items()
    )
    return (
        f"INSERT INTO {target_table} ({target_columns}) "
        f"SELECT {select_columns} {from_sql}"
    )


def _cast_select_expression(
    connection_type: str,
    column_name: str,
    target_type: str,
) -> str:
    quoted_column = quote_identifier(column_name, connection_type)
    return f"CAST({quoted_column} AS {target_type}) AS {quoted_column}"


def _extract_row_count(executed: Any) -> int:
    row_count = _coerce_row_count(getattr(executed, "rowcount", None))
    if row_count is not None:
        return row_count

    if isinstance(executed, Mapping):
        row_count = _extract_row_count_from_mapping(executed)
        if row_count is not None:
            return row_count

    summary = getattr(executed, "summary", None)
    if isinstance(summary, Mapping):
        row_count = _extract_row_count_from_mapping(summary)
        if row_count is not None:
            return row_count

    for attribute in ("written_rows", "writtenRows", "processed_rows", "rows"):
        row_count = _coerce_row_count(getattr(executed, attribute, None))
        if row_count is not None:
            return row_count

    return 0


def _extract_row_count_from_mapping(value: Mapping[str, Any]) -> int | None:
    for key in (
        "rowcount",
        "row_count",
        "written_rows",
        "writtenRows",
        "processedRows",
        "rows",
    ):
        row_count = _coerce_row_count(value.get(key))
        if row_count is not None:
            return row_count
    return None


def _coerce_row_count(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        row_count = int(value)
    except (TypeError, ValueError):
        return None
    if row_count < 0:
        return None
    return row_count


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
    query_label: str | None = None,
) -> None:
    _execute_ch_command(
        connection,
        apply_query_label(
            f"TRUNCATE TABLE IF EXISTS {table_name}{_ch_cluster_clause(ch_cluster)}",
            query_label,
        ),
    )


def _ch_cluster_clause(ch_cluster: str | None) -> str:
    if ch_cluster is None:
        return ""
    normalized = ch_cluster.strip()
    if not normalized:
        raise ValueError("ch_cluster must not be empty.")
    return f" ON CLUSTER {_format_ch_cluster_name(normalized)}"


def _format_ch_cluster_name(cluster_name: str) -> str:
    if cluster_name[0] in {"'", '"', "`"}:
        return cluster_name
    if _is_simple_identifier(cluster_name):
        return cluster_name
    return "'" + cluster_name.replace("'", "''") + "'"


def _is_simple_identifier(identifier: str) -> bool:
    if not identifier:
        return False
    if not (identifier[0].isalpha() or identifier[0] == "_"):
        return False
    return all(char.isalnum() or char == "_" for char in identifier)


def _execute_ch_command(connection: Any, sql: str) -> None:
    if "ON CLUSTER" not in sql:
        connection.command(sql)
        return

    try:
        connection.command(
            sql,
            settings={
                "distributed_ddl_task_timeout": 300,
                "distributed_ddl_output_mode": "none",
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
