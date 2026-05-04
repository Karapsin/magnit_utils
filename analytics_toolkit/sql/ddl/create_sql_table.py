from __future__ import annotations

import time
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

import pandas as pd
from sqlglot import exp, parse_one

from ..connection.config import resolve_connection_backend
from ..connection.errors import UnsupportedConnectionTypeError
from analytics_toolkit.general import time_print


def create_sql_table(
    connection_type: str,
    connection: Any,
    table_name: str,
    batch: pd.DataFrame,
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "core",
    ch_sharding_key: str = "rand()",
    ch_distributed_table: bool = False,
) -> None:
    backend = resolve_connection_backend(connection_type)
    time_print(f"Creating target table {table_name} on {connection_type}")
    create_sqls = build_create_table_sqls(
        backend,
        table_name,
        batch,
        gp_distributed_by_key=gp_distributed_by_key,
        ch_partition_by=ch_partition_by,
        ch_order_by=ch_order_by,
        ch_engine=ch_engine,
        ch_cluster=ch_cluster,
        ch_sharding_key=ch_sharding_key,
        ch_distributed_table=ch_distributed_table,
    )

    if backend == "gp":
        cursor = connection.cursor()
        try:
            for create_sql in create_sqls:
                cursor.execute(create_sql)
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
            for create_sql in create_sqls:
                cursor.execute(create_sql)
            return
        finally:
            cursor.close()

    if backend == "ch":
        for create_sql in create_sqls:
            _execute_ch_command(connection, create_sql)
        if ch_distributed_table:
            _wait_for_ch_table(connection, table_name)
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def build_create_table_sql(
    connection_type: str,
    table_name: str,
    batch: pd.DataFrame,
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "core",
    ch_sharding_key: str = "rand()",
    ch_distributed_table: bool = False,
) -> str:
    return ";\n".join(
        build_create_table_sqls(
            connection_type,
            table_name,
            batch,
            gp_distributed_by_key=gp_distributed_by_key,
            ch_partition_by=ch_partition_by,
            ch_order_by=ch_order_by,
            ch_engine=ch_engine,
            ch_cluster=ch_cluster,
            ch_sharding_key=ch_sharding_key,
            ch_distributed_table=ch_distributed_table,
        )
    )


def build_create_table_sqls(
    connection_type: str,
    table_name: str,
    batch: pd.DataFrame,
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "core",
    ch_sharding_key: str = "rand()",
    ch_distributed_table: bool = False,
) -> list[str]:
    backend = resolve_connection_backend(connection_type)
    column_defs = []
    for column_name in batch.columns:
        series = batch[column_name]
        if backend == "gp":
            db_type = _infer_gp_type(series)
            column_defs.append(
                f'{quote_identifier(column_name, backend)} {db_type}'
            )
        elif backend == "trino":
            db_type = _infer_trino_type(series)
            column_defs.append(
                f'{quote_identifier(column_name, backend)} {db_type}'
            )
        elif backend == "ch":
            db_type = _infer_ch_type(series)
            column_defs.append(
                f"{quote_identifier(column_name, backend)} {db_type}"
            )
        else:
            raise UnsupportedConnectionTypeError(
                "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
            )

    joined_columns = ", ".join(column_defs)
    if backend == "ch":
        if ch_distributed_table:
            return build_ch_distributed_create_table_sqls(
                table_name=table_name,
                joined_columns=joined_columns,
                ch_partition_by=ch_partition_by,
                ch_order_by=ch_order_by,
                ch_engine=ch_engine,
                ch_cluster=ch_cluster,
                ch_sharding_key=ch_sharding_key,
            )
        return [
            f"CREATE TABLE {table_name} ({joined_columns}) "
            "ENGINE = MergeTree ORDER BY tuple()"
        ]
    if backend == "trino":
        return [
            f"CREATE TABLE {table_name} ({joined_columns}) "
            "WITH (format = 'PARQUET', object_store_layout_enabled = true)"
        ]
    if backend == "gp":
        storage_sql = (
            "WITH (appendoptimized = TRUE, compresstype = zstd, compresslevel = 2)"
        )
        if gp_distributed_by_key:
            distribution_sql = (
                f"DISTRIBUTED BY ({column_list_sql(gp_distributed_by_key, backend)})"
            )
        else:
            distribution_sql = "DISTRIBUTED RANDOMLY"
        return [
            f"CREATE TABLE {table_name} ({joined_columns}) {storage_sql} {distribution_sql}"
        ]
    return [f"CREATE TABLE {table_name} ({joined_columns})"]


def build_ch_distributed_create_table_sqls(
    table_name: str,
    joined_columns: str,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "core",
    ch_sharding_key: str = "rand()",
) -> list[str]:
    shard_table = build_ch_shard_table_name(table_name)
    cluster_name = _normalize_non_empty_string(ch_cluster, "ch_cluster")
    engine = _normalize_non_empty_string(ch_engine, "ch_engine")
    sharding_key = _normalize_non_empty_string(ch_sharding_key, "ch_sharding_key")
    partition_sql = _build_ch_partition_by_sql(ch_partition_by)
    order_by_sql = _build_ch_order_by_sql(ch_order_by)
    database_name, shard_relation_name = split_ch_table_name_for_distributed_engine(
        shard_table
    )

    shard_sql = (
        f"CREATE TABLE IF NOT EXISTS {shard_table}\n"
        f"ON CLUSTER {cluster_name}\n"
        f"({joined_columns})\n"
        f"ENGINE = {engine}\n"
        f"{partition_sql}"
        f"{order_by_sql}"
    )
    distributed_sql = (
        f"CREATE TABLE IF NOT EXISTS {table_name}\n"
        f"ON CLUSTER {cluster_name}\n"
        f"({joined_columns})\n"
        "ENGINE = Distributed(\n"
        f"    {_sql_string_literal(cluster_name)},\n"
        f"    {database_name},\n"
        f"    {_sql_string_literal(shard_relation_name)},\n"
        f"    {sharding_key}\n"
        ")"
    )
    local_distributed_sql = (
        f"CREATE TABLE IF NOT EXISTS {table_name}\n"
        f"({joined_columns})\n"
        "ENGINE = Distributed(\n"
        f"    {_sql_string_literal(cluster_name)},\n"
        f"    {database_name},\n"
        f"    {_sql_string_literal(shard_relation_name)},\n"
        f"    {sharding_key}\n"
        ")"
    )
    return [shard_sql, distributed_sql, local_distributed_sql]


def build_ch_shard_table_name(table_name: str) -> str:
    return _add_table_identifier_suffix(table_name, "_shard", "clickhouse")


def split_ch_table_name_for_distributed_engine(table_name: str) -> tuple[str, str]:
    table = _parse_table_name(table_name, "clickhouse")
    relation_name = _identifier_name(table.this)
    database = table.args.get("db")
    if database is None:
        return "currentDatabase()", relation_name
    return _sql_string_literal(_identifier_name(database)), relation_name


def column_list_sql(columns: Sequence[str], connection_type: str) -> str:
    backend = resolve_connection_backend(connection_type)
    return ", ".join(
        quote_identifier(column_name, backend) for column_name in columns
    )


def quote_identifier(identifier: str, connection_type: str) -> str:
    backend = resolve_connection_backend(connection_type)
    if backend == "ch":
        escaped = identifier.replace("`", "``")
        return f"`{escaped}`"

    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _add_table_identifier_suffix(table_name: str, suffix: str, dialect: str) -> str:
    table = _parse_table_name(table_name, dialect)
    table_identifier = table.this
    suffixed_identifier = exp.to_identifier(
        f"{_identifier_name(table_identifier)}{suffix}",
        quoted=bool(table_identifier.args.get("quoted")),
    )
    suffixed_table = table.copy()
    suffixed_table.set("this", suffixed_identifier)
    return suffixed_table.sql(dialect=dialect)


def _parse_table_name(table_name: str, dialect: str) -> exp.Table:
    table = parse_one(table_name, read=dialect, into=exp.Table)
    if not isinstance(table, exp.Table) or not isinstance(table.this, exp.Identifier):
        raise ValueError(f"Invalid table name: {table_name}")
    return table


def _identifier_name(identifier: exp.Expression) -> str:
    if not isinstance(identifier, exp.Identifier):
        raise ValueError(f"Invalid table identifier: {identifier}")
    return str(identifier.this)


def _build_ch_partition_by_sql(
    ch_partition_by: Sequence[str] | str | None,
) -> str:
    if ch_partition_by is None:
        return ""
    expression = _normalize_ch_expression(ch_partition_by, "ch_partition_by")
    return f"PARTITION BY {expression}\n"


def _build_ch_order_by_sql(ch_order_by: Sequence[str] | str | None) -> str:
    expression = (
        "tuple()"
        if ch_order_by is None
        else _normalize_ch_expression(ch_order_by, "ch_order_by")
    )
    return f"ORDER BY {expression}"


def _normalize_ch_expression(value: Sequence[str] | str, option_name: str) -> str:
    if isinstance(value, str):
        return _normalize_non_empty_string(value, option_name)

    columns = [_normalize_non_empty_string(column, option_name) for column in value]
    if not columns:
        raise ValueError(f"{option_name} must not be empty when provided.")
    if len(set(columns)) != len(columns):
        raise ValueError(f"{option_name} must not contain duplicate column names.")
    quoted_columns = [quote_identifier(column, "ch") for column in columns]
    if len(quoted_columns) == 1:
        return quoted_columns[0]
    return f"({', '.join(quoted_columns)})"


def _normalize_non_empty_string(value: str, option_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{option_name} must not be empty.")
    return normalized


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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


def _wait_for_ch_table(
    connection: Any,
    table_name: str,
    timeout_seconds: int = 60,
    poll_interval_seconds: float = 1,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        result = connection.query(f"EXISTS TABLE {table_name}")
        if result.result_rows and result.result_rows[0][0]:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"ClickHouse table {table_name} was not visible after "
                f"{timeout_seconds} second(s)."
            )
        time.sleep(poll_interval_seconds)


def _infer_gp_type(series: pd.Series) -> str:
    return _infer_common_sql_type(series)


def _infer_trino_type(series: pd.Series) -> str:
    common_type = _infer_common_sql_type(series)
    if common_type == "DOUBLE PRECISION":
        return "DOUBLE"
    if common_type == "TEXT":
        return "VARCHAR"
    return common_type


def _infer_ch_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        base_type = "Bool"
    elif pd.api.types.is_integer_dtype(series):
        base_type = "Int64"
    elif pd.api.types.is_float_dtype(series):
        base_type = "Float64"
    elif pd.api.types.is_datetime64_any_dtype(series):
        base_type = "DateTime64(6)"
    else:
        non_null = series.dropna()
        if not non_null.empty and all(isinstance(value, Decimal) for value in non_null):
            base_type = "Float64"
        elif not non_null.empty and all(
            hasattr(value, "year")
            and hasattr(value, "month")
            and hasattr(value, "day")
            for value in non_null
        ):
            base_type = "Date"
        else:
            base_type = "String"

    if series.isna().any():
        return f"Nullable({base_type})"
    return base_type


def _infer_common_sql_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"

    non_null = series.dropna()
    if not non_null.empty and all(
        hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day")
        for value in non_null
    ):
        return "DATE"
    return "TEXT"
