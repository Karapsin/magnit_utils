from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

import pandas as pd
from sqlglot import exp, parse_one

from ..backend_adapters import get_backend_adapter
from ..capabilities import get_backend_capability
from ..connection.config import resolve_connection_backend
from ..connection.errors import UnsupportedConnectionTypeError
from ..labels import apply_query_label
from ..operation_runner import tracked_sql_operation
from ..plans import SqlOperationMetadata, SqlOperationResult, SqlPlan
from analytics_toolkit.general import time_print
from .models import CreateSqlTableOptions


def create_sql_table(
    connection_type: str,
    connection: Any,
    table_name: str,
    batch: pd.DataFrame,
    column_types: Mapping[str, str] | None = None,
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    ch_distributed_table: bool = False,
    dry_run: bool = False,
    return_sql: bool = False,
    query_label: str | None = None,
    return_metadata: bool = False,
) -> SqlPlan | SqlOperationResult | None:
    backend = resolve_connection_backend(connection_type)
    options = CreateSqlTableOptions(
        connection_type=connection_type,
        backend=backend,
        connection=connection,
        table_name=table_name,
        batch=batch,
        column_types=column_types,
        gp_distributed_by_key=gp_distributed_by_key,
        ch_partition_by=ch_partition_by,
        ch_order_by=ch_order_by,
        ch_engine=ch_engine,
        ch_cluster=ch_cluster,
        ch_sharding_key=ch_sharding_key,
        ch_distributed_table=ch_distributed_table,
        dry_run=dry_run,
        return_sql=return_sql,
        query_label=query_label,
        return_metadata=return_metadata,
    )
    time_print(f"Creating target table {table_name} on {connection_type}")
    create_sqls = build_create_table_sqls(
        options.backend,
        options.table_name,
        options.batch,
        column_types=options.column_types,
        gp_distributed_by_key=options.gp_distributed_by_key,
        ch_partition_by=options.ch_partition_by,
        ch_order_by=options.ch_order_by,
        ch_engine=options.ch_engine,
        ch_cluster=options.ch_cluster,
        ch_sharding_key=options.ch_sharding_key,
        ch_distributed_table=options.ch_distributed_table,
        query_label=options.query_label,
    )
    metadata = SqlOperationMetadata(
        statement_count=len(create_sqls),
        query_label=options.query_label,
    )
    plan = SqlPlan(
        operation="create_table",
        target_alias=options.connection_type,
        target_backend=options.backend,
        target_table=options.table_name,
        metadata=metadata,
    )
    plan.extend(
        create_sqls,
        alias=options.connection_type,
        backend=options.backend,
        phase="create_table",
        target_table=options.table_name,
    )

    if options.dry_run or options.return_sql:
        return plan

    with tracked_sql_operation(
        metadata=metadata,
        operation_name="create_sql_table",
        alias=options.connection_type,
        backend=options.backend,
        phase="create_target",
        query_label=options.query_label,
    ):
        get_backend_adapter(options.backend).execute_commands(options.connection, create_sqls)
        if options.backend == "ch":
            if options.ch_distributed_table:
                _wait_for_ch_table(options.connection, options.table_name)
    if options.return_metadata:
        return SqlOperationResult(rows=None, metadata=metadata, plan=plan)
    return None


def build_create_table_sql(
    connection_type: str,
    table_name: str,
    batch: pd.DataFrame,
    column_types: Mapping[str, str] | None = None,
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    ch_distributed_table: bool = False,
    query_label: str | None = None,
) -> str:
    return ";\n".join(
        build_create_table_sqls(
            connection_type,
            table_name,
            batch,
            column_types=column_types,
            gp_distributed_by_key=gp_distributed_by_key,
            ch_partition_by=ch_partition_by,
            ch_order_by=ch_order_by,
            ch_engine=ch_engine,
            ch_cluster=ch_cluster,
            ch_sharding_key=ch_sharding_key,
            ch_distributed_table=ch_distributed_table,
            query_label=query_label,
        )
    )


def build_create_table_sqls(
    connection_type: str,
    table_name: str,
    batch: pd.DataFrame,
    column_types: Mapping[str, str] | None = None,
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    ch_distributed_table: bool = False,
    query_label: str | None = None,
) -> list[str]:
    backend = resolve_connection_backend(connection_type)
    joined_columns = _build_column_definitions(backend, batch, column_types)
    return _apply_query_label_to_sqls(
        _build_backend_create_table_sqls(
            backend=backend,
            table_name=table_name,
            joined_columns=joined_columns,
            gp_distributed_by_key=gp_distributed_by_key,
            ch_partition_by=ch_partition_by,
            ch_order_by=ch_order_by,
            ch_engine=ch_engine,
            ch_cluster=ch_cluster,
            ch_sharding_key=ch_sharding_key,
            ch_distributed_table=ch_distributed_table,
        ),
        query_label,
    )


def _build_column_definitions(
    backend: str,
    batch: pd.DataFrame,
    column_types: Mapping[str, str] | None,
) -> str:
    column_defs = []
    for column_name in batch.columns:
        db_type = (
            _explicit_column_type(column_types, column_name)
            if column_types is not None
            else _infer_backend_type(backend, batch[column_name])
        )
        column_defs.append(f"{quote_identifier(column_name, backend)} {db_type}")
    return ", ".join(column_defs)


def _infer_backend_type(backend: str, series: pd.Series) -> str:
    try:
        infer_type = _COLUMN_TYPE_INFERERS[backend]
    except KeyError as exc:
        raise UnsupportedConnectionTypeError(
            "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
        ) from exc
    return infer_type(series)


def _build_backend_create_table_sqls(
    *,
    backend: str,
    table_name: str,
    joined_columns: str,
    gp_distributed_by_key: list[str] | None,
    ch_partition_by: Sequence[str] | str | None,
    ch_order_by: Sequence[str] | str | None,
    ch_engine: str,
    ch_cluster: str,
    ch_sharding_key: str,
    ch_distributed_table: bool,
) -> list[str]:
    try:
        build_sqls = _CREATE_TABLE_SQL_BUILDERS[backend]
    except KeyError as exc:
        raise UnsupportedConnectionTypeError(
            "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
        ) from exc
    return build_sqls(
        table_name=table_name,
        joined_columns=joined_columns,
        gp_distributed_by_key=gp_distributed_by_key,
        ch_partition_by=ch_partition_by,
        ch_order_by=ch_order_by,
        ch_engine=ch_engine,
        ch_cluster=ch_cluster,
        ch_sharding_key=ch_sharding_key,
        ch_distributed_table=ch_distributed_table,
    )


def _build_gp_create_table_sqls(
    *,
    table_name: str,
    joined_columns: str,
    gp_distributed_by_key: list[str] | None,
    **_: object,
) -> list[str]:
    storage_sql = (
        "WITH (appendonly=true,\n"
        "        blocksize=32768,\n"
        "        compresstype=zstd,\n"
        "        compresslevel=4,\n"
        "        orientation=column)"
    )
    if gp_distributed_by_key:
        distribution_sql = (
            f"DISTRIBUTED BY ({column_list_sql(gp_distributed_by_key, 'gp')})"
        )
    else:
        distribution_sql = "DISTRIBUTED RANDOMLY"
    return [
        f"CREATE TABLE {table_name} ({joined_columns}) "
        f"{storage_sql} {distribution_sql}"
    ]


def _build_trino_create_table_sqls(
    *,
    table_name: str,
    joined_columns: str,
    **_: object,
) -> list[str]:
    return [
        f"CREATE TABLE {table_name} ({joined_columns}) "
        "WITH (format = 'PARQUET', object_store_layout_enabled = true)"
    ]


def _build_ch_create_table_sqls(
    *,
    table_name: str,
    joined_columns: str,
    ch_partition_by: Sequence[str] | str | None,
    ch_order_by: Sequence[str] | str | None,
    ch_engine: str,
    ch_cluster: str,
    ch_sharding_key: str,
    ch_distributed_table: bool,
    **_: object,
) -> list[str]:
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


def _apply_query_label_to_sqls(sqls: list[str], query_label: str | None) -> list[str]:
    return [apply_query_label(sql, query_label) for sql in sqls]


def _explicit_column_type(
    column_types: Mapping[str, str],
    column_name: str,
) -> str:
    try:
        db_type = column_types[column_name]
    except KeyError as exc:
        raise ValueError(f"Missing explicit SQL type for column {column_name!r}.") from exc
    normalized = db_type.strip()
    if not normalized:
        raise ValueError(f"SQL type for column {column_name!r} must not be empty.")
    return normalized


def build_ch_distributed_create_table_sqls(
    table_name: str,
    joined_columns: str,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
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
        f"ON CLUSTER {_format_ch_cluster_name(cluster_name)}\n"
        f"({joined_columns})\n"
        f"ENGINE = {engine}\n"
        f"{partition_sql}"
        f"{order_by_sql}"
    )
    distributed_sql = (
        f"CREATE TABLE IF NOT EXISTS {table_name}\n"
        f"ON CLUSTER {_format_ch_cluster_name(cluster_name)}\n"
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
    quote_char = get_backend_capability(backend).identifier_quote
    escaped = identifier.replace(quote_char, quote_char * 2)
    return f"{quote_char}{escaped}{quote_char}"


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


def _format_ch_cluster_name(cluster_name: str) -> str:
    normalized = cluster_name.strip()
    if not normalized:
        return normalized
    if normalized[0] in {"'", '"', "`"}:
        return normalized
    if _is_simple_identifier(normalized):
        return normalized
    return _sql_string_literal(normalized)


def _is_simple_identifier(identifier: str) -> bool:
    if not identifier:
        return False
    if not (identifier[0].isalpha() or identifier[0] == "_"):
        return False
    return all(char.isalnum() or char == "_" for char in identifier)


def _execute_ch_command(connection: Any, sql: str) -> None:
    get_backend_adapter("ch").execute_command(connection, sql)


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


_COLUMN_TYPE_INFERERS = {
    "gp": _infer_gp_type,
    "trino": _infer_trino_type,
    "ch": _infer_ch_type,
}


_CREATE_TABLE_SQL_BUILDERS = {
    "gp": _build_gp_create_table_sqls,
    "trino": _build_trino_create_table_sqls,
    "ch": _build_ch_create_table_sqls,
}
