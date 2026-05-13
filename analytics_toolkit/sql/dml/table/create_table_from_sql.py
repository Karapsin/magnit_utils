from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd
import sqlparse

from ...connection.config import get_connection_config
from ...connection.errors import InvalidSqlInputError
from ...connection.get_sql_connection import get_sql_connection
from ...ddl.create_sql_table import create_sql_table
from ..transfer.schema import inspect_source_query_schema, map_source_schema_to_target
from .table_ops import (
    drop_ch_distributed_table_pair,
    drop_table,
    insert_from_query,
)
from .table_validation import normalize_key_columns, validate_key_columns_in_columns
from analytics_toolkit.general import time_print


def transfer_table(**kwargs: Any) -> int:
    from ..transfer.flow.api import transfer_table as _transfer_table

    return _transfer_table(**kwargs)


def create_table_from_sql(
    source_db: str,
    table_name: str,
    sql: str,
    *,
    table_db: str | None = None,
    insert_data: bool = False,
    drop_target_if_exists: bool = False,
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    sharding_key: str = "rand()",
    trino_insert_chunk_size: int | None = None,
) -> int | None:
    target_table = _normalize_table_name(table_name)
    source_sql = _normalize_single_query(sql)
    source_config = get_connection_config(source_db)
    target_config = (
        source_config
        if table_db is None
        else get_connection_config(table_db)
    )
    gp_distribution = normalize_key_columns(gp_distributed_by_key)
    ch_partition = _normalize_ch_columns_or_expression(
        ch_partition_by,
        "ch_partition_by",
    )
    ch_order = _normalize_ch_columns_or_expression(ch_order_by, "ch_order_by")
    ch_engine_name = _normalize_ch_string(ch_engine, "ch_engine")
    ch_cluster_name = _normalize_ch_string(ch_cluster, "ch_cluster")
    ch_sharding_key = _normalize_ch_string(sharding_key, "sharding_key")

    _validate_backend_options(
        target_backend=target_config.backend,
        gp_distributed_by_key=gp_distribution,
        ch_partition_by=ch_partition,
        ch_order_by=ch_order,
        ch_engine=ch_engine_name,
        ch_cluster=ch_cluster_name,
        ch_sharding_key=ch_sharding_key,
    )
    if trino_insert_chunk_size is not None and trino_insert_chunk_size <= 0:
        raise ValueError("trino_insert_chunk_size must be a positive integer.")

    source_connection: Any | None = None
    target_connection: Any | None = None
    inserted_rows: int | None = None
    delegate_transfer = False

    try:
        source_connection = get_sql_connection(source_config.connection_key)
        target_connection = (
            source_connection
            if source_config.connection_key == target_config.connection_key
            else get_sql_connection(target_config.connection_key)
        )

        time_print(
            f"Inspecting source query schema on {source_config.connection_key}"
        )
        source_schema = inspect_source_query_schema(
            source_config.backend,
            source_connection,
            source_sql,
        )
        source_columns = [column.name for column in source_schema]
        _validate_source_columns(source_columns)
        validate_key_columns_in_columns(gp_distribution, source_columns)
        _validate_ch_columns_in_columns(ch_partition, source_columns, "ch_partition_by")
        _validate_ch_columns_in_columns(ch_order, source_columns, "ch_order_by")

        target_column_types = map_source_schema_to_target(
            source_schema,
            target_config.backend,
        )
        schema_batch = pd.DataFrame(columns=source_columns)

        if drop_target_if_exists:
            if target_config.backend == "ch":
                time_print(
                    "Dropping existing ClickHouse distributed table pair "
                    f"{target_table}"
                )
                drop_ch_distributed_table_pair(
                    target_connection,
                    target_table,
                    ch_cluster=ch_cluster_name,
                )
            else:
                time_print(
                    f"Dropping existing table {target_table} "
                    f"on {target_config.connection_key}"
                )
                drop_table(
                    target_config.backend,
                    target_connection,
                    target_table,
                )

        create_sql_table(
            target_config.backend,
            target_connection,
            target_table,
            schema_batch,
            column_types=target_column_types,
            gp_distributed_by_key=gp_distribution,
            ch_partition_by=ch_partition,
            ch_order_by=ch_order,
            ch_engine=ch_engine_name,
            ch_cluster=ch_cluster_name,
            ch_sharding_key=ch_sharding_key,
            ch_distributed_table=target_config.backend == "ch",
        )

        if not insert_data:
            return None

        if source_config.backend == target_config.backend:
            inserted_rows = insert_from_query(
                target_config.backend,
                target_connection,
                target_table,
                source_sql,
                target_column_types,
            )
        else:
            delegate_transfer = True
    finally:
        _close_connections(
            source_connection=source_connection,
            source_key=source_config.connection_key,
            target_connection=target_connection,
            target_key=target_config.connection_key,
        )

    if delegate_transfer:
        return transfer_table(
            from_db=source_config.connection_key,
            to_db=target_config.connection_key,
            from_sql=source_sql,
            to_table=target_table,
            replace_target_table=False,
            gp_distributed_by_key=gp_distribution,
            trino_insert_chunk_size=trino_insert_chunk_size,
            ch_partition_by=ch_partition,
            ch_order_by=ch_order,
            ch_engine=ch_engine_name,
            ch_cluster=ch_cluster_name,
            sharding_key=ch_sharding_key,
        )
    return inserted_rows


def _normalize_table_name(table_name: str) -> str:
    normalized = table_name.strip()
    if not normalized:
        raise InvalidSqlInputError("table_name must not be empty.")
    return normalized


def _normalize_single_query(query: str) -> str:
    normalized = query.strip()
    if not normalized:
        raise InvalidSqlInputError("sql must not be empty.")

    statements = [
        statement.strip().rstrip(";").rstrip()
        for statement in sqlparse.split(normalized)
        if statement.strip()
    ]
    if len(statements) != 1:
        raise InvalidSqlInputError(
            "create_table_from_sql expects exactly one SQL statement."
        )
    return statements[0]


def _validate_source_columns(columns: Sequence[str]) -> None:
    if not columns:
        raise ValueError("sql must return at least one column.")
    duplicates = [column for column in columns if columns.count(column) > 1]
    if duplicates:
        duplicated_columns = ", ".join(dict.fromkeys(duplicates))
        raise ValueError(f"sql must not return duplicate columns: {duplicated_columns}")


def _validate_backend_options(
    *,
    target_backend: str,
    gp_distributed_by_key: list[str] | None,
    ch_partition_by: list[str] | str | None,
    ch_order_by: list[str] | str | None,
    ch_engine: str,
    ch_cluster: str,
    ch_sharding_key: str,
) -> None:
    if gp_distributed_by_key and target_backend != "gp":
        raise ValueError(
            "gp_distributed_by_key can only be used when table_db has type 'gp'."
        )
    if target_backend != "ch":
        if ch_partition_by is not None:
            raise ValueError(
                "ch_partition_by can only be used when table_db has type 'ch'."
            )
        if ch_order_by is not None:
            raise ValueError(
                "ch_order_by can only be used when table_db has type 'ch'."
            )
        if ch_engine != "ReplicatedMergeTree":
            raise ValueError("ch_engine can only be used when table_db has type 'ch'.")
        if ch_cluster != "{cluster}":
            raise ValueError("ch_cluster can only be used when table_db has type 'ch'.")
        if ch_sharding_key != "rand()":
            raise ValueError(
                "sharding_key can only be used when table_db has type 'ch'."
            )


def _normalize_ch_columns_or_expression(
    value: Sequence[str] | str | None,
    option_name: str,
) -> list[str] | str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return _normalize_ch_string(value, option_name)

    normalized = [_normalize_ch_string(column, option_name) for column in value]
    if not normalized:
        raise ValueError(f"{option_name} must not be empty when provided.")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{option_name} must not contain duplicate column names.")
    return normalized


def _normalize_ch_string(value: str, option_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{option_name} must not be empty.")
    return normalized


def _validate_ch_columns_in_columns(
    value: list[str] | str | None,
    columns: Sequence[str],
    option_name: str,
) -> None:
    if value is None or isinstance(value, str):
        return

    available_columns = {str(column) for column in columns}
    missing_columns = [column for column in value if column not in available_columns]
    if missing_columns:
        raise ValueError(
            f"{option_name} columns were not found in the source query: "
            + ", ".join(missing_columns)
        )


def _close_connections(
    *,
    source_connection: Any | None,
    source_key: str,
    target_connection: Any | None,
    target_key: str,
) -> None:
    if target_connection is not None and target_connection is not source_connection:
        time_print(f"Closing {target_key} connection")
        target_connection.close()
    if source_connection is not None:
        time_print(f"Closing {source_key} connection")
        source_connection.close()
