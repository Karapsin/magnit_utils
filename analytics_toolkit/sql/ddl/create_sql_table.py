from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from ..connection.errors import UnsupportedConnectionTypeError
from ..general.logging import time_print


def create_sql_table(
    connection_type: str,
    connection: Any,
    table_name: str,
    batch: pd.DataFrame,
    gp_distributed_by_key: list[str] | None = None,
) -> None:
    time_print(f"Creating target table {table_name} on {connection_type}")
    create_sql = build_create_table_sql(
        connection_type,
        table_name,
        batch,
        gp_distributed_by_key=gp_distributed_by_key,
    )

    if connection_type == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(create_sql)
            connection.commit()
            return
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if connection_type == "trino":
        cursor = connection.cursor()
        try:
            cursor.execute(create_sql)
            return
        finally:
            cursor.close()

    if connection_type == "ch":
        connection.command(create_sql)
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def build_create_table_sql(
    connection_type: str,
    table_name: str,
    batch: pd.DataFrame,
    gp_distributed_by_key: list[str] | None = None,
) -> str:
    column_defs = []
    for column_name in batch.columns:
        series = batch[column_name]
        if connection_type == "gp":
            db_type = _infer_gp_type(series)
            column_defs.append(
                f'{quote_identifier(column_name, connection_type)} {db_type}'
            )
        elif connection_type == "trino":
            db_type = _infer_trino_type(series)
            column_defs.append(
                f'{quote_identifier(column_name, connection_type)} {db_type}'
            )
        elif connection_type == "ch":
            db_type = _infer_ch_type(series)
            column_defs.append(
                f"{quote_identifier(column_name, connection_type)} {db_type}"
            )
        else:
            raise UnsupportedConnectionTypeError(
                "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
            )

    joined_columns = ", ".join(column_defs)
    if connection_type == "ch":
        return (
            f"CREATE TABLE {table_name} ({joined_columns}) "
            "ENGINE = MergeTree ORDER BY tuple()"
        )
    if connection_type == "trino":
        return (
            f"CREATE TABLE {table_name} ({joined_columns}) "
            "WITH (format = 'PARQUET', object_store_layout_enabled = true)"
        )
    if connection_type == "gp":
        storage_sql = (
            "WITH (appendoptimized = TRUE, compresstype = zstd, compresslevel = 2)"
        )
        if gp_distributed_by_key:
            distribution_sql = (
                f"DISTRIBUTED BY ({column_list_sql(gp_distributed_by_key, connection_type)})"
            )
        else:
            distribution_sql = "DISTRIBUTED RANDOMLY"
        return f"CREATE TABLE {table_name} ({joined_columns}) {storage_sql} {distribution_sql}"
    return f"CREATE TABLE {table_name} ({joined_columns})"


def column_list_sql(columns: Sequence[str], connection_type: str) -> str:
    return ", ".join(
        quote_identifier(column_name, connection_type) for column_name in columns
    )


def quote_identifier(identifier: str, connection_type: str) -> str:
    if connection_type == "ch":
        escaped = identifier.replace("`", "``")
        return f"`{escaped}`"

    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


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
        if not non_null.empty and all(
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
