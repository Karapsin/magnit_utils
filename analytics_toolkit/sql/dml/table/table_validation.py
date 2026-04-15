from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ...ddl.create_sql_table import column_list_sql, quote_identifier
from ...connection.errors import UnsupportedConnectionTypeError
from ...general.logging import time_print


def normalize_key_columns(key_columns: list[str] | None) -> list[str] | None:
    if key_columns is None:
        return None

    normalized = [column.strip() for column in key_columns]
    if not normalized:
        raise ValueError("key_columns must not be empty when provided.")
    if any(not column for column in normalized):
        raise ValueError("key_columns must not contain empty column names.")
    if len(set(normalized)) != len(normalized):
        raise ValueError("key_columns must not contain duplicate column names.")
    return normalized


def validate_key_columns_in_columns(
    key_columns: list[str] | None,
    columns: Sequence[str],
) -> None:
    if not key_columns:
        return

    available_columns = {str(column) for column in columns}
    missing_columns = [column for column in key_columns if column not in available_columns]
    if missing_columns:
        raise ValueError(
            "key_columns were not found in the staged data: "
            + ", ".join(missing_columns)
        )


def validate_stage_uniqueness(
    connection_type: str,
    connection: Any,
    stage_table: str,
    key_columns: list[str] | None,
) -> None:
    if not key_columns:
        return

    time_print(
        f"Validating uniqueness for stage table {stage_table} using key_columns={key_columns}"
    )
    if _stage_has_duplicate_keys(connection_type, connection, stage_table, key_columns):
        raise ValueError(
            "Duplicate key values found in staged data for key_columns: "
            + ", ".join(key_columns)
        )


def validate_stage_target_key_overlap(
    connection_type: str,
    connection: Any,
    stage_table: str,
    target_table: str,
    key_columns: list[str] | None,
    target_exists: bool,
    replace_target_table: bool,
) -> None:
    if not key_columns or replace_target_table or not target_exists:
        return

    time_print(
        f"Validating staged keys do not already exist in target table {target_table}"
    )
    if _stage_keys_overlap_target(
        connection_type=connection_type,
        connection=connection,
        stage_table=stage_table,
        target_table=target_table,
        key_columns=key_columns,
    ):
        raise ValueError(
            "Staged data contains key values that already exist in target table for key_columns: "
            + ", ".join(key_columns)
        )


def _stage_has_duplicate_keys(
    connection_type: str,
    connection: Any,
    stage_table: str,
    key_columns: Sequence[str],
) -> bool:
    key_sql = column_list_sql(key_columns, connection_type)
    query = (
        f"SELECT 1 FROM {stage_table} "
        f"GROUP BY {key_sql} "
        "HAVING COUNT(*) > 1 "
        "LIMIT 1"
    )

    if connection_type in {"gp", "trino"}:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchone() is not None
        finally:
            cursor.close()

    if connection_type == "ch":
        result = connection.query(query)
        return bool(result.result_rows)

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def _stage_keys_overlap_target(
    connection_type: str,
    connection: Any,
    stage_table: str,
    target_table: str,
    key_columns: Sequence[str],
) -> bool:
    join_condition = " AND ".join(
        _null_safe_key_equality(connection_type, "stage_src", "target_dst", column_name)
        for column_name in key_columns
    )
    query = (
        "SELECT 1 "
        f"FROM {stage_table} AS stage_src "
        f"INNER JOIN {target_table} AS target_dst ON {join_condition} "
        "LIMIT 1"
    )

    if connection_type in {"gp", "trino"}:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchone() is not None
        finally:
            cursor.close()

    if connection_type == "ch":
        result = connection.query(query)
        return bool(result.result_rows)

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def _null_safe_key_equality(
    connection_type: str,
    left_alias: str,
    right_alias: str,
    column_name: str,
) -> str:
    quoted_column = quote_identifier(column_name, connection_type)
    left_expr = f"{left_alias}.{quoted_column}"
    right_expr = f"{right_alias}.{quoted_column}"
    return (
        f"({left_expr} = {right_expr} "
        f"OR ({left_expr} IS NULL AND {right_expr} IS NULL))"
    )
