from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ...backend_adapters import get_backend_adapter
from analytics_toolkit.general import time_print


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
    return get_backend_adapter(connection_type).stage_has_duplicate_keys(
        connection,
        stage_table,
        key_columns,
    )


def _stage_keys_overlap_target(
    connection_type: str,
    connection: Any,
    stage_table: str,
    target_table: str,
    key_columns: Sequence[str],
) -> bool:
    return get_backend_adapter(connection_type).stage_keys_overlap_target(
        connection,
        stage_table,
        target_table,
        key_columns,
    )


def _null_safe_key_equality(
    connection_type: str,
    left_alias: str,
    right_alias: str,
    column_name: str,
) -> str:
    return get_backend_adapter(connection_type).null_safe_key_equality(
        left_alias,
        right_alias,
        column_name,
    )
