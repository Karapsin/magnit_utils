from __future__ import annotations

from collections.abc import Iterator, Sequence
from decimal import Decimal
from itertools import islice
from typing import Any

import pandas as pd
from psycopg2.extras import execute_values

from ...ddl.create_sql_table import column_list_sql
from ...connection.config import (
    TrinoConfig,
    get_connection_config,
    resolve_connection_backend,
)
from ...connection.errors import SqlConfigError, UnsupportedConnectionTypeError
from analytics_toolkit.general import time_print


class AmbiguousTableLoadError(Exception):
    pass


DEFAULT_TRINO_INSERT_CHUNK_SIZE = 1000


def insert_table_batch(
    connection_type: str,
    connection_ref: dict[str, Any],
    table_name: str,
    batch: pd.DataFrame,
    retry_fn: Any,
    retry_cnt: int,
    timeout_increment: int | float,
    target_column_types: dict[str, str] | None = None,
    trino_insert_chunk_size: int | None = None,
) -> int:
    backend = resolve_connection_backend(connection_type)
    normalized_batch = normalize_batch(batch) if backend != "trino" else batch

    def operation(attempt: int) -> int:
        connection = connection_ref["connection"]
        try:
            if backend == "gp":
                _insert_gp_batch(connection, table_name, normalized_batch)
                return len(normalized_batch)
            if backend == "trino":
                _insert_trino_batch(
                    connection,
                    table_name,
                    normalized_batch,
                    target_column_types=target_column_types,
                    trino_insert_chunk_size=trino_insert_chunk_size,
                    connection_type=connection_type,
                )
                return len(normalized_batch)
            if backend == "ch":
                _insert_ch_batch(connection, table_name, normalized_batch)
                return len(normalized_batch)
        except Exception as exc:
            if backend == "gp":
                if getattr(connection, "closed", 0):
                    raise
            else:
                time_print(
                    f"Stage insert on {connection_type} failed for {table_name}; "
                    "the current stage table will be discarded and reloaded from scratch."
                )
                time_print(
                    f"Original {connection_type} insert error for {table_name}: "
                    f"{type(exc).__name__}: {exc!r}"
                )
                raise AmbiguousTableLoadError(
                    f"Ambiguous stage insert outcome on {connection_type} for {table_name}"
                ) from exc
            raise

        raise UnsupportedConnectionTypeError(
            "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
        )

    return retry_fn(
        operation_name=f"inserting batch into stage table {table_name} on {connection_type}",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )


def normalize_batch(batch: pd.DataFrame) -> pd.DataFrame:
    normalized = batch.copy()
    for column_name in normalized.columns:
        series = normalized[column_name]
        normalized[column_name] = series.astype(object).where(series.notna(), None)
    return normalized


def _insert_gp_batch(connection: Any, table_name: str, batch: pd.DataFrame) -> None:
    column_list = column_list_sql(batch.columns, "gp")
    rows = list(batch.itertuples(index=False, name=None))
    sql = f"INSERT INTO {table_name} ({column_list}) VALUES %s"

    cursor = connection.cursor()
    try:
        execute_values(cursor, sql, rows, page_size=len(rows))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def _insert_trino_batch(
    connection: Any,
    table_name: str,
    batch: pd.DataFrame,
    target_column_types: dict[str, str] | None = None,
    trino_insert_chunk_size: int | None = None,
    connection_type: str = "trino",
) -> None:
    column_list = column_list_sql(batch.columns, "trino")
    column_count = len(batch.columns)
    row_placeholders = f"({', '.join('?' for _ in range(column_count))})"
    chunk_size = _get_trino_insert_chunk_size(
        trino_insert_chunk_size,
        connection_type,
    )
    cursor = connection.cursor()
    try:
        row_iterator = _iter_trino_rows(batch, target_column_types)
        for row_chunk in _chunk_rows(row_iterator, chunk_size):
            values_sql = ", ".join(row_placeholders for _ in row_chunk)
            params = [value for row in row_chunk for value in row]
            sql = f"INSERT INTO {table_name} ({column_list}) VALUES {values_sql}"
            time_print(f"Writing {len(row_chunk)} row(s) to trino table {table_name}")
            cursor.execute(sql, params)
    finally:
        cursor.close()


def _insert_ch_batch(client: Any, table_name: str, batch: pd.DataFrame) -> None:
    normalized_batch = normalize_ch_batch(batch)
    client.insert_df(
        table=table_name,
        df=normalized_batch,
        column_names=list(batch.columns),
    )


def normalize_ch_batch(batch: pd.DataFrame) -> pd.DataFrame:
    normalized = batch.map(_normalize_ch_scalar)
    for column_name in normalized.columns:
        series = normalized[column_name]
        normalized[column_name] = series.astype(object).where(series.notna(), None)
    return normalized


def _normalize_ch_scalar(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [_normalize_ch_scalar(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_ch_scalar(item) for item in value)
    if isinstance(value, dict):
        return {
            _normalize_ch_scalar(key): _normalize_ch_scalar(item)
            for key, item in value.items()
        }
    return value


def _iter_trino_rows(
    batch: pd.DataFrame,
    target_column_types: dict[str, str] | None,
) -> Iterator[tuple[Any, ...]]:
    for row in batch.itertuples(index=False, name=None):
        normalized_values = []
        for column_name, value in zip(batch.columns, row, strict=True):
            target_type = (
                target_column_types.get(column_name)
                if target_column_types is not None
                else None
            )
            normalized_values.append(_normalize_trino_value(value, target_type))
        yield tuple(normalized_values)


def _normalize_trino_value(value: Any, target_type: str | None) -> Any:
    if _is_null_like(value):
        return None

    if value is None:
        return None

    normalized_target_type = (target_type or "").lower()
    if normalized_target_type.startswith(("varchar", "char", "string")):
        return str(value)
    if normalized_target_type == "bigint":
        return int(value)
    return value


def _build_trino_values_tuple(
    columns: Sequence[str],
    row: Sequence[Any],
    target_column_types: dict[str, str] | None,
) -> str:
    values_sql = []
    for column_name, value in zip(columns, row, strict=True):
        target_type = (
            target_column_types.get(column_name)
            if target_column_types is not None
            else None
        )
        values_sql.append(_trino_literal(value, target_type))
    return f"({', '.join(values_sql)})"


def _chunk_rows(
    rows: Iterator[tuple[Any, ...]],
    chunk_size: int,
) -> Iterator[list[tuple[Any, ...]]]:
    while True:
        chunk = list(islice(rows, chunk_size))
        if not chunk:
            return
        yield chunk


def _trino_literal(value: Any, target_type: str | None) -> str:
    if value is None:
        return "NULL"

    normalized_target_type = (target_type or "").lower()

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return "NULL"
        timestamp_value = value.to_pydatetime()
        if normalized_target_type == "date":
            return f"DATE '{timestamp_value.strftime('%Y-%m-%d')}'"
        return f"TIMESTAMP '{timestamp_value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

    if hasattr(value, "isoformat") and normalized_target_type == "date":
        return f"DATE '{value.isoformat()}'"

    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    if isinstance(value, (int, float)):
        return str(value)

    escaped = str(value).replace("'", "''")
    if normalized_target_type:
        return f"CAST('{escaped}' AS {target_type})"
    return f"'{escaped}'"


def _get_trino_insert_chunk_size(
    explicit_value: int | None,
    connection_type: str = "trino",
) -> int:
    if explicit_value is not None:
        if explicit_value <= 0:
            raise ValueError("trino_insert_chunk_size must be a positive integer.")
        return explicit_value

    try:
        config = get_connection_config(connection_type)
    except (SqlConfigError, UnsupportedConnectionTypeError):
        return DEFAULT_TRINO_INSERT_CHUNK_SIZE
    if isinstance(config, TrinoConfig) and config.insert_chunk_size is not None:
        return config.insert_chunk_size
    return DEFAULT_TRINO_INSERT_CHUNK_SIZE


def _is_null_like(value: Any) -> bool:
    if value is None:
        return True

    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
