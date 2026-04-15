from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

import pandas as pd
from psycopg2.extras import execute_values

from ...ddl.create_sql_table import column_list_sql
from ...connection.errors import UnsupportedConnectionTypeError
from analytics_toolkit.general import time_print


class AmbiguousTableLoadError(Exception):
    pass


def insert_table_batch(
    connection_type: str,
    connection_ref: dict[str, Any],
    table_name: str,
    batch: pd.DataFrame,
    retry_fn: Any,
    retry_cnt: int,
    timeout_increment: int | float,
    target_column_types: dict[str, str] | None = None,
) -> int:
    normalized_batch = normalize_batch(batch)

    def operation(attempt: int) -> int:
        connection = connection_ref["connection"]
        try:
            if connection_type == "gp":
                _insert_gp_batch(connection, table_name, normalized_batch)
                return len(normalized_batch)
            if connection_type == "trino":
                _insert_trino_batch(
                    connection,
                    table_name,
                    normalized_batch,
                    target_column_types=target_column_types,
                )
                return len(normalized_batch)
            if connection_type == "ch":
                _insert_ch_batch(connection, table_name, normalized_batch)
                return len(normalized_batch)
        except Exception as exc:
            if connection_type == "gp":
                if getattr(connection, "closed", 0):
                    raise
            else:
                time_print(
                    f"Stage insert on {connection_type} failed for {table_name}; "
                    "the current stage table will be discarded and reloaded from scratch."
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
) -> None:
    column_list = column_list_sql(batch.columns, "trino")
    cursor = connection.cursor()
    try:
        rows = list(_iter_trino_rows(batch, target_column_types))
        for row_chunk in _chunk_rows(rows, 1000):
            values_sql = ", ".join(
                _build_trino_values_tuple(
                    batch.columns,
                    row,
                    target_column_types,
                )
                for row in row_chunk
            )
            sql = f"INSERT INTO {table_name} ({column_list}) VALUES {values_sql}"
            time_print(f"Writing {len(row_chunk)} row(s) to trino table {table_name}")
            cursor.execute(sql)
    finally:
        cursor.close()


def _insert_ch_batch(client: Any, table_name: str, batch: pd.DataFrame) -> None:
    client.insert_df(table=table_name, df=batch, column_names=list(batch.columns))


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
    rows: Sequence[tuple[Any, ...]],
    chunk_size: int,
) -> Iterator[list[tuple[Any, ...]]]:
    for index in range(0, len(rows), chunk_size):
        yield list(rows[index : index + chunk_size])


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
