from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pandas as pd

from ....connection.errors import UnsupportedConnectionTypeError
from ....labels import apply_query_label
from analytics_toolkit.general import time_print
from ..runtime.models import RowBatch
from ..runtime.retry import replace_connection, rollback_quietly, run_with_retry


def iter_source_batches(
    connection_key: str,
    connection_backend: str,
    connection_ref: dict[str, Any],
    query: str,
    batch_size: int,
    retry_cnt: int,
    timeout_increment: int | float,
    query_label: str | None = None,
    get_batch_size: Callable[[], int] | None = None,
) -> Iterator[RowBatch]:
    labeled_query = apply_query_label(query, query_label)
    batch_size_getter = get_batch_size or (lambda: batch_size)
    if connection_backend in {"gp", "trino"}:
        yield from _iter_dbapi_batches(
            connection_key,
            connection_backend,
            connection_ref,
            labeled_query,
            batch_size_getter,
            retry_cnt=retry_cnt,
            timeout_increment=timeout_increment,
        )
        return
    if connection_backend == "ch":
        yield from _iter_clickhouse_batches(
            connection_key,
            connection_ref,
            labeled_query,
            batch_size_getter,
            retry_cnt=retry_cnt,
            timeout_increment=timeout_increment,
        )
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def _iter_dbapi_batches(
    connection_key: str,
    connection_backend: str,
    connection_ref: dict[str, Any],
    query: str,
    get_batch_size: Callable[[], int],
    retry_cnt: int,
    timeout_increment: int | float,
) -> Iterator[RowBatch]:
    cursor, columns = _start_dbapi_query_with_retry(
        connection_key,
        connection_backend,
        connection_ref,
        query,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
    )
    try:
        while True:
            rows = cursor.fetchmany(get_batch_size())
            if not rows:
                break
            yield RowBatch(columns=columns, rows=_rows_as_tuples(rows))
    except Exception:
        time_print(f"SQL failed while reading transfer source:\n{query}")
        raise
    finally:
        cursor.close()


def _iter_clickhouse_batches(
    connection_key: str,
    connection_ref: dict[str, Any],
    query: str,
    get_batch_size: Callable[[], int],
    retry_cnt: int,
    timeout_increment: int | float,
) -> Iterator[RowBatch]:
    context_manager: Any | None = None
    stream_iterator: Iterator[pd.DataFrame] | None = None
    first_block: pd.DataFrame | None = None

    try:
        context_manager, stream_iterator, first_block = _start_clickhouse_stream_with_retry(
            connection_key,
            connection_ref,
            query,
            retry_cnt=retry_cnt,
            timeout_increment=timeout_increment,
        )

        columns: list[str] | None = None
        pending_rows: list[tuple[Any, ...]] = []

        if first_block is not None and not first_block.empty:
            columns = list(first_block.columns)
            pending_rows.extend(_dataframe_rows_as_tuples(first_block))

        if stream_iterator is not None:
            for block in stream_iterator:
                if block.empty:
                    continue

                if columns is None:
                    columns = list(block.columns)
                pending_rows.extend(_dataframe_rows_as_tuples(block))

                while True:
                    current_batch_size = get_batch_size()
                    if len(pending_rows) < current_batch_size:
                        break
                    batch_rows = pending_rows[:current_batch_size]
                    pending_rows = pending_rows[current_batch_size:]
                    yield RowBatch(columns=columns, rows=batch_rows)

        if pending_rows and columns is not None:
            yield RowBatch(columns=columns, rows=pending_rows)
    except Exception:
        time_print(f"SQL failed while reading transfer source:\n{query}")
        raise
    finally:
        if context_manager is not None:
            context_manager.__exit__(None, None, None)


def _start_dbapi_query_with_retry(
    connection_key: str,
    connection_backend: str,
    connection_ref: dict[str, Any],
    query: str,
    retry_cnt: int,
    timeout_increment: int | float,
) -> tuple[Any, list[str]]:
    def operation(attempt: int) -> tuple[Any, list[str]]:
        cursor = connection_ref["connection"].cursor()
        try:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description or []]
            return cursor, columns
        except Exception:
            cursor.close()
            if connection_backend == "gp":
                rollback_quietly(connection_ref["connection"])
            replace_connection(connection_key, connection_ref)
            raise

    return run_with_retry(
        operation_name=f"starting source query on {connection_key} ({connection_backend})",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )


def _start_clickhouse_stream_with_retry(
    connection_key: str,
    connection_ref: dict[str, Any],
    query: str,
    retry_cnt: int,
    timeout_increment: int | float,
) -> tuple[Any, Iterator[pd.DataFrame], pd.DataFrame | None]:
    def operation(attempt: int) -> tuple[Any, Iterator[pd.DataFrame], pd.DataFrame | None]:
        context_manager = connection_ref["connection"].query_df_stream(query)
        try:
            stream = context_manager.__enter__()
            iterator = iter(stream)
            while True:
                try:
                    block = next(iterator)
                except StopIteration:
                    return context_manager, iterator, None
                if block.empty:
                    continue
                return context_manager, iterator, block
        except Exception:
            context_manager.__exit__(None, None, None)
            replace_connection(connection_key, connection_ref)
            raise

    return run_with_retry(
        operation_name=f"starting source query on {connection_key} (ch)",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )


def _rows_as_tuples(rows: list[Any]) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in rows]


def _dataframe_rows_as_tuples(block: pd.DataFrame) -> list[tuple[Any, ...]]:
    return list(block.itertuples(index=False, name=None))
