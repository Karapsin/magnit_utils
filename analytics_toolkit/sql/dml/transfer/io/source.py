from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pandas as pd

from ....connection.errors import UnsupportedConnectionTypeError
from ....general.logging import time_print
from ..runtime.retry import replace_connection, rollback_quietly, run_with_retry


def iter_source_batches(
    connection_type: str,
    connection_ref: dict[str, Any],
    query: str,
    batch_size: int,
    retry_cnt: int,
    timeout_increment: int | float,
) -> Iterator[pd.DataFrame]:
    if connection_type in {"gp", "trino"}:
        yield from _iter_dbapi_batches(
            connection_type,
            connection_ref,
            query,
            batch_size,
            retry_cnt=retry_cnt,
            timeout_increment=timeout_increment,
        )
        return
    if connection_type == "ch":
        yield from _iter_clickhouse_batches(
            connection_ref,
            query,
            batch_size,
            retry_cnt=retry_cnt,
            timeout_increment=timeout_increment,
        )
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def _iter_dbapi_batches(
    connection_type: str,
    connection_ref: dict[str, Any],
    query: str,
    batch_size: int,
    retry_cnt: int,
    timeout_increment: int | float,
) -> Iterator[pd.DataFrame]:
    cursor, columns = _start_dbapi_query_with_retry(
        connection_type,
        connection_ref,
        query,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
    )
    try:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield pd.DataFrame(rows, columns=columns)
    except Exception:
        time_print(f"SQL failed while reading transfer source:\n{query}")
        raise
    finally:
        cursor.close()


def _iter_clickhouse_batches(
    connection_ref: dict[str, Any],
    query: str,
    batch_size: int,
    retry_cnt: int,
    timeout_increment: int | float,
) -> Iterator[pd.DataFrame]:
    context_manager: Any | None = None
    stream_iterator: Iterator[pd.DataFrame] | None = None
    first_block: pd.DataFrame | None = None

    try:
        context_manager, stream_iterator, first_block = _start_clickhouse_stream_with_retry(
            connection_ref,
            query,
            retry_cnt=retry_cnt,
            timeout_increment=timeout_increment,
        )

        pending_frames: list[pd.DataFrame] = []
        pending_rows = 0

        if first_block is not None and not first_block.empty:
            pending_frames.append(first_block)
            pending_rows += len(first_block)

        if stream_iterator is not None:
            for block in stream_iterator:
                if block.empty:
                    continue

                pending_frames.append(block)
                pending_rows += len(block)

                while pending_rows >= batch_size:
                    combined = pd.concat(pending_frames, ignore_index=True)
                    yield combined.iloc[:batch_size].reset_index(drop=True)

                    remainder = combined.iloc[batch_size:].reset_index(drop=True)
                    pending_frames = [remainder] if not remainder.empty else []
                    pending_rows = len(remainder)

        if pending_rows > 0:
            yield pd.concat(pending_frames, ignore_index=True)
    except Exception:
        time_print(f"SQL failed while reading transfer source:\n{query}")
        raise
    finally:
        if context_manager is not None:
            context_manager.__exit__(None, None, None)


def _start_dbapi_query_with_retry(
    connection_type: str,
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
            if connection_type == "gp":
                rollback_quietly(connection_ref["connection"])
            replace_connection(connection_type, connection_ref)
            raise

    return run_with_retry(
        operation_name=f"starting source query on {connection_type}",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )


def _start_clickhouse_stream_with_retry(
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
            replace_connection("ch", connection_ref)
            raise

    return run_with_retry(
        operation_name="starting source query on ch",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )
