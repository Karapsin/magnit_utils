from __future__ import annotations

from typing import Any

from tqdm import tqdm

from ....connection.get_sql_connection import get_sql_connection
from analytics_toolkit.general import time_print
from ...load.load_sql_table import insert_rows_batch
from .finalize import cleanup_stage, finalize_loaded_stage
from ..runtime.models import (
    AdaptiveBatchSizer,
    TransferConnectionRefs,
    TransferOptions,
    TransferStageState,
)
from ..runtime.retry import close_connection_ref, run_with_retry
from ..io.source import iter_source_batches
from ..schema import inspect_source_query_schema, map_source_schema_to_target
from .stage import create_stage_state, initialize_stage_for_first_batch


def run_transfer_attempt(
    options: TransferOptions,
    read_retry_cnt: int,
    insert_retry_cnt: int,
) -> int:
    connection_refs = TransferConnectionRefs(
        source={"connection": get_sql_connection(options.from_db_key)},
        target={"connection": get_sql_connection(options.to_db_key)},
    )
    total_rows = 0
    transfer_error: Exception | None = None
    cleanup_error: Exception | None = None
    stage_state = create_stage_state(options, connection_refs)

    try:
        source_schema = inspect_source_query_schema(
            options.from_db_backend,
            connection_refs.source["connection"],
            options.source_sql,
        )
        stage_state.source_column_types = {
            column.name: column.native_type for column in source_schema
        }
        if source_schema:
            stage_state.stage_column_types = map_source_schema_to_target(
                source_schema,
                options.to_db_backend,
            )
        total_rows = load_stage_batches(
            options=options,
            connection_refs=connection_refs,
            stage_state=stage_state,
            read_retry_cnt=read_retry_cnt,
            insert_retry_cnt=insert_retry_cnt,
        )
        finalize_loaded_stage(
            options=options,
            connection_refs=connection_refs,
            stage_state=stage_state,
            total_rows=total_rows,
        )
    except Exception as exc:
        transfer_error = exc
    finally:
        try:
            cleanup_stage(
                options=options,
                connection_refs=connection_refs,
                stage_state=stage_state,
                read_retry_cnt=read_retry_cnt,
            )
        except Exception as exc:
            cleanup_error = exc
        finally:
            close_connection_ref(connection_refs.source, options.from_db_key, "source")
            close_connection_ref(connection_refs.target, options.to_db_key, "target")

    if transfer_error is not None:
        if cleanup_error is not None:
            time_print(
                f"Cleanup failed while handling transfer error: {cleanup_error!r}"
            )
        raise transfer_error.with_traceback(transfer_error.__traceback__)
    if cleanup_error is not None:
        raise cleanup_error.with_traceback(cleanup_error.__traceback__)
    return total_rows


def load_stage_batches(
    options: TransferOptions,
    connection_refs: TransferConnectionRefs,
    stage_state: TransferStageState,
    read_retry_cnt: int,
    insert_retry_cnt: int,
) -> int:
    total_rows = 0
    progress_bar = _make_transfer_progress_bar(options)
    progress_tracker = _ProgressTracker(progress_bar)
    batch_sizer = AdaptiveBatchSizer(
        enabled=options.adaptive_batch_size,
        current_size=options.batch_size,
        min_size=options.min_batch_size,
        max_size=options.max_batch_size,
        target_seconds=options.target_batch_seconds,
    )
    try:
        for batch in iter_source_batches(
            options.from_db_key,
            options.from_db_backend,
            connection_refs.source,
            options.source_sql,
            options.batch_size,
            retry_cnt=read_retry_cnt,
            timeout_increment=options.timeout_increment,
            query_label=options.query_label,
            get_batch_size=lambda: batch_sizer.current_size,
        ):
            if batch.empty:
                continue

            if stage_state.first_non_empty_batch is None:
                initialize_stage_for_first_batch(
                    options=options,
                    connection_refs=connection_refs,
                    stage_state=stage_state,
                    batch=batch,
                )

            progress_tracker.start_batch()
            inserted_rows = insert_rows_batch(
                options.to_db_backend,
                connection_refs.target,
                stage_state.stage_table,
                batch.columns,
                batch.rows,
                retry_fn=run_with_retry,
                retry_cnt=insert_retry_cnt,
                timeout_increment=options.timeout_increment,
                target_column_types=stage_state.stage_column_types,
                trino_insert_chunk_size=options.trino_insert_chunk_size,
                query_label=options.query_label,
                on_success=batch_sizer.update,
                on_progress=progress_tracker.update,
            )
            progress_tracker.complete_batch(inserted_rows)
            total_rows += inserted_rows
            time_print(
                f"Transferred batch of {inserted_rows} row(s) "
                f"to {options.to_db_key}.{stage_state.stage_table}"
            )
        return total_rows
    finally:
        progress_bar.close()


class _ProgressTracker:
    def __init__(self, progress_bar: Any) -> None:
        self.progress_bar = progress_bar
        self.total_rows = 0
        self._batch_start_rows = 0

    def start_batch(self) -> None:
        self._batch_start_rows = self.total_rows

    def update(self, rows: int) -> None:
        self.total_rows += rows
        self.progress_bar.update(rows)

    def complete_batch(self, rows: int) -> None:
        batch_progress_rows = self.total_rows - self._batch_start_rows
        remaining_rows = rows - batch_progress_rows
        if remaining_rows > 0:
            self.update(remaining_rows)


def _make_transfer_progress_bar(options: TransferOptions) -> Any:
    return tqdm(
        total=None,
        desc=f"transfer_table {options.to_db_key}.{options.target_table}",
        unit="row",
        disable=not options.progress,
    )
