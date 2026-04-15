from __future__ import annotations

from ....connection.get_sql_connection import get_sql_connection
from ....general.logging import time_print
from ...load.load_sql_table import insert_table_batch
from .finalize import cleanup_stage, finalize_loaded_stage
from ..runtime.models import TransferConnectionRefs, TransferOptions, TransferStageState
from ..runtime.retry import close_connection_ref, run_with_retry
from ..io.source import iter_source_batches
from .stage import create_stage_state, initialize_stage_for_first_batch


def run_transfer_attempt(
    options: TransferOptions,
    read_retry_cnt: int,
    insert_retry_cnt: int,
) -> int:
    connection_refs = TransferConnectionRefs(
        source={"connection": get_sql_connection(options.from_db)},
        target={"connection": get_sql_connection(options.to_db)},
    )
    total_rows = 0
    transfer_error: Exception | None = None
    cleanup_error: Exception | None = None
    stage_state = create_stage_state(options, connection_refs)

    try:
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
            close_connection_ref(connection_refs.source, options.from_db, "source")
            close_connection_ref(connection_refs.target, options.to_db, "target")

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
    for batch in iter_source_batches(
        options.from_db,
        connection_refs.source,
        options.source_sql,
        options.batch_size,
        retry_cnt=read_retry_cnt,
        timeout_increment=options.timeout_increment,
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

        inserted_rows = insert_table_batch(
            options.to_db,
            connection_refs.target,
            stage_state.stage_table,
            batch,
            retry_fn=run_with_retry,
            retry_cnt=insert_retry_cnt,
            timeout_increment=options.timeout_increment,
            target_column_types=stage_state.stage_column_types,
        )
        total_rows += inserted_rows
        time_print(
            f"Transferred batch of {inserted_rows} row(s) to {options.to_db}.{stage_state.stage_table}"
        )
    return total_rows
