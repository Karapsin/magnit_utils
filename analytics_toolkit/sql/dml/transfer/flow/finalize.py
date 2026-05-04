from __future__ import annotations

from ...table.table_ops import (
    analyze_table,
    clear_ch_distributed_table_data,
    clear_target_table,
    drop_table_with_retry,
    finalize_stage_table,
)
from ...table.table_validation import (
    validate_stage_target_key_overlap,
    validate_stage_uniqueness,
)
from ..runtime.models import TransferConnectionRefs, TransferOptions, TransferStageState
from ..runtime.retry import replace_connection, rollback_quietly, run_with_retry


def finalize_loaded_stage(
    options: TransferOptions,
    connection_refs: TransferConnectionRefs,
    stage_state: TransferStageState,
    total_rows: int,
) -> None:
    if total_rows == 0:
        finalize_empty_transfer(options, connection_refs, stage_state)
        return

    if stage_state.first_non_empty_batch is None:
        raise RuntimeError("Expected a non-empty batch when rows were transferred.")
    if stage_state.stage_table is None:
        raise RuntimeError("Expected stage table to be initialized.")

    validate_stage_uniqueness(
        connection_type=options.to_db_backend,
        connection=connection_refs.target["connection"],
        stage_table=stage_state.stage_table,
        key_columns=options.key_columns,
    )
    validate_stage_target_key_overlap(
        connection_type=options.to_db_backend,
        connection=connection_refs.target["connection"],
        stage_table=stage_state.stage_table,
        target_table=options.target_table,
        key_columns=options.key_columns,
        target_exists=stage_state.target_exists,
        replace_target_table=options.replace_target_table,
    )
    finalize_stage_table(
        options.to_db_backend,
        connection_refs.target["connection"],
        stage_table=stage_state.stage_table,
        target_table=options.target_table,
        replace_target_table=options.replace_target_table,
        target_exists=stage_state.target_exists,
        sample_batch=stage_state.first_non_empty_batch,
        gp_distributed_by_key=options.gp_distributed_by_key,
        ch_partition_by=options.ch_partition_by,
        ch_order_by=options.ch_order_by,
        ch_engine=options.ch_engine,
        ch_cluster=options.ch_cluster,
        ch_sharding_key=options.ch_sharding_key,
    )
    analyze_table(
        connection_type=options.to_db_backend,
        connection=connection_refs.target["connection"],
        table_name=options.target_table,
    )


def finalize_empty_transfer(
    options: TransferOptions,
    connection_refs: TransferConnectionRefs,
    stage_state: TransferStageState,
) -> None:
    if options.replace_target_table:
        if not stage_state.target_exists:
            raise ValueError("Cannot create target table from an empty result set.")
        if options.to_db_backend == "ch":
            clear_ch_distributed_table_data(
                connection_refs.target["connection"],
                options.target_table,
                ch_cluster=options.ch_cluster,
            )
            return
        clear_target_table(
            options.to_db_backend,
            connection_refs.target["connection"],
            options.target_table,
        )
        return

    if not stage_state.target_exists:
        raise ValueError("Cannot create target table from an empty result set.")


def cleanup_stage(
    options: TransferOptions,
    connection_refs: TransferConnectionRefs,
    stage_state: TransferStageState,
    read_retry_cnt: int,
) -> None:
    if not stage_state.stage_table_created:
        return

    drop_table_with_retry(
        options.to_db_backend,
        options.to_db_key,
        connection_refs.target,
        stage_state.stage_table,
        retry_fn=run_with_retry,
        retry_cnt=read_retry_cnt,
        timeout_increment=options.timeout_increment,
        rollback_fn=rollback_quietly,
        replace_connection_fn=replace_connection,
    )
