from ....ch_options import validate_ch_columns_in_columns
from ...load.stage import create_stage_table
from ...table.table_ops import get_trino_table_column_types, table_exists
from ...table.table_validation import validate_key_columns_in_columns
from ..runtime.models import (
    RowBatch,
    TransferConnectionRefs,
    TransferOptions,
    TransferStageState,
)


def create_stage_state(
    options: TransferOptions,
    connection_refs: TransferConnectionRefs,
) -> TransferStageState:
    return TransferStageState(
        target_exists=table_exists(
            options.to_db_backend,
            connection_refs.target["connection"],
            options.target_table,
            connection_key=options.to_db_key,
        )
    )


def initialize_stage_for_first_batch(
    options: TransferOptions,
    connection_refs: TransferConnectionRefs,
    stage_state: TransferStageState,
    batch: RowBatch,
) -> None:
    sample_batch = batch.to_dataframe(
        include_rows=stage_state.stage_column_types is None,
    )
    stage_state.first_non_empty_batch = sample_batch
    validate_key_columns_in_columns(
        options.key_columns,
        batch.columns,
    )
    validate_key_columns_in_columns(
        options.gp_distributed_by_key,
        batch.columns,
    )
    validate_ch_columns_in_columns(
        options.ch_partition_by,
        batch.columns,
        "ch_partition_by",
        data_name="staged data",
    )
    validate_ch_columns_in_columns(
        options.ch_order_by,
        batch.columns,
        "ch_order_by",
        data_name="staged data",
    )
    stage_state.stage_table = create_stage_table(
        connection_type=options.to_db_backend,
        connection=connection_refs.target["connection"],
        target_table=options.target_table,
        batch=sample_batch,
        column_types=stage_state.stage_column_types,
        gp_distributed_by_key=options.gp_distributed_by_key,
        connection_key=options.to_db_key,
        query_label=options.query_label,
    )
    stage_state.stage_table_created = True
    if options.to_db_backend == "trino" and stage_state.stage_column_types is None:
        stage_state.stage_column_types = get_trino_table_column_types(
            connection_refs.target["connection"],
            stage_state.stage_table,
            connection_key=options.to_db_key,
        )
