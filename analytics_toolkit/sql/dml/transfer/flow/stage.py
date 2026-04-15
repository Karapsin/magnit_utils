import pandas as pd

from ...load.stage import create_stage_table
from ...table.table_ops import get_trino_table_column_types, table_exists
from ...table.table_validation import validate_key_columns_in_columns
from ..runtime.models import TransferConnectionRefs, TransferOptions, TransferStageState


def create_stage_state(
    options: TransferOptions,
    connection_refs: TransferConnectionRefs,
) -> TransferStageState:
    return TransferStageState(
        target_exists=table_exists(
            options.to_db,
            connection_refs.target["connection"],
            options.target_table,
        )
    )


def initialize_stage_for_first_batch(
    options: TransferOptions,
    connection_refs: TransferConnectionRefs,
    stage_state: TransferStageState,
    batch: pd.DataFrame,
) -> None:
    stage_state.first_non_empty_batch = batch.copy()
    validate_key_columns_in_columns(
        options.key_columns,
        stage_state.first_non_empty_batch.columns,
    )
    validate_key_columns_in_columns(
        options.gp_distributed_by_key,
        stage_state.first_non_empty_batch.columns,
    )
    stage_state.stage_table = create_stage_table(
        connection_type=options.to_db,
        connection=connection_refs.target["connection"],
        target_table=options.target_table,
        batch=batch,
        gp_distributed_by_key=options.gp_distributed_by_key,
    )
    stage_state.stage_table_created = True
    if options.to_db == "trino":
        stage_state.stage_column_types = get_trino_table_column_types(
            connection_refs.target["connection"],
            stage_state.stage_table,
        )
