from collections.abc import Sequence

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
    validate_ch_columns_in_columns(
        options.ch_partition_by,
        stage_state.first_non_empty_batch.columns,
        "ch_partition_by",
    )
    validate_ch_columns_in_columns(
        options.ch_order_by,
        stage_state.first_non_empty_batch.columns,
        "ch_order_by",
    )
    stage_state.stage_table = create_stage_table(
        connection_type=options.to_db_backend,
        connection=connection_refs.target["connection"],
        target_table=options.target_table,
        batch=batch,
        gp_distributed_by_key=options.gp_distributed_by_key,
        connection_key=options.to_db_key,
    )
    stage_state.stage_table_created = True
    if options.to_db_backend == "trino":
        stage_state.stage_column_types = get_trino_table_column_types(
            connection_refs.target["connection"],
            stage_state.stage_table,
            connection_key=options.to_db_key,
        )


def validate_ch_columns_in_columns(
    value: list[str] | str | None,
    columns: Sequence[str],
    option_name: str,
) -> None:
    if value is None or isinstance(value, str):
        return

    available_columns = {str(column) for column in columns}
    missing_columns = [column for column in value if column not in available_columns]
    if missing_columns:
        raise ValueError(
            f"{option_name} columns were not found in the staged data: "
            + ", ".join(missing_columns)
        )
