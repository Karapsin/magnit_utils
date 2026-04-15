from __future__ import annotations

from typing import Any

import pandas as pd

from ...ddl.create_sql_table import create_sql_table
from ...connection.errors import UnsupportedConnectionTypeError
from ...connection.get_sql_connection import get_sql_connection
from ...general.logging import time_print
from .load_sql_table import insert_table_batch
from .models import LoadOptions, LoadState
from .stage import create_stage_table
from ..table.table_ops import (
    analyze_table,
    drop_table,
    insert_from_table,
    get_trino_table_column_types,
    table_exists,
)
from ..table.table_validation import (
    normalize_key_columns,
    validate_key_columns_in_columns,
    validate_stage_target_key_overlap,
)


def load_df(
    connection_type: str,
    destination_table: str,
    df: pd.DataFrame,
    append: bool = False,
    gp_distributed_by_key: list[str] | None = None,
    key_columns: list[str] | None = None,
) -> int:
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    options = _build_load_options(
        connection_type=connection_type,
        destination_table=destination_table,
        append=append,
        gp_distributed_by_key=gp_distributed_by_key,
        key_columns=key_columns,
    )
    connection_ref = {"connection": get_sql_connection(options.connection_type)}
    state: LoadState | None = None
    try:
        state = LoadState(
            target_exists=table_exists(
                options.connection_type,
                connection_ref["connection"],
                options.destination_table,
            )
        )

        if df.empty:
            if options.append and state.target_exists:
                time_print(
                    f"Skipping empty DataFrame append into {options.connection_type}.{options.destination_table}"
                )
                return 0
            raise ValueError("Cannot create or replace a table from an empty DataFrame.")

        if options.gp_distributed_by_key:
            validate_key_columns_in_columns(options.gp_distributed_by_key, df.columns)

        validate_key_columns_in_columns(options.key_columns, df.columns)
        _validate_dataframe_key_uniqueness(df, options.key_columns)

        if not options.append and state.target_exists:
            time_print(
                f"Dropping existing table {options.destination_table} on {options.connection_type}"
            )
            drop_table(
                options.connection_type,
                connection_ref["connection"],
                options.destination_table,
            )
            state.target_exists = False

        if not state.target_exists:
            create_sql_table(
                options.connection_type,
                connection_ref["connection"],
                options.destination_table,
                df,
                gp_distributed_by_key=options.gp_distributed_by_key,
            )

        if options.connection_type == "trino":
            state.target_column_types = get_trino_table_column_types(
                connection_ref["connection"],
                options.destination_table,
            )

        inserted_rows = _load_dataframe(
            options=options,
            state=state,
            connection_ref=connection_ref,
            df=df,
        )

        analyze_table(
            connection_type=options.connection_type,
            connection=connection_ref["connection"],
            table_name=options.destination_table,
        )
        time_print(
            f"Finished loading DataFrame into {options.connection_type}.{options.destination_table}: {inserted_rows} row(s)"
        )
        return inserted_rows
    finally:
        _cleanup_load(connection_ref, options, state)

def _build_load_options(
    connection_type: str,
    destination_table: str,
    append: bool,
    gp_distributed_by_key: list[str] | None,
    key_columns: list[str] | None,
) -> LoadOptions:
    options = LoadOptions(
        connection_type=connection_type.strip().lower(),
        destination_table=destination_table.strip(),
        append=append,
        gp_distributed_by_key=_normalize_gp_distributed_by_key(gp_distributed_by_key),
        key_columns=normalize_key_columns(key_columns),
    )

    if options.connection_type not in {"trino", "gp", "ch"}:
        raise UnsupportedConnectionTypeError(
            "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
        )
    if not options.destination_table:
        raise ValueError("destination_table must not be empty.")
    if options.gp_distributed_by_key and options.connection_type != "gp":
        raise ValueError("gp_distributed_by_key can only be used when connection_type='gp'.")
    return options


def _load_dataframe(
    options: LoadOptions,
    state: LoadState,
    connection_ref: dict[str, Any],
    df: pd.DataFrame,
) -> int:
    if options.append and state.target_exists and options.key_columns:
        state.overlap_stage_table = create_stage_table(
            connection_type=options.connection_type,
            connection=connection_ref["connection"],
            target_table=options.destination_table,
            batch=df,
            gp_distributed_by_key=options.gp_distributed_by_key,
        )
        insert_table_batch(
            options.connection_type,
            connection_ref,
            state.overlap_stage_table,
            df,
            retry_fn=_run_without_retry,
            retry_cnt=1,
            timeout_increment=0,
            target_column_types=state.target_column_types,
        )
        validate_stage_target_key_overlap(
            connection_type=options.connection_type,
            connection=connection_ref["connection"],
            stage_table=state.overlap_stage_table,
            target_table=options.destination_table,
            key_columns=options.key_columns,
            target_exists=state.target_exists,
            replace_target_table=False,
        )
        insert_from_table(
            options.connection_type,
            connection_ref["connection"],
            options.destination_table,
            state.overlap_stage_table,
        )
        return len(df)

    return insert_table_batch(
        options.connection_type,
        connection_ref,
        options.destination_table,
        df,
        retry_fn=_run_without_retry,
        retry_cnt=1,
        timeout_increment=0,
        target_column_types=state.target_column_types,
    )


def _cleanup_load(
    connection_ref: dict[str, Any],
    options: LoadOptions,
    state: LoadState | None,
) -> None:
    if state is not None and state.overlap_stage_table is not None:
        try:
            drop_table(
                options.connection_type,
                connection_ref["connection"],
                state.overlap_stage_table,
            )
        except Exception:
            time_print(
                f"Failed to drop temporary load_df stage table {state.overlap_stage_table}"
            )
    time_print(f"Closing {options.connection_type} connection")
    connection_ref["connection"].close()


def _run_without_retry(
    operation_name: str,
    retry_cnt: int,
    timeout_increment: int | float,
    operation: Any,
) -> Any:
    return operation(1)


def _normalize_gp_distributed_by_key(
    gp_distributed_by_key: list[str] | None,
) -> list[str] | None:
    if gp_distributed_by_key is None:
        return None

    normalized = [column.strip() for column in gp_distributed_by_key]
    if not normalized:
        raise ValueError("gp_distributed_by_key must not be empty when provided.")
    if any(not column for column in normalized):
        raise ValueError("gp_distributed_by_key must not contain empty column names.")
    if len(set(normalized)) != len(normalized):
        raise ValueError("gp_distributed_by_key must not contain duplicate column names.")
    return normalized


def _validate_dataframe_key_uniqueness(
    df: pd.DataFrame,
    key_columns: list[str] | None,
) -> None:
    if not key_columns:
        return

    if df.duplicated(subset=key_columns, keep=False).any():
        raise ValueError(
            "Duplicate key values found in DataFrame for key_columns: "
            + ", ".join(key_columns)
        )
