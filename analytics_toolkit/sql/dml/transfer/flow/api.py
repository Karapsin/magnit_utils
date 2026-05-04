from __future__ import annotations

from collections.abc import Sequence

from ....connection.config import TrinoConfig, get_connection_config
from analytics_toolkit.general import time_print
from ...load.load_sql_table import AmbiguousTableLoadError
from ...table.table_validation import normalize_key_columns
from .attempt import run_transfer_attempt
from ..runtime.models import TransferOptions
from ..runtime.retry import run_with_retry

def transfer_table(
    from_db: str,
    to_db: str,
    from_sql: str,
    to_table: str,
    replace_target_table: bool = True,
    batch_size: int = 100_000,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    full_retry_cnt: int = 5,
    full_timeout_increment: int | float = 60 * 10,
    key_columns: list[str] | None = None,
    gp_distributed_by_key: list[str] | None = None,
    trino_insert_chunk_size: int | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "core",
    sharding_key: str = "rand()",
) -> int:
    options = build_transfer_options(
        from_db=from_db,
        to_db=to_db,
        from_sql=from_sql,
        to_table=to_table,
        replace_target_table=replace_target_table,
        batch_size=batch_size,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        full_retry_cnt=full_retry_cnt,
        full_timeout_increment=full_timeout_increment,
        key_columns=key_columns,
        gp_distributed_by_key=gp_distributed_by_key,
        trino_insert_chunk_size=trino_insert_chunk_size,
        ch_partition_by=ch_partition_by,
        ch_order_by=ch_order_by,
        ch_engine=ch_engine,
        ch_cluster=ch_cluster,
        ch_sharding_key=sharding_key,
    )

    time_print(
        f"Starting table transfer from {options.from_db_key} "
        f"to {options.to_db_key}: {options.target_table}"
    )

    def transfer_operation(attempt: int) -> int:
        if options.to_db_backend == "gp":
            return run_transfer_attempt(
                options=options,
                read_retry_cnt=options.retry_cnt,
                insert_retry_cnt=options.retry_cnt,
            )

        def stage_restart_operation(inner_attempt: int) -> int:
            try:
                return run_transfer_attempt(
                    options=options,
                    read_retry_cnt=options.retry_cnt,
                    insert_retry_cnt=1,
                )
            except AmbiguousTableLoadError as exc:
                time_print(
                    f"Discarding staged load for {options.to_db_key} "
                    f"and restarting from scratch: {exc!r}"
                )
                raise

        return run_with_retry(
            operation_name=(
                f"restarting staged transfer from {options.from_db_key} "
                f"to {options.to_db_key}: {options.target_table}"
            ),
            retry_cnt=options.retry_cnt,
            timeout_increment=options.timeout_increment,
            operation=stage_restart_operation,
            retryable_exceptions=(AmbiguousTableLoadError,),
        )

    if options.replace_target_table:
        total_rows = run_with_retry(
            operation_name=(
                f"restarting full transfer from {options.from_db_key} "
                f"to {options.to_db_key}: {options.target_table}"
            ),
            retry_cnt=options.full_retry_cnt,
            timeout_increment=options.full_timeout_increment,
            operation=transfer_operation,
        )
    else:
        total_rows = transfer_operation(1)

    time_print(
        f"Finished table transfer from {options.from_db_key} "
        f"to {options.to_db_key}: {total_rows} row(s)"
    )
    return total_rows


def build_transfer_options(
    from_db: str,
    to_db: str,
    from_sql: str,
    to_table: str,
    replace_target_table: bool = True,
    batch_size: int = 100_000,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    full_retry_cnt: int = 5,
    full_timeout_increment: int | float = 60 * 10,
    key_columns: list[str] | None = None,
    gp_distributed_by_key: list[str] | None = None,
    trino_insert_chunk_size: int | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "core",
    ch_sharding_key: str = "rand()",
) -> TransferOptions:
    from_config = get_connection_config(from_db)
    to_config = get_connection_config(to_db)
    configured_trino_insert_chunk_size = (
        to_config.insert_chunk_size if isinstance(to_config, TrinoConfig) else None
    )
    options = TransferOptions(
        from_db_key=from_config.connection_key,
        from_db_backend=from_config.backend,
        to_db_key=to_config.connection_key,
        to_db_backend=to_config.backend,
        source_sql=from_sql.strip(),
        target_table=to_table.strip(),
        replace_target_table=replace_target_table,
        batch_size=batch_size,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        full_retry_cnt=full_retry_cnt,
        full_timeout_increment=full_timeout_increment,
        key_columns=normalize_key_columns(key_columns),
        gp_distributed_by_key=normalize_key_columns(gp_distributed_by_key),
        trino_insert_chunk_size=(
            trino_insert_chunk_size
            if trino_insert_chunk_size is not None
            else configured_trino_insert_chunk_size
        ),
        ch_partition_by=_normalize_ch_columns_or_expression(
            ch_partition_by,
            "ch_partition_by",
        ),
        ch_order_by=_normalize_ch_columns_or_expression(ch_order_by, "ch_order_by"),
        ch_engine=_normalize_ch_string(ch_engine, "ch_engine"),
        ch_cluster=_normalize_ch_string(ch_cluster, "ch_cluster"),
        ch_sharding_key=_normalize_ch_string(ch_sharding_key, "sharding_key"),
    )

    if options.from_db_key == options.to_db_key:
        raise ValueError("from_db and to_db must be different.")
    if not options.source_sql:
        raise ValueError("from_sql must not be empty.")
    if not options.target_table:
        raise ValueError("to_table must not be empty.")
    if options.batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")
    if options.retry_cnt < 1:
        raise ValueError("retry_cnt must be at least 1.")
    if options.timeout_increment < 0:
        raise ValueError("timeout_increment must be non-negative.")
    if options.full_retry_cnt < 1:
        raise ValueError("full_retry_cnt must be at least 1.")
    if options.full_timeout_increment < 0:
        raise ValueError("full_timeout_increment must be non-negative.")
    if options.gp_distributed_by_key is not None and options.to_db_backend != "gp":
        raise ValueError(
            "gp_distributed_by_key can only be used when to_db has type 'gp'."
        )
    if options.trino_insert_chunk_size is not None and options.trino_insert_chunk_size <= 0:
        raise ValueError("trino_insert_chunk_size must be a positive integer.")
    if options.to_db_backend != "ch":
        _validate_ch_options_not_used(options)
    return options


def _normalize_ch_columns_or_expression(
    value: Sequence[str] | str | None,
    option_name: str,
) -> list[str] | str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return _normalize_ch_string(value, option_name)

    normalized = [_normalize_ch_string(column, option_name) for column in value]
    if not normalized:
        raise ValueError(f"{option_name} must not be empty when provided.")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{option_name} must not contain duplicate column names.")
    return normalized


def _normalize_ch_string(value: str, option_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{option_name} must not be empty.")
    return normalized


def _validate_ch_options_not_used(options: TransferOptions) -> None:
    if options.ch_partition_by is not None:
        raise ValueError("ch_partition_by can only be used when to_db has type 'ch'.")
    if options.ch_order_by is not None:
        raise ValueError("ch_order_by can only be used when to_db has type 'ch'.")
    if options.ch_engine != "ReplicatedMergeTree":
        raise ValueError("ch_engine can only be used when to_db has type 'ch'.")
    if options.ch_cluster != "core":
        raise ValueError("ch_cluster can only be used when to_db has type 'ch'.")
    if options.ch_sharding_key != "rand()":
        raise ValueError("sharding_key can only be used when to_db has type 'ch'.")
