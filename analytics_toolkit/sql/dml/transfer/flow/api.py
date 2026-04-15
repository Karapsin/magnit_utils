from __future__ import annotations

from ....connection.errors import UnsupportedConnectionTypeError
from ....general.logging import time_print
from ...load.load_sql_table import AmbiguousTableLoadError
from ...table.table_validation import normalize_key_columns
from .attempt import run_transfer_attempt
from ..runtime.models import TransferOptions
from ..runtime.retry import run_with_retry


SUPPORTED_CONNECTION_TYPES = {"trino", "gp", "ch"}


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
    )

    time_print(
        f"Starting table transfer from {options.from_db} to {options.to_db}: {options.target_table}"
    )

    def transfer_operation(attempt: int) -> int:
        if options.to_db == "gp":
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
                    f"Discarding staged load for {options.to_db} and restarting from scratch: {exc!r}"
                )
                raise

        return run_with_retry(
            operation_name=(
                f"restarting staged transfer from {options.from_db} "
                f"to {options.to_db}: {options.target_table}"
            ),
            retry_cnt=options.retry_cnt,
            timeout_increment=options.timeout_increment,
            operation=stage_restart_operation,
            retryable_exceptions=(AmbiguousTableLoadError,),
        )

    if options.replace_target_table:
        total_rows = run_with_retry(
            operation_name=(
                f"restarting full transfer from {options.from_db} "
                f"to {options.to_db}: {options.target_table}"
            ),
            retry_cnt=options.full_retry_cnt,
            timeout_increment=options.full_timeout_increment,
            operation=transfer_operation,
        )
    else:
        total_rows = transfer_operation(1)

    time_print(
        f"Finished table transfer from {options.from_db} to {options.to_db}: {total_rows} row(s)"
    )
    return total_rows


def build_transfer_options(
    from_db: str,
    to_db: str,
    from_sql: str,
    to_table: str,
    replace_target_table: bool,
    batch_size: int,
    retry_cnt: int,
    timeout_increment: int | float,
    full_retry_cnt: int,
    full_timeout_increment: int | float,
    key_columns: list[str] | None,
    gp_distributed_by_key: list[str] | None,
) -> TransferOptions:
    options = TransferOptions(
        from_db=normalize_connection_type(from_db),
        to_db=normalize_connection_type(to_db),
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
    )

    if options.from_db == options.to_db:
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
    if options.gp_distributed_by_key is not None and options.to_db != "gp":
        raise ValueError("gp_distributed_by_key can only be used when to_db is 'gp'.")
    return options


def normalize_connection_type(connection_type: str) -> str:
    normalized = connection_type.strip().lower()
    if normalized not in SUPPORTED_CONNECTION_TYPES:
        raise UnsupportedConnectionTypeError(
            "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
        )
    return normalized
