from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from ....connection.get_sql_connection import get_sql_connection
from analytics_toolkit.general import time_print


def run_with_retry(
    operation_name: str,
    retry_cnt: int,
    timeout_increment: int | float,
    operation: Callable[[int], Any],
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
    non_retryable_predicate: Callable[[Exception], bool] | None = None,
) -> Any:
    last_error: Exception | None = None
    should_not_retry = non_retryable_predicate or is_non_retryable_sql_error

    for attempt in range(1, retry_cnt + 1):
        try:
            return operation(attempt)
        except Exception as exc:
            if not isinstance(exc, retryable_exceptions):
                raise
            if should_not_retry(exc):
                time_print(
                    f"{operation_name} failed with a non-retryable error: {exc!r}"
                )
                raise
            last_error = exc
            if attempt >= retry_cnt:
                time_print(
                    f"{operation_name} failed after {attempt} attempt(s): {exc!r}"
                )
                break

            sleep_seconds = attempt * timeout_increment
            time_print(
                f"{operation_name} failed on attempt {attempt}/{retry_cnt}: {exc!r}"
            )
            time_print(f"Retrying {operation_name} in {sleep_seconds:.2f}s")
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    if last_error is None:
        raise RuntimeError(f"{operation_name} failed without capturing an exception.")
    raise last_error.with_traceback(last_error.__traceback__)


def is_non_retryable_sql_error(exc: Exception) -> bool:
    """Return True for deterministic SQL errors that another attempt won't fix."""
    class_names = _exception_class_names(exc)
    if class_names & {
        "SyntaxError",
        "UndefinedTable",
        "UndefinedColumn",
        "UndefinedFunction",
        "InvalidColumnReference",
        "InvalidTableDefinition",
        "InvalidSchemaName",
        "SchemaNotFoundError",
        "TableNotFoundError",
    }:
        return True

    sqlstate = str(getattr(exc, "pgcode", "") or getattr(exc, "sqlstate", "")).strip()
    if sqlstate in {
        "42601",  # syntax_error
        "42P01",  # undefined_table
        "42703",  # undefined_column
        "42883",  # undefined_function
        "3F000",  # invalid_schema_name
        "42P07",  # duplicate_table
    }:
        return True

    error_name = str(
        getattr(exc, "error_name", "") or getattr(exc, "name", "")
    ).strip().upper()
    if error_name in {
        "SYNTAX_ERROR",
        "TABLE_NOT_FOUND",
        "UNKNOWN_TABLE",
        "COLUMN_NOT_FOUND",
        "SCHEMA_NOT_FOUND",
        "FUNCTION_NOT_FOUND",
        "ALREADY_EXISTS",
    }:
        return True

    message = _exception_message(exc)
    if any(pattern in message for pattern in _NON_RETRYABLE_MESSAGE_PATTERNS):
        return True
    return "table" in message and (
        "does not exist" in message or "doesn't exist" in message
    )


def _exception_class_names(exc: BaseException) -> set[str]:
    return {cls.__name__ for cls in type(exc).mro()}


def _exception_message(exc: BaseException) -> str:
    return " ".join(str(part) for part in exc.args if part).lower() or str(exc).lower()


_NON_RETRYABLE_MESSAGE_PATTERNS = (
    "syntax error",
    "syntax_error",
    "mismatched input",
    "table not found",
    "table_not_found",
    "relation does not exist",
    "unknown table",
    "no such table",
    "undefined table",
    "undefined_table",
)


def rollback_quietly(connection: Any) -> None:
    try:
        connection.rollback()
    except Exception:
        return


def replace_connection(connection_key: str, connection_ref: dict[str, Any]) -> None:
    try:
        connection_ref["connection"].close()
    except Exception:
        pass
    connection_ref["connection"] = get_sql_connection(connection_key)


def close_connection_ref(
    connection_ref: dict[str, Any],
    connection_type: str,
    role: str,
) -> None:
    connection = connection_ref.get("connection")
    if connection is None:
        return
    time_print(f"Closing {connection_type} {role} connection")
    try:
        connection.close()
    except Exception as exc:
        time_print(
            f"Failed to close {connection_type} {role} connection cleanly: {exc!r}"
        )
