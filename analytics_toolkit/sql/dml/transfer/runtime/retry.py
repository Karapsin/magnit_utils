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
) -> Any:
    last_error: Exception | None = None

    for attempt in range(1, retry_cnt + 1):
        try:
            return operation(attempt)
        except Exception as exc:
            if not isinstance(exc, retryable_exceptions):
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


def rollback_quietly(connection: Any) -> None:
    try:
        connection.rollback()
    except Exception:
        return


def replace_connection(connection_type: str, connection_ref: dict[str, Any]) -> None:
    try:
        connection_ref["connection"].close()
    except Exception:
        pass
    connection_ref["connection"] = get_sql_connection(connection_type)


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
