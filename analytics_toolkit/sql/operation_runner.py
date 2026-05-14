from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from analytics_toolkit.general import time_print

from .connection.errors import SqlOperationContext, annotate_sql_exception


T = TypeVar("T")
ConnectionRef = dict[str, Any]


def run_connection_operation(
    *,
    operation_name: str,
    connection_key: str,
    backend: str,
    retry_cnt: int,
    timeout_increment: int | float,
    open_connection: Callable[[str], Any],
    operation: Callable[[ConnectionRef, int], T],
    context_factory: Callable[[int], SqlOperationContext],
    cleanup: Callable[[ConnectionRef], None] | None = None,
) -> T:
    """Run a public SQL operation with a fresh connection for each retry."""

    def attempt_operation(attempt: int) -> T:
        connection_ref: ConnectionRef = {"connection": open_connection(connection_key)}
        try:
            return operation(connection_ref, attempt)
        except Exception as exc:
            annotate_sql_exception(exc, context_factory(attempt))
            if backend == "gp":
                _rollback_quietly(connection_ref["connection"])
            raise
        finally:
            if cleanup is None:
                _close_connection(connection_ref, connection_key)
            else:
                cleanup(connection_ref)

    return _run_with_retry(
        operation_name=operation_name,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=attempt_operation,
    )


def run_retrying_operation(
    *,
    operation_name: str,
    retry_cnt: int,
    timeout_increment: int | float,
    operation: Callable[[int], T],
    context_factory: Callable[[int], SqlOperationContext],
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> T:
    def annotated_operation(attempt: int) -> T:
        try:
            return operation(attempt)
        except Exception as exc:
            annotate_sql_exception(exc, context_factory(attempt))
            raise

    return _run_with_retry(
        operation_name=operation_name,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=annotated_operation,
        retryable_exceptions=retryable_exceptions,
    )


def run_annotated_once(
    *,
    operation: Callable[[], T],
    context: SqlOperationContext,
) -> T:
    try:
        return operation()
    except Exception as exc:
        annotate_sql_exception(exc, context)
        raise


def _close_connection(connection_ref: ConnectionRef, connection_key: str) -> None:
    time_print(f"Closing {connection_key} connection")
    connection_ref["connection"].close()


def _run_with_retry(**kwargs: Any) -> Any:
    from .dml.transfer.runtime.retry import run_with_retry

    return run_with_retry(**kwargs)


def _rollback_quietly(connection: Any) -> None:
    from .dml.transfer.runtime.retry import rollback_quietly

    rollback_quietly(connection)
