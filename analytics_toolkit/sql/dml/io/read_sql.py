from __future__ import annotations

from typing import Any

import pandas as pd
import sqlparse

from ...backend_adapters import get_backend_adapter
from ...connection.errors import (
    InvalidSqlInputError,
    SqlOperationContext,
    UnsupportedConnectionTypeError,
    sql_preview,
)
from ...connection.config import get_connection_config
from ...connection.get_sql_connection import get_sql_connection
from ...labels import apply_query_label
from ...operation_runner import run_connection_operation, tracked_sql_operation
from ...plans import SqlOperationMetadata, SqlOperationResult
from ...query_timing import run_timed_query
from analytics_toolkit.general import time_print
from .models import ReadSqlOptions


def _read_trino(conn: Any, query: str, print_queries: bool = False) -> pd.DataFrame:
    return get_backend_adapter("trino").read_dataframe(
        conn,
        query,
        print_queries=print_queries,
        print_query=_maybe_print_query,
        read_dbapi_query=_read_dbapi_query,
    )


def _read_gp(conn: Any, query: str, print_queries: bool = False) -> pd.DataFrame:
    return get_backend_adapter("gp").read_dataframe(
        conn,
        query,
        print_queries=print_queries,
        print_query=_maybe_print_query,
        read_dbapi_query=_read_dbapi_query,
    )


def _read_ch(client: Any, query: str, print_queries: bool = False) -> pd.DataFrame:
    return get_backend_adapter("ch").read_dataframe(
        client,
        query,
        print_queries=print_queries,
        print_query=_maybe_print_query,
        read_dbapi_query=_read_dbapi_query,
    )


def _read_dbapi_query(conn: Any, query: str) -> pd.DataFrame:
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        columns = [column[0] for column in cursor.description or []]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        cursor.close()


def read_sql(
    connection_type: str,
    query: str,
    print_queries: bool = False,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    query_label: str | None = None,
    return_metadata: bool = False,
) -> pd.DataFrame | SqlOperationResult:
    return _read_sql_impl(
        connection_type=connection_type,
        query=query,
        print_queries=print_queries,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        query_label=query_label,
        return_metadata=return_metadata,
    )


def read_sql_with_metadata(
    connection_type: str,
    query: str,
    print_queries: bool = False,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    query_label: str | None = None,
) -> SqlOperationResult:
    return _read_sql_impl(
        connection_type=connection_type,
        query=query,
        print_queries=print_queries,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        query_label=query_label,
        return_metadata=True,
    )


def _read_sql_impl(
    connection_type: str,
    query: str,
    *,
    print_queries: bool,
    retry_cnt: int,
    timeout_increment: int | float,
    query_label: str | None,
    return_metadata: bool,
) -> pd.DataFrame | SqlOperationResult:
    options = _build_read_sql_options(
        connection_type=connection_type,
        query=query,
        print_queries=print_queries,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        query_label=query_label,
        return_metadata=return_metadata,
    )
    metadata = SqlOperationMetadata(
        statement_count=1,
        query_label=options.query_label,
    )

    def operation(connection_ref: dict[str, Any], attempt: int) -> pd.DataFrame:
        with tracked_sql_operation(
            metadata=metadata,
            operation_name="read_sql",
            alias=options.connection_key,
            backend=options.backend,
            phase="read",
            retry_attempt=attempt,
            query_label=options.query_label,
            preview_sql=options.sql,
        ):
            result = _read_backend(
                options.backend,
                connection_ref["connection"],
                options.sql,
                print_queries=options.print_queries,
            )
            metadata.read_rows = len(result)
            metadata.source_rows = len(result)
            return result

    def context(attempt: int) -> SqlOperationContext:
        return SqlOperationContext(
            operation="read_sql",
            alias=options.connection_key,
            backend=options.backend,
            phase="read",
            retry_attempt=attempt,
            sql_preview=sql_preview(options.sql),
        )

    result = run_connection_operation(
        operation_name=f"reading query on {options.connection_key} ({options.backend})",
        connection_key=options.connection_key,
        backend=options.backend,
        retry_cnt=options.retry_cnt,
        timeout_increment=options.timeout_increment,
        open_connection=get_sql_connection,
        operation=operation,
        context_factory=context,
    )
    if return_metadata:
        return SqlOperationResult(
            rows=len(result),
            metadata=metadata,
            data=result,
        )
    return result


def _build_read_sql_options(
    *,
    connection_type: str,
    query: str,
    print_queries: bool,
    retry_cnt: int,
    timeout_increment: int | float,
    query_label: str | None,
    return_metadata: bool,
) -> ReadSqlOptions:
    config = get_connection_config(connection_type)
    connection_key = config.connection_key
    backend = config.backend
    sql = query.strip()

    if not sql:
        raise InvalidSqlInputError("Query string must not be empty.")
    if retry_cnt < 1:
        raise ValueError("retry_cnt must be at least 1.")
    if timeout_increment < 0:
        raise ValueError("timeout_increment must be non-negative.")

    statements = [
        statement.strip()
        for statement in sqlparse.split(sql)
        if statement.strip()
    ]
    if len(statements) != 1:
        raise InvalidSqlInputError("read_sql expects exactly one SQL statement.")
    sql = apply_query_label(statements[0].rstrip(";").rstrip(), query_label)
    return ReadSqlOptions(
        connection_key=connection_key,
        backend=backend,
        sql=sql,
        print_queries=print_queries,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        query_label=query_label,
        return_metadata=return_metadata,
    )


def _maybe_print_query(query: str, print_queries: bool) -> None:
    if print_queries:
        statements = [
            statement.strip()
            for statement in sqlparse.split(query)
            if statement.strip()
        ]
        statement_to_print = statements[0] if statements else query.strip()
        time_print(f"Executing query:\n{statement_to_print}")


_READ_FUNCTION_NAMES = {
    "trino": "_read_trino",
    "gp": "_read_gp",
    "ch": "_read_ch",
}


def _read_backend(
    backend: str,
    connection: Any,
    sql: str,
    *,
    print_queries: bool,
) -> pd.DataFrame:
    function_name = _READ_FUNCTION_NAMES.get(backend)
    if function_name is None:
        raise UnsupportedConnectionTypeError(
            "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
        )
    return run_timed_query(
        backend,
        lambda: globals()[function_name](
            connection,
            sql,
            print_queries=print_queries,
        ),
    )
