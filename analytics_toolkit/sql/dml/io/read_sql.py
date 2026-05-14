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
from ...operation_runner import run_connection_operation
from analytics_toolkit.general import time_print


def _read_trino(conn: Any, query: str, print_queries: bool = True) -> pd.DataFrame:
    return get_backend_adapter("trino").read_dataframe(
        conn,
        query,
        print_queries=print_queries,
        print_query=_maybe_print_query,
        read_dbapi_query=_read_dbapi_query,
    )


def _read_gp(conn: Any, query: str, print_queries: bool = True) -> pd.DataFrame:
    return get_backend_adapter("gp").read_dataframe(
        conn,
        query,
        print_queries=print_queries,
        print_query=_maybe_print_query,
        read_dbapi_query=_read_dbapi_query,
    )


def _read_ch(client: Any, query: str, print_queries: bool = True) -> pd.DataFrame:
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
    print_queries: bool = True,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    query_label: str | None = None,
) -> pd.DataFrame:
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

    def operation(connection_ref: dict[str, Any], attempt: int) -> pd.DataFrame:
        del attempt
        return _read_backend(
            backend,
            connection_ref["connection"],
            sql,
            print_queries=print_queries,
        )

    def context(attempt: int) -> SqlOperationContext:
        return SqlOperationContext(
            operation="read_sql",
            alias=connection_key,
            backend=backend,
            phase="read",
            retry_attempt=attempt,
            sql_preview=sql_preview(sql),
        )

    return run_connection_operation(
        operation_name=f"reading query on {connection_key} ({backend})",
        connection_key=connection_key,
        backend=backend,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        open_connection=get_sql_connection,
        operation=operation,
        context_factory=context,
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
    return globals()[function_name](
        connection,
        sql,
        print_queries=print_queries,
    )
