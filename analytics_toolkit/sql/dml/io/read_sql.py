from __future__ import annotations

from typing import Any

import pandas as pd
import sqlparse

from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from ...connection.config import get_connection_config
from ...connection.get_sql_connection import get_sql_connection
from ..transfer.runtime.retry import rollback_quietly, run_with_retry
from analytics_toolkit.general import time_print


def _read_trino(conn: Any, query: str, print_queries: bool = True) -> pd.DataFrame:
    time_print("Reading DataFrame from trino")
    try:
        _maybe_print_query(query, print_queries)
        return _read_dbapi_query(conn, query)
    except Exception:
        time_print(f"SQL failed on trino:\n{query}")
        raise


def _read_gp(conn: Any, query: str, print_queries: bool = True) -> pd.DataFrame:
    time_print("Reading DataFrame from gp")
    try:
        _maybe_print_query(query, print_queries)
        return _read_dbapi_query(conn, query)
    except Exception:
        time_print(f"SQL failed on gp:\n{query}")
        raise


def _read_ch(client: Any, query: str, print_queries: bool = True) -> pd.DataFrame:
    time_print("Reading DataFrame from ch")
    try:
        _maybe_print_query(query, print_queries)
        return client.query_df(query)
    except Exception:
        time_print(f"SQL failed on ch:\n{query}")
        raise


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

    statements = [statement.strip() for statement in sqlparse.split(sql) if statement.strip()]
    if len(statements) != 1:
        raise InvalidSqlInputError("read_sql expects exactly one SQL statement.")
    sql = statements[0].rstrip(";").rstrip()

    def operation(attempt: int) -> pd.DataFrame:
        connection = get_sql_connection(connection_key)
        try:
            if backend == "trino":
                return _read_trino(connection, sql, print_queries=print_queries)
            if backend == "gp":
                return _read_gp(connection, sql, print_queries=print_queries)
            if backend == "ch":
                return _read_ch(connection, sql, print_queries=print_queries)
            raise UnsupportedConnectionTypeError(
                "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
            )
        except Exception:
            if backend == "gp":
                rollback_quietly(connection)
            raise
        finally:
            time_print(f"Closing {connection_key} connection")
            connection.close()

    return run_with_retry(
        operation_name=f"reading query on {connection_key} ({backend})",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )


def _maybe_print_query(query: str, print_queries: bool) -> None:
    if print_queries:
        statements = [statement.strip() for statement in sqlparse.split(query) if statement.strip()]
        statement_to_print = statements[0] if statements else query.strip()
        time_print(f"Executing query:\n{statement_to_print}")
