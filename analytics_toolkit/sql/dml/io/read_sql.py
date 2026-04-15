from __future__ import annotations

from typing import Any

import pandas as pd
import sqlparse

from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from ...connection.get_sql_connection import with_sql_connection
from ...general.logging import time_print


@with_sql_connection("trino")
def _read_trino(conn: Any, query: str, print_queries: bool = True) -> pd.DataFrame:
    time_print("Reading DataFrame from trino")
    try:
        _maybe_print_query(query, print_queries)
        return _read_dbapi_query(conn, query)
    except Exception:
        time_print(f"SQL failed on trino:\n{query}")
        raise


@with_sql_connection("gp")
def _read_gp(conn: Any, query: str, print_queries: bool = True) -> pd.DataFrame:
    time_print("Reading DataFrame from gp")
    try:
        _maybe_print_query(query, print_queries)
        return _read_dbapi_query(conn, query)
    except Exception:
        time_print(f"SQL failed on gp:\n{query}")
        raise


@with_sql_connection("ch")
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
) -> pd.DataFrame:
    normalized_type = connection_type.strip().lower()
    sql = query.strip()

    if not sql:
        raise InvalidSqlInputError("Query string must not be empty.")

    statements = [statement.strip() for statement in sqlparse.split(sql) if statement.strip()]
    if len(statements) != 1:
        raise InvalidSqlInputError("read_sql expects exactly one SQL statement.")
    sql = statements[0].rstrip(";").rstrip()

    if normalized_type == "trino":
        return _read_trino(sql, print_queries=print_queries)
    if normalized_type == "gp":
        return _read_gp(sql, print_queries=print_queries)
    if normalized_type == "ch":
        return _read_ch(sql, print_queries=print_queries)

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def _maybe_print_query(query: str, print_queries: bool) -> None:
    if print_queries:
        statements = [statement.strip() for statement in sqlparse.split(query) if statement.strip()]
        statement_to_print = statements[0] if statements else query.strip()
        time_print(f"Executing query:\n{statement_to_print}")
