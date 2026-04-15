from __future__ import annotations

import random
import time
from typing import Any, Iterator

import sqlparse
from tqdm import tqdm

from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from ...connection.get_sql_connection import with_sql_connection
from analytics_toolkit.general import time_print


@with_sql_connection("trino")
def _execute_trino(
    conn: Any,
    query: str,
    random_sleep_seconds: float | None = 5,
    print_queries: bool = True,
) -> Any:
    cursor = conn.cursor()
    statements = _split_sql_statements(query)
    time_print(f"Executing {len(statements)} statement(s) on trino")
    statement: str | None = None
    try:
        total = len(statements)
        for index, statement in enumerate(
            _iterate_statements_with_progress(statements, "trino"),
            start=1,
        ):
            _maybe_print_query(statement, print_queries)
            _execute_trino_statement(cursor, statement)
            _maybe_sleep_between_queries(index, total, random_sleep_seconds)
    except Exception:
        failed_query = statement if statement is not None else query
        time_print(f"SQL failed on trino:\n{failed_query}")
        raise
    return None


@with_sql_connection("gp")
def _execute_gp(
    conn: Any,
    query: str,
    random_sleep_seconds: float | None = 5,
    print_queries: bool = True,
) -> Any:
    statement: str | None = None
    try:
        with conn.cursor() as cursor:
            statements = _split_sql_statements(query)
            time_print(f"Executing {len(statements)} statement(s) on gp")
            total = len(statements)
            for index, statement in enumerate(
                _iterate_statements_with_progress(statements, "gp"),
                start=1,
            ):
                _maybe_print_query(statement, print_queries)
                cursor.execute(statement)
                _maybe_sleep_between_queries(index, total, random_sleep_seconds)
            conn.commit()
            return None
    except Exception:
        failed_query = statement if statement is not None else query
        time_print(f"SQL failed on gp:\n{failed_query}")
        conn.rollback()
        raise


@with_sql_connection("ch")
def _execute_ch(
    client: Any,
    query: str,
    random_sleep_seconds: float | None = 5,
    print_queries: bool = True,
) -> Any:
    statements = _split_sql_statements(query)
    time_print(f"Executing {len(statements)} statement(s) on ch")
    statement: str | None = None
    try:
        total = len(statements)
        for index, statement in enumerate(
            _iterate_statements_with_progress(statements, "ch"),
            start=1,
        ):
            _maybe_print_query(statement, print_queries)
            _execute_ch_statement(client, statement)
            _maybe_sleep_between_queries(index, total, random_sleep_seconds)
    except Exception:
        failed_query = statement if statement is not None else query
        time_print(f"SQL failed on ch:\n{failed_query}")
        raise
    return None


def execute_sql(
    connection_type: str,
    query: str,
    random_sleep_seconds: float | None = 5,
    print_queries: bool = True,
) -> Any:
    normalized_type = connection_type.strip().lower()
    sql = query.strip()

    if not sql:
        raise InvalidSqlInputError("Query string must not be empty.")

    if normalized_type == "trino":
        return _execute_trino(
            sql,
            random_sleep_seconds=random_sleep_seconds,
            print_queries=print_queries,
        )
    if normalized_type == "gp":
        return _execute_gp(
            sql,
            random_sleep_seconds=random_sleep_seconds,
            print_queries=print_queries,
        )
    if normalized_type == "ch":
        return _execute_ch(
            sql,
            random_sleep_seconds=random_sleep_seconds,
            print_queries=print_queries,
        )

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def _execute_ch_statement(client: Any, query: str) -> None:
    client.command(query)


def _execute_trino_statement(cursor: Any, query: str) -> None:
    cursor.execute(query)


def _split_sql_statements(query: str) -> list[str]:
    return [
        statement.strip().rstrip(";").rstrip()
        for statement in sqlparse.split(query)
        if statement.strip()
    ]


def _iterate_statements_with_progress(
    statements: list[str], connection_type: str
) -> Iterator[str]:
    if len(statements) <= 1:
        return iter(statements)

    return iter(
        tqdm(
            statements,
            desc=f"{connection_type} statements",
            unit="stmt",
        )
    )


def _maybe_print_query(query: str, print_queries: bool) -> None:
    if print_queries:
        statements = _split_sql_statements(query)
        statement_to_print = statements[0] if statements else query.strip()
        time_print(f"Executing query:\n{statement_to_print}")


def _maybe_sleep_between_queries(
    current: int, total: int, random_sleep_seconds: float | None
) -> None:
    if total <= 1 or current >= total or random_sleep_seconds is None:
        return
    if random_sleep_seconds <= 0:
        return

    sleep_seconds = random.expovariate(1 / random_sleep_seconds)
    time_print(f"Sleeping for {sleep_seconds:.2f}s before next query")
    time.sleep(sleep_seconds)
