from __future__ import annotations

import random
import time
from typing import Any, Iterator

import sqlparse
from tqdm import tqdm

from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from ...connection.config import get_connection_config
from ...connection.get_sql_connection import get_sql_connection
from ..transfer.runtime.retry import rollback_quietly, run_with_retry
from analytics_toolkit.general import time_print


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
            _maybe_print_query(statement, print_queries, split_preview=True)
            _execute_trino_statement(cursor, statement)
            _maybe_sleep_between_queries(index, total, random_sleep_seconds)
    except Exception:
        failed_query = statement if statement is not None else query
        time_print(f"SQL failed on trino:\n{failed_query}")
        raise
    return None


def _execute_gp(
    conn: Any,
    query: str,
    random_sleep_seconds: float | None = 5,
    print_queries: bool = True,
    gp_break_query: bool = False,
    gp_commit_each_statement: bool = False,
) -> Any:
    statement: str | None = None
    try:
        with conn.cursor() as cursor:
            should_commit_at_end = True
            if not gp_break_query:
                time_print("Executing 1 statement set on gp")
                statement = query
                _maybe_print_query(statement, print_queries, split_preview=False)
                cursor.execute(statement)
            else:
                statements = _split_sql_statements(query)
                time_print(f"Executing {len(statements)} statement(s) on gp")
                total = len(statements)
                for index, statement in enumerate(
                    _iterate_statements_with_progress(statements, "gp"),
                    start=1,
                ):
                    _maybe_print_query(statement, print_queries, split_preview=True)
                    cursor.execute(statement)
                    if gp_commit_each_statement:
                        conn.commit()
                        should_commit_at_end = False
                    _maybe_sleep_between_queries(index, total, random_sleep_seconds)
            if should_commit_at_end:
                conn.commit()
            return None
    except Exception:
        failed_query = statement if statement is not None else query
        time_print(f"SQL failed on gp:\n{failed_query}")
        rollback_quietly(conn)
        raise


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
            _maybe_print_query(statement, print_queries, split_preview=True)
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
    gp_break_query: bool = False,
    gp_commit_each_statement: bool = False,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
) -> Any:
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

    def operation(attempt: int) -> Any:
        connection = get_sql_connection(connection_key)
        try:
            if backend == "trino":
                return _execute_trino(
                    connection,
                    sql,
                    random_sleep_seconds=random_sleep_seconds,
                    print_queries=print_queries,
                )
            if backend == "gp":
                return _execute_gp(
                    connection,
                    sql,
                    random_sleep_seconds=random_sleep_seconds,
                    print_queries=print_queries,
                    gp_break_query=gp_break_query,
                    gp_commit_each_statement=gp_commit_each_statement,
                )
            if backend == "ch":
                return _execute_ch(
                    connection,
                    sql,
                    random_sleep_seconds=random_sleep_seconds,
                    print_queries=print_queries,
                )
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
        operation_name=f"executing SQL on {connection_key} ({backend})",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
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


def _maybe_print_query(query: str, print_queries: bool, split_preview: bool) -> None:
    if print_queries:
        if split_preview:
            statements = _split_sql_statements(query)
            statement_to_print = statements[0] if statements else query.strip()
        else:
            statement_to_print = query.strip()
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
