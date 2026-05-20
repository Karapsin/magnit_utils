from __future__ import annotations

from typing import Any

import pandas as pd

from ...connection.config import get_connection_config
from ...connection.errors import (
    InvalidSqlInputError,
    SqlOperationContext,
    UnsupportedConnectionTypeError,
    sql_preview,
)
from ...connection.get_sql_connection import get_sql_connection
from ...labels import apply_query_label
from ...operation_runner import run_connection_operation, tracked_sql_operation
from ...plans import SqlOperationMetadata, SqlOperationResult
from ...query_timing import run_timed_query
from analytics_toolkit.general import time_print
from .execute_sql import (
    _execute_ch_statement,
    _execute_trino_statement,
    _iterate_statements_with_progress,
    _maybe_print_query,
    _split_sql_statements,
)
from .models import ExecuteReadOptions


def execute_read(
    connection_type: str,
    query: str,
    print_queries: bool = False,
    gp_break_query: bool = False,
    gp_commit_each_statement: bool = False,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    query_label: str | None = None,
    return_metadata: bool = False,
) -> pd.DataFrame | SqlOperationResult:
    options = _build_execute_read_options(
        connection_type=connection_type,
        query=query,
        print_queries=print_queries,
        gp_break_query=gp_break_query,
        gp_commit_each_statement=gp_commit_each_statement,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        query_label=query_label,
        return_metadata=return_metadata,
    )
    metadata = SqlOperationMetadata(
        statement_count=len(options.statements),
        query_label=options.query_label,
    )

    def operation(connection_ref: dict[str, Any], attempt: int) -> pd.DataFrame:
        with tracked_sql_operation(
            metadata=metadata,
            operation_name="execute_read",
            alias=options.connection_key,
            backend=options.backend,
            phase="execute_read",
            retry_attempt=attempt,
            query_label=options.query_label,
            preview_sql="\n".join(options.statements),
        ):
            result = _execute_read_backend(
                options.backend,
                connection_ref["connection"],
                options.statements,
                print_queries=options.print_queries,
                gp_break_query=options.gp_break_query,
                gp_commit_each_statement=options.gp_commit_each_statement,
            )
            metadata.read_rows = len(result)
            metadata.source_rows = len(result)
            return result

    def context(attempt: int) -> SqlOperationContext:
        return SqlOperationContext(
            operation="execute_read",
            alias=options.connection_key,
            backend=options.backend,
            phase="execute_read",
            retry_attempt=attempt,
            sql_preview=sql_preview(options.statements[-1]),
        )

    result = run_connection_operation(
        operation_name=(
            f"executing SQL and reading final query on "
            f"{options.connection_key} ({options.backend})"
        ),
        connection_key=options.connection_key,
        backend=options.backend,
        retry_cnt=options.retry_cnt,
        timeout_increment=options.timeout_increment,
        open_connection=get_sql_connection,
        operation=operation,
        context_factory=context,
    )
    if options.return_metadata:
        return SqlOperationResult(
            rows=len(result),
            metadata=metadata,
            data=result,
        )
    return result


def _build_execute_read_options(
    *,
    connection_type: str,
    query: str,
    print_queries: bool,
    gp_break_query: bool,
    gp_commit_each_statement: bool,
    retry_cnt: int,
    timeout_increment: int | float,
    query_label: str | None,
    return_metadata: bool,
) -> ExecuteReadOptions:
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

    statements = _split_sql_statements(sql)
    if not statements:
        raise InvalidSqlInputError("Query string must not be empty.")
    if query_label is not None:
        statements = [
            apply_query_label(statement, query_label)
            for statement in statements
        ]
    return ExecuteReadOptions(
        connection_key=connection_key,
        backend=backend,
        statements=statements,
        print_queries=print_queries,
        gp_break_query=gp_break_query,
        gp_commit_each_statement=gp_commit_each_statement,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        query_label=query_label,
        return_metadata=return_metadata,
    )


def _execute_read_trino(
    conn: Any,
    statements: list[str],
    print_queries: bool = False,
) -> pd.DataFrame:
    time_print(
        f"Executing {max(len(statements) - 1, 0)} setup statement(s) "
        "and reading final query on trino"
    )
    cursor = conn.cursor()
    try:
        _execute_setup_statements(
            cursor,
            statements[:-1],
            connection_type="trino",
            execute_statement=_execute_trino_statement,
            print_queries=print_queries,
        )
        return _read_dbapi_cursor(cursor, statements[-1], "trino", print_queries)
    except Exception:
        time_print(f"SQL failed on trino:\n{statements[-1]}")
        raise
    finally:
        cursor.close()


def _execute_read_gp(
    conn: Any,
    statements: list[str],
    print_queries: bool = False,
    gp_break_query: bool = False,
    gp_commit_each_statement: bool = False,
) -> pd.DataFrame:
    time_print(
        f"Executing {max(len(statements) - 1, 0)} setup statement(s) "
        "and reading final query on gp"
    )
    cursor = conn.cursor()
    should_commit_at_end = len(statements) > 1
    try:
        setup_statements = statements[:-1]
        if setup_statements and not gp_break_query:
            setup_sql = ";\n".join(setup_statements)
            _maybe_print_query(setup_sql, print_queries, split_preview=False)
            run_timed_query("gp", lambda: cursor.execute(setup_sql))
        else:
            for statement in _iterate_statements_with_progress(setup_statements, "gp"):
                _maybe_print_query(statement, print_queries, split_preview=True)
                run_timed_query(
                    "gp",
                    lambda statement=statement: cursor.execute(statement),
                )
                if gp_commit_each_statement:
                    conn.commit()
                    should_commit_at_end = False

        result = _read_dbapi_cursor(cursor, statements[-1], "gp", print_queries)
        if should_commit_at_end:
            conn.commit()
        return result
    except Exception:
        time_print(f"SQL failed on gp:\n{statements[-1]}")
        raise
    finally:
        cursor.close()


def _execute_read_ch(
    client: Any,
    statements: list[str],
    print_queries: bool = False,
) -> pd.DataFrame:
    time_print(
        f"Executing {max(len(statements) - 1, 0)} setup statement(s) "
        "and reading final query on ch"
    )
    try:
        _execute_setup_statements(
            client,
            statements[:-1],
            connection_type="ch",
            execute_statement=_execute_ch_statement,
            print_queries=print_queries,
        )
        _maybe_print_query(statements[-1], print_queries, split_preview=True)
        return run_timed_query("ch", lambda: client.query_df(statements[-1]))
    except Exception:
        time_print(f"SQL failed on ch:\n{statements[-1]}")
        raise


def _execute_setup_statements(
    executor: Any,
    statements: list[str],
    *,
    connection_type: str,
    execute_statement: Any,
    print_queries: bool,
) -> None:
    for statement in _iterate_statements_with_progress(statements, connection_type):
        _maybe_print_query(statement, print_queries, split_preview=True)
        run_timed_query(
            connection_type,
            lambda statement=statement: execute_statement(executor, statement),
        )


def _read_dbapi_cursor(
    cursor: Any,
    query: str,
    connection_type: str,
    print_queries: bool,
) -> pd.DataFrame:
    time_print(f"Reading DataFrame from {connection_type}")
    _maybe_print_query(query, print_queries, split_preview=True)

    def read_query() -> pd.DataFrame:
        cursor.execute(query)
        columns = [column[0] for column in cursor.description or []]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)

    return run_timed_query(connection_type, read_query)


_EXECUTE_READ_FUNCTION_NAMES = {
    "trino": "_execute_read_trino",
    "gp": "_execute_read_gp",
    "ch": "_execute_read_ch",
}


def _execute_read_backend(
    backend: str,
    connection: Any,
    statements: list[str],
    *,
    print_queries: bool,
    gp_break_query: bool,
    gp_commit_each_statement: bool,
) -> pd.DataFrame:
    function_name = _EXECUTE_READ_FUNCTION_NAMES.get(backend)
    if function_name is None:
        raise UnsupportedConnectionTypeError(
            "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
        )
    if backend == "gp":
        return globals()[function_name](
            connection,
            statements,
            print_queries=print_queries,
            gp_break_query=gp_break_query,
            gp_commit_each_statement=gp_commit_each_statement,
        )
    return globals()[function_name](
        connection,
        statements,
        print_queries=print_queries,
    )
