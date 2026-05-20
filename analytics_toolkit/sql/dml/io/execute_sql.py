from __future__ import annotations

from typing import Any, Iterator

import sqlparse
from tqdm import tqdm

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
from ...plans import SqlOperationMetadata, SqlOperationResult, SqlPlan
from ...query_timing import run_timed_query
from analytics_toolkit.general import time_print
from .models import ExecuteSqlOptions


def _execute_trino(
    conn: Any,
    query: str,
    print_queries: bool = False,
    progress: bool = True,
) -> Any:
    cursor = conn.cursor()
    statements = _split_sql_statements(query)
    time_print(f"Executing {len(statements)} statement(s) on trino")
    statement: str | None = None
    try:
        for statement in _iterate_statements_with_progress(
            statements,
            "trino",
            progress=progress,
        ):
            _maybe_print_query(statement, print_queries, split_preview=True)
            run_timed_query(
                "trino",
                lambda statement=statement: _execute_trino_statement(
                    cursor,
                    statement,
                ),
            )
    except Exception:
        failed_query = statement if statement is not None else query
        time_print(f"SQL failed on trino:\n{failed_query}")
        raise
    return None


def _execute_gp(
    conn: Any,
    query: str,
    print_queries: bool = False,
    gp_break_query: bool = False,
    gp_commit_each_statement: bool = False,
    progress: bool = True,
) -> Any:
    statement: str | None = None
    try:
        with conn.cursor() as cursor:
            should_commit_at_end = True
            if not gp_break_query:
                time_print("Executing 1 statement set on gp")
                statement = query
                _maybe_print_query(statement, print_queries, split_preview=False)
                run_timed_query("gp", lambda: cursor.execute(statement))
            else:
                statements = _split_sql_statements(query)
                time_print(f"Executing {len(statements)} statement(s) on gp")
                for statement in _iterate_statements_with_progress(
                    statements,
                    "gp",
                    progress=progress,
                ):
                    _maybe_print_query(statement, print_queries, split_preview=True)
                    run_timed_query(
                        "gp",
                        lambda statement=statement: cursor.execute(statement),
                    )
                    if gp_commit_each_statement:
                        conn.commit()
                        should_commit_at_end = False
            if should_commit_at_end:
                conn.commit()
            return None
    except Exception:
        failed_query = statement if statement is not None else query
        time_print(f"SQL failed on gp:\n{failed_query}")
        raise


def _execute_ch(
    client: Any,
    query: str,
    print_queries: bool = False,
    progress: bool = True,
) -> Any:
    statements = _split_sql_statements(query)
    time_print(f"Executing {len(statements)} statement(s) on ch")
    statement: str | None = None
    try:
        for statement in _iterate_statements_with_progress(
            statements,
            "ch",
            progress=progress,
        ):
            _maybe_print_query(statement, print_queries, split_preview=True)
            run_timed_query(
                "ch",
                lambda statement=statement: _execute_ch_statement(client, statement),
            )
    except Exception:
        failed_query = statement if statement is not None else query
        time_print(f"SQL failed on ch:\n{failed_query}")
        raise
    return None


def execute_sql(
    connection_type: str,
    query: str,
    print_queries: bool = False,
    gp_break_query: bool = False,
    gp_commit_each_statement: bool = False,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    query_label: str | None = None,
    dry_run: bool = False,
    return_sql: bool = False,
    return_metadata: bool = False,
    progress: bool = True,
) -> Any:
    options = _build_execute_sql_options(
        connection_type=connection_type,
        query=query,
        print_queries=print_queries,
        gp_break_query=gp_break_query,
        gp_commit_each_statement=gp_commit_each_statement,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        query_label=query_label,
        dry_run=dry_run,
        return_sql=return_sql,
        return_metadata=return_metadata,
        progress=progress,
    )

    if options.dry_run or options.return_sql:
        return build_execute_sql_plan(options)

    statements = _planned_execute_statements(options)
    metadata = SqlOperationMetadata(
        statement_count=len(statements),
        query_label=options.query_label,
    )

    def operation(connection_ref: dict[str, Any], attempt: int) -> Any:
        with tracked_sql_operation(
            metadata=metadata,
            operation_name="execute_sql",
            alias=options.connection_key,
            backend=options.backend,
            phase="execute",
            retry_attempt=attempt,
            query_label=options.query_label,
            preview_sql=options.sql,
        ):
            result = _execute_backend(
                options.backend,
                connection_ref["connection"],
                options.sql,
                print_queries=options.print_queries,
                gp_break_query=options.gp_break_query,
                gp_commit_each_statement=options.gp_commit_each_statement,
                progress=options.progress,
            )
            metadata.affected_rows = None
            return result

    def context(attempt: int) -> SqlOperationContext:
        return SqlOperationContext(
            operation="execute_sql",
            alias=options.connection_key,
            backend=options.backend,
            phase="execute",
            retry_attempt=attempt,
            sql_preview=sql_preview(options.sql),
        )

    result = run_connection_operation(
        operation_name=f"executing SQL on {options.connection_key} ({options.backend})",
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
            rows=None,
            metadata=metadata,
        )
    return result


def _build_execute_sql_options(
    *,
    connection_type: str,
    query: str,
    print_queries: bool,
    gp_break_query: bool,
    gp_commit_each_statement: bool,
    retry_cnt: int,
    timeout_increment: int | float,
    query_label: str | None,
    dry_run: bool,
    return_sql: bool,
    return_metadata: bool,
    progress: bool,
) -> ExecuteSqlOptions:
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
    _validate_progress(progress)
    sql = apply_query_label(sql, query_label)
    return ExecuteSqlOptions(
        connection_key=connection_key,
        backend=backend,
        sql=sql,
        print_queries=print_queries,
        gp_break_query=gp_break_query,
        gp_commit_each_statement=gp_commit_each_statement,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        query_label=query_label,
        dry_run=dry_run,
        return_sql=return_sql,
        return_metadata=return_metadata,
        progress=progress,
    )


def build_execute_sql_plan(options: ExecuteSqlOptions) -> SqlPlan:
    statements = _planned_execute_statements(options)
    plan = SqlPlan(
        operation="execute_sql",
        target_alias=options.connection_key,
        target_backend=options.backend,
        options={
            "print_queries": options.print_queries,
            "gp_break_query": options.gp_break_query,
            "gp_commit_each_statement": options.gp_commit_each_statement,
        },
        metadata=SqlOperationMetadata(
            statement_count=len(statements),
            query_label=options.query_label,
        ),
    )
    for statement in statements:
        plan.add(
            statement,
            alias=options.connection_key,
            backend=options.backend,
            phase="execute",
        )
    return plan


def _planned_execute_statements(options: ExecuteSqlOptions) -> list[str]:
    if options.backend == "gp" and not options.gp_break_query:
        return [options.sql]
    return _split_sql_statements(options.sql)


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
    statements: list[str],
    connection_type: str,
    *,
    progress: bool = True,
) -> Iterator[str]:
    if len(statements) <= 1 or not progress:
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


def _validate_progress(progress: bool) -> None:
    if not isinstance(progress, bool):
        raise ValueError("progress must be a boolean.")


_EXECUTE_FUNCTION_NAMES = {
    "trino": "_execute_trino",
    "gp": "_execute_gp",
    "ch": "_execute_ch",
}


def _execute_backend(
    backend: str,
    connection: Any,
    sql: str,
    *,
    print_queries: bool,
    gp_break_query: bool,
    gp_commit_each_statement: bool,
    progress: bool,
) -> Any:
    function_name = _EXECUTE_FUNCTION_NAMES.get(backend)
    if function_name is None:
        raise UnsupportedConnectionTypeError(
            "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
        )
    if backend == "gp":
        return globals()[function_name](
            connection,
            sql,
            print_queries=print_queries,
            gp_break_query=gp_break_query,
            gp_commit_each_statement=gp_commit_each_statement,
            progress=progress,
        )
    return globals()[function_name](
        connection,
        sql,
        print_queries=print_queries,
        progress=progress,
    )
