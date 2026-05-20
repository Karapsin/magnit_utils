from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReadSqlOptions:
    connection_key: str
    backend: str
    sql: str
    print_queries: bool = False
    retry_cnt: int = 5
    timeout_increment: int | float = 5
    query_label: str | None = None
    return_metadata: bool = False


@dataclass(frozen=True)
class ExecuteSqlOptions:
    connection_key: str
    backend: str
    sql: str
    print_queries: bool = False
    gp_break_query: bool = False
    gp_commit_each_statement: bool = False
    retry_cnt: int = 5
    timeout_increment: int | float = 5
    query_label: str | None = None
    dry_run: bool = False
    return_sql: bool = False
    return_metadata: bool = False
    progress: bool = True


@dataclass(frozen=True)
class ExecuteReadOptions:
    connection_key: str
    backend: str
    statements: list[str]
    print_queries: bool = False
    gp_break_query: bool = False
    gp_commit_each_statement: bool = False
    retry_cnt: int = 5
    timeout_increment: int | float = 5
    query_label: str | None = None
    return_metadata: bool = False
    progress: bool = True
