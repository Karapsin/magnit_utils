from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

execute_sql_module = importlib.import_module("analytics_toolkit.sql.dml.io.execute_sql")
read_sql_module = importlib.import_module("analytics_toolkit.sql.dml.io.read_sql")
load_df_module = importlib.import_module("analytics_toolkit.sql.dml.load.load_df")
retry_module = importlib.import_module("analytics_toolkit.sql.dml.transfer.runtime.retry")


class FakeConnection:
    def __init__(self, name: str) -> None:
        self.name = name
        self.close_calls = 0
        self.rollback_calls = 0

    def close(self) -> None:
        self.close_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


class FakeUndefinedTableError(Exception):
    pgcode = "42P01"


class FakeTrinoSyntaxError(Exception):
    error_name = "SYNTAX_ERROR"


def test_read_sql_retries_whole_flow_with_fresh_gp_connection(monkeypatch) -> None:
    first_connection = FakeConnection("first")
    second_connection = FakeConnection("second")
    connections = [first_connection, second_connection]
    attempts: list[str] = []
    expected = pd.DataFrame({"value": [1]})

    monkeypatch.setattr(
        read_sql_module,
        "get_sql_connection",
        lambda connection_type: connections.pop(0),
    )

    def fake_read_gp(conn: FakeConnection, query: str, print_queries: bool = True) -> pd.DataFrame:
        attempts.append(conn.name)
        if conn.name == "first":
            raise RuntimeError("temporary failure")
        return expected

    monkeypatch.setattr(read_sql_module, "_read_gp", fake_read_gp)

    result = read_sql_module.read_sql(
        "gp",
        "select 1",
        retry_cnt=2,
        timeout_increment=0,
    )

    pd.testing.assert_frame_equal(result, expected)
    assert attempts == ["first", "second"]
    assert first_connection.rollback_calls == 1
    assert first_connection.close_calls == 1
    assert second_connection.close_calls == 1


def test_read_sql_does_not_retry_undefined_table(monkeypatch) -> None:
    first_connection = FakeConnection("first")
    second_connection = FakeConnection("second")
    connections = [first_connection, second_connection]
    attempts: list[str] = []

    monkeypatch.setattr(
        read_sql_module,
        "get_sql_connection",
        lambda connection_type: connections.pop(0),
    )

    def fake_read_gp(conn: FakeConnection, query: str, print_queries: bool = True) -> pd.DataFrame:
        attempts.append(conn.name)
        raise FakeUndefinedTableError('relation "missing_table" does not exist')

    monkeypatch.setattr(read_sql_module, "_read_gp", fake_read_gp)

    try:
        read_sql_module.read_sql(
            "gp",
            "select * from missing_table",
            retry_cnt=3,
            timeout_increment=0,
        )
    except FakeUndefinedTableError:
        pass
    else:
        raise AssertionError("Expected undefined-table error to be raised.")

    assert attempts == ["first"]
    assert len(connections) == 1
    assert first_connection.rollback_calls == 1
    assert first_connection.close_calls == 1
    assert second_connection.close_calls == 0


def test_execute_sql_retries_whole_flow_with_fresh_connection(monkeypatch) -> None:
    first_connection = FakeConnection("first")
    second_connection = FakeConnection("second")
    connections = [first_connection, second_connection]
    attempts: list[str] = []

    monkeypatch.setattr(
        execute_sql_module,
        "get_sql_connection",
        lambda connection_type: connections.pop(0),
    )

    def fake_execute_trino(
        conn: FakeConnection,
        query: str,
        random_sleep_seconds: float | None = 5,
        print_queries: bool = True,
    ) -> None:
        attempts.append(conn.name)
        if conn.name == "first":
            raise RuntimeError("temporary failure")

    monkeypatch.setattr(execute_sql_module, "_execute_trino", fake_execute_trino)

    execute_sql_module.execute_sql(
        "trino",
        "select 1; select 2",
        random_sleep_seconds=None,
        retry_cnt=2,
        timeout_increment=0,
    )

    assert attempts == ["first", "second"]
    assert first_connection.close_calls == 1
    assert second_connection.close_calls == 1


def test_run_with_retry_does_not_retry_trino_syntax_error() -> None:
    attempts: list[int] = []

    def operation(attempt: int) -> None:
        attempts.append(attempt)
        raise FakeTrinoSyntaxError("line 1:8: mismatched input 'fromm'")

    try:
        retry_module.run_with_retry(
            operation_name="executing SQL on trino",
            retry_cnt=3,
            timeout_increment=0,
            operation=operation,
        )
    except FakeTrinoSyntaxError:
        pass
    else:
        raise AssertionError("Expected syntax error to be raised.")

    assert attempts == [1]


def test_load_df_retries_whole_flow_from_start(monkeypatch) -> None:
    first_connection = FakeConnection("first")
    second_connection = FakeConnection("second")
    connections = [first_connection, second_connection]
    events: list[tuple[str, str]] = []
    call_count = {"insert": 0}
    df = pd.DataFrame({"id": [1], "value": ["x"]})

    monkeypatch.setattr(
        load_df_module,
        "get_sql_connection",
        lambda connection_type: connections.pop(0),
    )
    monkeypatch.setattr(load_df_module, "table_exists", lambda *args, **kwargs: False)

    def fake_create_sql_table(
        connection_type: str,
        connection: FakeConnection,
        table_name: str,
        batch: pd.DataFrame,
        gp_distributed_by_key: list[str] | None = None,
    ) -> None:
        events.append(("create", connection.name))

    def fake_insert_table_batch(*args, **kwargs) -> int:
        connection_ref = args[1]
        connection = connection_ref["connection"]
        events.append(("insert", connection.name))
        call_count["insert"] += 1
        if call_count["insert"] == 1:
            raise RuntimeError("temporary failure")
        return len(df)

    def fake_analyze_table(connection_type: str, connection: FakeConnection, table_name: str) -> None:
        events.append(("analyze", connection.name))

    monkeypatch.setattr(load_df_module, "create_sql_table", fake_create_sql_table)
    monkeypatch.setattr(load_df_module, "insert_table_batch", fake_insert_table_batch)
    monkeypatch.setattr(load_df_module, "analyze_table", fake_analyze_table)

    inserted_rows = load_df_module.load_df(
        "gp",
        "schema.target_table",
        df,
        retry_cnt=2,
        timeout_increment=0,
    )

    assert inserted_rows == 1
    assert events == [
        ("create", "first"),
        ("insert", "first"),
        ("create", "second"),
        ("insert", "second"),
        ("analyze", "second"),
    ]
    assert first_connection.rollback_calls == 1
    assert first_connection.close_calls == 1
    assert second_connection.close_calls == 1
