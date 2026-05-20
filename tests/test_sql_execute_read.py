from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

execute_read_module = importlib.import_module(
    "analytics_toolkit.sql.dml.io.execute_read"
)
sql_module = importlib.import_module("analytics_toolkit.sql")


class FakeClickHouseClient:
    def __init__(self, result: pd.DataFrame | None = None) -> None:
        self.commands: list[str] = []
        self.read_queries: list[str] = []
        self.close_calls = 0
        self.result = result if result is not None else pd.DataFrame({"value": [1]})

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query_df(self, sql: str) -> pd.DataFrame:
        self.read_queries.append(sql)
        return self.result.copy()

    def close(self) -> None:
        self.close_calls += 1


class FakeCursor:
    def __init__(self, connection: FakeDbapiConnection) -> None:
        self.connection = connection
        self.description: list[tuple[str, ...]] | None = None
        self.close_calls = 0

    def execute(self, sql: str) -> None:
        self.connection.executed.append(sql)
        if sql.lower().startswith("select"):
            self.description = [("id",), ("label",)]
        else:
            self.description = None

    def fetchall(self) -> list[tuple[int, str]]:
        return [(1, "ok")]

    def close(self) -> None:
        self.close_calls += 1


class FakeDbapiConnection:
    def __init__(self, name: str = "conn") -> None:
        self.name = name
        self.executed: list[str] = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0
        self.cursor_obj = FakeCursor(self)

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


def test_execute_read_is_exported() -> None:
    assert sql_module.execute_read is execute_read_module.execute_read


def test_execute_read_clickhouse_executes_setup_and_reads_last(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = pd.DataFrame({"value": [10]})
    client = FakeClickHouseClient(expected)
    monkeypatch.setattr(
        execute_read_module,
        "get_sql_connection",
        lambda connection_key: client,
    )

    result = execute_read_module.execute_read(
        "ch",
        """
        CREATE TEMPORARY TABLE tmp AS SELECT 1 AS value;
        SELECT value FROM tmp
        """,
        print_queries=False,
    )

    pd.testing.assert_frame_equal(result, expected)
    assert client.commands == ["CREATE TEMPORARY TABLE tmp AS SELECT 1 AS value"]
    assert client.read_queries == ["SELECT value FROM tmp"]
    assert client.close_calls == 1


def test_execute_read_gp_executes_setup_statement_set_and_reads_last(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeDbapiConnection()
    monkeypatch.setattr(
        execute_read_module,
        "get_sql_connection",
        lambda connection_key: connection,
    )

    result = execute_read_module.execute_read(
        "gp",
        """
        CREATE TEMP TABLE tmp AS SELECT 1 AS id, 'ok' AS label;
        INSERT INTO tmp SELECT 2, 'still ok';
        SELECT id, label FROM tmp
        """,
        print_queries=False,
    )

    assert connection.executed == [
        "CREATE TEMP TABLE tmp AS SELECT 1 AS id, 'ok' AS label;\n"
        "INSERT INTO tmp SELECT 2, 'still ok'",
        "SELECT id, label FROM tmp",
    ]
    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame({"id": [1], "label": ["ok"]}),
    )
    assert connection.commit_calls == 1
    assert connection.close_calls == 1
    assert connection.cursor_obj.close_calls == 1


def test_execute_read_gp_break_query_executes_setup_statements_separately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeDbapiConnection()
    monkeypatch.setattr(
        execute_read_module,
        "get_sql_connection",
        lambda connection_key: connection,
    )

    execute_read_module.execute_read(
        "gp",
        "CREATE TEMP TABLE tmp AS SELECT 1; "
        "INSERT INTO tmp SELECT 2; "
        "SELECT * FROM tmp",
        print_queries=False,
        gp_break_query=True,
        gp_commit_each_statement=True,
    )

    assert connection.executed == [
        "CREATE TEMP TABLE tmp AS SELECT 1",
        "INSERT INTO tmp SELECT 2",
        "SELECT * FROM tmp",
    ]
    assert connection.commit_calls == 2


def test_execute_read_logs_elapsed_for_setup_and_final_query_by_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    connection = FakeDbapiConnection()
    monkeypatch.setattr(
        execute_read_module,
        "get_sql_connection",
        lambda connection_key: connection,
    )

    execute_read_module.execute_read(
        "gp",
        "CREATE TEMP TABLE tmp AS SELECT 1; SELECT * FROM tmp",
        gp_break_query=True,
        retry_cnt=1,
        timeout_increment=0,
    )

    output = capsys.readouterr().out
    assert "Executing query:" not in output
    assert output.count("SQL query on gp finished: success in ") == 2


def test_execute_read_retries_with_fresh_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_connection = FakeClickHouseClient()
    second_connection = FakeClickHouseClient(pd.DataFrame({"value": [2]}))
    connections = [first_connection, second_connection]
    attempts: list[FakeClickHouseClient] = []
    print_flags: list[bool] = []

    monkeypatch.setattr(
        execute_read_module,
        "get_sql_connection",
        lambda connection_key: connections.pop(0),
    )

    def fake_execute_read_ch(
        client: FakeClickHouseClient,
        statements: list[str],
        print_queries: bool = True,
    ) -> pd.DataFrame:
        attempts.append(client)
        print_flags.append(print_queries)
        if client is first_connection:
            raise RuntimeError("temporary failure")
        return client.result

    monkeypatch.setattr(
        execute_read_module,
        "_execute_read_ch",
        fake_execute_read_ch,
    )

    result = execute_read_module.execute_read(
        "ch",
        "SELECT 1",
        retry_cnt=2,
        timeout_increment=0,
    )

    pd.testing.assert_frame_equal(result, pd.DataFrame({"value": [2]}))
    assert attempts == [first_connection, second_connection]
    assert print_flags == [False, False]
    assert first_connection.close_calls == 1
    assert second_connection.close_calls == 1


def test_execute_read_rejects_empty_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        execute_read_module,
        "get_sql_connection",
        lambda connection_key: pytest.fail("connection should not be opened"),
    )

    with pytest.raises(execute_read_module.InvalidSqlInputError):
        execute_read_module.execute_read("ch", "   ")
