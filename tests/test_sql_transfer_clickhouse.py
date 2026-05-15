from __future__ import annotations

import importlib
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

transfer_api_module = importlib.import_module(
    "analytics_toolkit.sql.dml.transfer.flow.api"
)
transfer_attempt_module = importlib.import_module(
    "analytics_toolkit.sql.dml.transfer.flow.attempt"
)


TARGET_TABLE = "test_transfer_target"
TARGET_SHARD_TABLE = "test_transfer_target_shard"


class FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class FakeSourceCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self.description = [
            ("month_date", 1082, None, None, None, None),
            ("users", 20, None, None, None, None),
        ]
        self.executed_queries: list[str] = []
        self.close_calls = 0

    def execute(self, query: str) -> None:
        self.executed_queries.append(query)

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        batch = self._rows[:size]
        self._rows = self._rows[size:]
        return batch

    def close(self) -> None:
        self.close_calls += 1


class FakeSourceConnection:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self.cursors: list[FakeSourceCursor] = []
        self.close_calls = 0

    def cursor(self) -> FakeSourceCursor:
        cursor = FakeSourceCursor(self._rows.copy())
        self.cursors.append(cursor)
        return cursor

    def close(self) -> None:
        self.close_calls += 1


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.command_settings: list[dict[str, object] | None] = []
        self.queries: list[str] = []
        self.inserts: list[dict[str, object]] = []
        self.created_tables: set[str] = set()
        self.close_calls = 0

    def command(
        self,
        sql: str,
        settings: dict[str, object] | None = None,
    ) -> None:
        self.commands.append(sql)
        self.command_settings.append(settings)
        self._track_table_ddl(sql)

    def query(self, sql: str) -> FakeResult:
        self.queries.append(sql)
        if sql.startswith("EXISTS TABLE "):
            table_name = sql.removeprefix("EXISTS TABLE ").strip()
            return FakeResult([(int(table_name in self.created_tables),)])
        return FakeResult([])

    def insert_df(
        self,
        table: str,
        df: pd.DataFrame,
        column_names: list[str],
    ) -> None:
        self.inserts.append(
            {
                "table": table,
                "df": df.copy(),
                "column_names": list(column_names),
            }
        )

    def insert(
        self,
        table: str,
        data: list[tuple[Any, ...]],
        column_names: list[str],
        column_type_names: list[str] | None = None,
    ) -> None:
        self.inserts.append(
            {
                "table": table,
                "data": list(data),
                "column_names": list(column_names),
                "column_type_names": (
                    list(column_type_names)
                    if column_type_names is not None
                    else None
                ),
            }
        )

    def close(self) -> None:
        self.close_calls += 1

    def _track_table_ddl(self, sql: str) -> None:
        if sql.startswith("CREATE TABLE IF NOT EXISTS "):
            table_name = sql.removeprefix("CREATE TABLE IF NOT EXISTS ").split()[0]
            self.created_tables.add(table_name)
            return
        if sql.startswith("CREATE TABLE "):
            table_name = sql.removeprefix("CREATE TABLE ").split()[0]
            self.created_tables.add(table_name)
            return
        if sql.startswith("DROP TABLE IF EXISTS "):
            table_name = sql.removeprefix("DROP TABLE IF EXISTS ").split()[0]
            self.created_tables.discard(table_name)


def test_transfer_table_clickhouse_target_creates_distributed_table_on_cluster(
    monkeypatch,
) -> None:
    source = FakeSourceConnection(rows=[(date(2024, 2, 1), 10)])
    target = FakeClickHouseClient()

    def fake_get_sql_connection(connection_key: str) -> object:
        if connection_key == "gp":
            return source
        if connection_key == "ch":
            return target
        raise AssertionError(f"Unexpected connection key: {connection_key}")

    monkeypatch.setattr(
        transfer_attempt_module,
        "get_sql_connection",
        fake_get_sql_connection,
    )

    transferred_rows = transfer_api_module.transfer_table(
        from_db="gp",
        to_db="ch",
        from_sql="select month_date, users from source_table",
        to_table=TARGET_TABLE,
        retry_cnt=1,
        timeout_increment=0,
        full_retry_cnt=1,
        full_timeout_increment=0,
        ch_partition_by=["month_date"],
        ch_order_by=["month_date"],
        sharding_key="cityHash64(month_date)",
    )

    assert transferred_rows == 1
    assert target.inserts[0]["table"].startswith("test_transfer_target__stage__")
    assert target.inserts[0]["data"] == [(date(2024, 2, 1), 10)]
    assert target.inserts[0]["column_names"] == ["month_date", "users"]
    assert target.inserts[0]["column_type_names"] == [
        "Nullable(Date)",
        "Nullable(Int64)",
    ]
    assert "df" not in target.inserts[0]

    cluster_distributed_creates = [
        command
        for command in target.commands
        if command.startswith(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE}\n")
        and "ON CLUSTER '{cluster}'" in command
    ]
    assert len(cluster_distributed_creates) == 1
    assert "ENGINE = Distributed(" in cluster_distributed_creates[0]
    assert "`month_date` Nullable(Date)" in cluster_distributed_creates[0]
    assert "`users` Nullable(Int64)" in cluster_distributed_creates[0]
    assert "    '{cluster}'," in cluster_distributed_creates[0]
    assert f"    '{TARGET_SHARD_TABLE}'," in cluster_distributed_creates[0]
    assert any(
        command.startswith(
            f"INSERT INTO {TARGET_TABLE} (`month_date`, `users`) "
            "SELECT CAST(`month_date` AS Nullable(Date)) AS `month_date`, "
            "CAST(`users` AS Nullable(Int64)) AS `users` "
            "FROM test_transfer_target__stage__"
        )
        for command in target.commands
    )

    assert (
        f"DROP TABLE IF EXISTS {TARGET_TABLE} ON CLUSTER '{{cluster}}'"
        in target.commands
    )
    assert (
        f"DROP TABLE IF EXISTS {TARGET_SHARD_TABLE} ON CLUSTER '{{cluster}}'"
        in target.commands
    )
