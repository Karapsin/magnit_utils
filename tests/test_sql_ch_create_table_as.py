from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ch_ctas_module = importlib.import_module(
    "analytics_toolkit.sql.dml.table.ch_create_table_as"
)
sql_module = importlib.import_module("analytics_toolkit.sql")


TARGET_TABLE = "default.events_result"
TARGET_SHARD_TABLE = "default.events_result_shard"
QUERY = """
SELECT
    dt,
    id,
    amount
FROM default.events_source
WHERE amount > 0
""".strip()


class FakeClickHouseResult:
    def __init__(self, result_rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = result_rows


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.command_settings: list[dict[str, object] | None] = []
        self.queries: list[str] = []
        self.close_calls = 0

    def command(
        self,
        sql: str,
        settings: dict[str, object] | None = None,
    ) -> None:
        self.commands.append(sql)
        self.command_settings.append(settings)

    def query(self, sql: str) -> FakeClickHouseResult:
        self.queries.append(sql)
        if sql == f"EXISTS TABLE {TARGET_TABLE}":
            return FakeClickHouseResult([(1,)])
        raise AssertionError(f"Unexpected query: {sql}")

    def close(self) -> None:
        self.close_calls += 1


@pytest.fixture
def fake_client(monkeypatch: pytest.MonkeyPatch) -> FakeClickHouseClient:
    client = FakeClickHouseClient()
    monkeypatch.setattr(
        ch_ctas_module,
        "get_sql_connection",
        lambda connection_key: client,
    )
    return client


def test_ch_create_table_as_is_exported() -> None:
    assert sql_module.ch_create_table_as is ch_ctas_module.ch_create_table_as


def test_ch_create_table_as_creates_pair_and_inserts_query(
    fake_client: FakeClickHouseClient,
) -> None:
    ch_ctas_module.ch_create_table_as(
        "ch",
        TARGET_TABLE,
        QUERY + ";",
        ch_partition_by=["dt"],
        ch_order_by=["dt", "id"],
        sharding_key="cityHash64(dt, id)",
    )

    assert fake_client.commands[:4] == [
        f"DROP TABLE IF EXISTS {TARGET_TABLE}",
        f"DROP TABLE IF EXISTS {TARGET_SHARD_TABLE}",
        f"DROP TABLE IF EXISTS {TARGET_TABLE} ON CLUSTER core",
        f"DROP TABLE IF EXISTS {TARGET_SHARD_TABLE} ON CLUSTER core",
    ]
    shard_sql, distributed_sql, local_distributed_sql = fake_client.commands[4:7]
    assert shard_sql.startswith(f"CREATE TABLE IF NOT EXISTS {TARGET_SHARD_TABLE}")
    assert "ON CLUSTER core" in shard_sql
    assert "ENGINE = ReplicatedMergeTree" in shard_sql
    assert "PARTITION BY `dt`" in shard_sql
    assert "ORDER BY (`dt`, `id`)" in shard_sql
    assert "FROM (\n" + QUERY + "\n)" in shard_sql
    assert "LIMIT 0" in shard_sql
    assert distributed_sql.startswith(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE}")
    assert "ON CLUSTER core" in distributed_sql
    assert "ENGINE = Distributed(" in distributed_sql
    assert "    'core'," in distributed_sql
    assert "    'default'," in distributed_sql
    assert "    'events_result_shard'," in distributed_sql
    assert "    cityHash64(dt, id)" in distributed_sql
    assert f"SELECT * FROM {TARGET_SHARD_TABLE} LIMIT 0" in distributed_sql
    assert local_distributed_sql.startswith(
        f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE}"
    )
    assert "ON CLUSTER" not in local_distributed_sql
    assert fake_client.commands[7] == f"INSERT INTO {TARGET_TABLE}\n{QUERY}"
    assert fake_client.queries == [f"EXISTS TABLE {TARGET_TABLE}"]
    assert fake_client.close_calls == 1


def test_ch_create_table_as_quotes_cluster_macro(
    fake_client: FakeClickHouseClient,
) -> None:
    ch_ctas_module.ch_create_table_as(
        "ch",
        TARGET_TABLE,
        QUERY,
        ch_cluster="{cluster}",
    )

    assert f"DROP TABLE IF EXISTS {TARGET_TABLE} ON CLUSTER '{{cluster}}'" in (
        fake_client.commands
    )
    shard_sql, distributed_sql, _ = fake_client.commands[4:7]
    assert "ON CLUSTER '{cluster}'" in shard_sql
    assert "ON CLUSTER '{cluster}'" in distributed_sql
    assert "    '{cluster}'," in distributed_sql


def test_ch_create_table_as_rejects_multiple_statements(
    fake_client: FakeClickHouseClient,
) -> None:
    with pytest.raises(
        ch_ctas_module.InvalidSqlInputError,
        match="exactly one SQL statement",
    ):
        ch_ctas_module.ch_create_table_as(
            "ch",
            TARGET_TABLE,
            "SELECT 1; SELECT 2",
        )

    assert fake_client.commands == []
    assert fake_client.close_calls == 0


def test_ch_create_table_as_rejects_non_clickhouse_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ch_ctas_module,
        "get_sql_connection",
        lambda connection_key: pytest.fail("connection should not be opened"),
    )

    with pytest.raises(
        ch_ctas_module.UnsupportedConnectionTypeError,
        match="requires a ch",
    ):
        ch_ctas_module.ch_create_table_as("gp", TARGET_TABLE, QUERY)
