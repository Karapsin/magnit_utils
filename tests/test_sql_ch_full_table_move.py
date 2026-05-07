from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ch_move_module = importlib.import_module(
    "analytics_toolkit.sql.dml.table.ch_full_table_move"
)
sql_module = importlib.import_module("analytics_toolkit.sql")


SOURCE_TABLE = "default.events_move"
SOURCE_SHARD_TABLE = "default.events_move_shard"
SOURCE_CUSTOM_SHARD_TABLE = "default.events_storage"
TARGET_TABLE = "default.events_target"
TARGET_SHARD_TABLE = "default.events_target_shard"

SOURCE_SHARD_DDL = f"""
CREATE TABLE {SOURCE_SHARD_TABLE}
ON CLUSTER core
UUID '11111111-1111-1111-1111-111111111111'
(
    `id` UInt64,
    `dt` Date,
    `amount` Decimal(18, 4),
    `name` LowCardinality(String)
)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, id)
SETTINGS index_granularity = 8192
""".strip()

SOURCE_DISTRIBUTED_DDL = f"""
CREATE TABLE {SOURCE_TABLE}
ON CLUSTER core
UUID '22222222-2222-2222-2222-222222222222'
(
    `id` UInt64,
    `dt` Date,
    `amount` Decimal(18, 4),
    `name` LowCardinality(String)
)
ENGINE = Distributed(
    'core',
    currentDatabase(),
    'events_move_shard',
    cityHash64(id)
)
""".strip()

SOURCE_SHARD_DDL_WITH_UUID_MACRO = f"""
CREATE TABLE {SOURCE_SHARD_TABLE}
UUID '11111111-1111-1111-1111-111111111111'
(
    `id` UInt64,
    `dt` Date,
    `amount` Decimal(18, 4),
    `name` LowCardinality(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{uuid}}/{{shard}}', '{{replica}}')
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, id)
""".strip()

SOURCE_DISTRIBUTED_DDL_WITHOUT_ON_CLUSTER = f"""
CREATE TABLE {SOURCE_TABLE}
UUID '22222222-2222-2222-2222-222222222222'
(
    `id` UInt64,
    `dt` Date,
    `amount` Decimal(18, 4),
    `name` LowCardinality(String)
)
ENGINE = Distributed(
    'core',
    currentDatabase(),
    'events_move_shard',
    cityHash64(id)
)
""".strip()

SOURCE_DISTRIBUTED_DDL_WITH_CLUSTER_MACRO = (
    SOURCE_DISTRIBUTED_DDL_WITHOUT_ON_CLUSTER.replace("'core'", "'{cluster}'")
)

SOURCE_CUSTOM_SHARD_DDL = SOURCE_SHARD_DDL.replace(
    SOURCE_SHARD_TABLE,
    SOURCE_CUSTOM_SHARD_TABLE,
)

SOURCE_DISTRIBUTED_DDL_WITH_CUSTOM_SHARD = SOURCE_DISTRIBUTED_DDL.replace(
    "'events_move_shard'",
    "'events_storage'",
)


class FakeClickHouseResult:
    def __init__(self, result_rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = result_rows


class FakeClickHouseClient:
    def __init__(
        self,
        source_shard_ddl: str = SOURCE_SHARD_DDL,
        source_distributed_ddl: str = SOURCE_DISTRIBUTED_DDL,
    ) -> None:
        self.source_shard_ddl = source_shard_ddl
        self.source_distributed_ddl = source_distributed_ddl
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
        if sql == f"SHOW CREATE TABLE {SOURCE_TABLE}":
            return FakeClickHouseResult([(self.source_distributed_ddl,)])
        if sql in {
            f"SHOW CREATE TABLE {SOURCE_SHARD_TABLE}",
            f"SHOW CREATE TABLE {SOURCE_CUSTOM_SHARD_TABLE}",
        }:
            return FakeClickHouseResult([(self.source_shard_ddl,)])
        if sql == f"EXISTS TABLE {TARGET_TABLE}":
            return FakeClickHouseResult([(1,)])
        raise AssertionError(f"Unexpected query: {sql}")

    def close(self) -> None:
        self.close_calls += 1


@pytest.fixture
def fake_client(monkeypatch: pytest.MonkeyPatch) -> FakeClickHouseClient:
    client = FakeClickHouseClient()
    monkeypatch.setattr(
        ch_move_module,
        "get_sql_connection",
        lambda connection_key: client,
    )
    return client


def _run_move(
    fake_client: FakeClickHouseClient,
    **kwargs: object,
) -> tuple[str, str, str]:
    ch_move_module.ch_full_table_move(
        "ch",
        SOURCE_TABLE,
        TARGET_TABLE,
        **kwargs,
    )
    return fake_client.commands[4], fake_client.commands[5], fake_client.commands[6]


def test_ch_full_table_move_is_exported() -> None:
    assert sql_module.ch_full_table_move is ch_move_module.ch_full_table_move


def test_ch_full_table_move_preserves_source_ddl_and_lifecycle(
    fake_client: FakeClickHouseClient,
) -> None:
    shard_create, distributed_create, local_distributed_create = _run_move(fake_client)

    assert fake_client.queries[:2] == [
        f"SHOW CREATE TABLE {SOURCE_TABLE}",
        f"SHOW CREATE TABLE {SOURCE_SHARD_TABLE}",
    ]
    assert fake_client.commands[:4] == [
        f"DROP TABLE IF EXISTS {TARGET_TABLE}",
        f"DROP TABLE IF EXISTS {TARGET_SHARD_TABLE}",
        f"DROP TABLE IF EXISTS {TARGET_TABLE} ON CLUSTER core",
        f"DROP TABLE IF EXISTS {TARGET_SHARD_TABLE} ON CLUSTER core",
    ]
    assert fake_client.commands[7] == (
        f"INSERT INTO {TARGET_TABLE} SELECT * FROM {SOURCE_TABLE}"
    )
    assert fake_client.commands[8:12] == [
        f"DROP TABLE IF EXISTS {SOURCE_TABLE}",
        f"DROP TABLE IF EXISTS {SOURCE_SHARD_TABLE}",
        f"DROP TABLE IF EXISTS {SOURCE_TABLE} ON CLUSTER core",
        f"DROP TABLE IF EXISTS {SOURCE_SHARD_TABLE} ON CLUSTER core",
    ]
    assert fake_client.close_calls == 1

    assert shard_create.startswith(
        f"CREATE TABLE IF NOT EXISTS {TARGET_SHARD_TABLE}"
    )
    assert f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE}" in distributed_create
    assert f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE}" in local_distributed_create
    assert "ON CLUSTER core" in shard_create
    assert "ON CLUSTER core" in distributed_create
    assert "ON CLUSTER" not in local_distributed_create
    assert "UUID" not in shard_create
    assert "UUID" not in distributed_create
    assert "`amount` Decimal(18, 4)" in shard_create
    assert "`name` LowCardinality(String)" in distributed_create
    assert "ENGINE = ReplicatedMergeTree" in shard_create
    assert "PARTITION BY toYYYYMM(dt)" in shard_create
    assert "ORDER BY (dt, id)" in shard_create
    assert "SETTINGS index_granularity = 8192" in shard_create
    assert "'core'" in distributed_create
    assert "'events_target_shard'" in distributed_create
    assert "'events_move_shard'" not in distributed_create
    assert "cityHash64(id)" in distributed_create


def test_ch_full_table_move_partition_override_replaces_partition_only(
    fake_client: FakeClickHouseClient,
) -> None:
    shard_create, distributed_create, _ = _run_move(
        fake_client,
        ch_partition_by=["dt"],
    )

    assert "PARTITION BY `dt`" in shard_create
    assert "ORDER BY (dt, id)" in shard_create
    assert "ENGINE = ReplicatedMergeTree" in shard_create
    assert "SETTINGS index_granularity = 8192" in shard_create
    assert "cityHash64(id)" in distributed_create


def test_ch_full_table_move_empty_partition_sequence_removes_partition_clause(
    fake_client: FakeClickHouseClient,
) -> None:
    shard_create, _, _ = _run_move(fake_client, ch_partition_by=[])

    assert "PARTITION BY" not in shard_create
    assert "ORDER BY (dt, id)" in shard_create


def test_ch_full_table_move_order_override_replaces_order_only(
    fake_client: FakeClickHouseClient,
) -> None:
    shard_create, _, _ = _run_move(fake_client, ch_order_by="tuple()")

    assert "ORDER BY tuple()" in shard_create
    assert "PARTITION BY toYYYYMM(dt)" in shard_create
    assert "ENGINE = ReplicatedMergeTree" in shard_create


def test_ch_full_table_move_engine_override_replaces_shard_engine_only(
    fake_client: FakeClickHouseClient,
) -> None:
    shard_create, distributed_create, _ = _run_move(
        fake_client,
        ch_engine="MergeTree",
    )

    assert "ENGINE = MergeTree" in shard_create
    assert "ReplicatedMergeTree" not in shard_create
    assert "ENGINE = Distributed(" in distributed_create


def test_ch_full_table_move_cluster_override_updates_target_only(
    fake_client: FakeClickHouseClient,
) -> None:
    _, distributed_create, _ = _run_move(fake_client, ch_cluster="analytics")

    assert fake_client.commands[:4] == [
        f"DROP TABLE IF EXISTS {TARGET_TABLE}",
        f"DROP TABLE IF EXISTS {TARGET_SHARD_TABLE}",
        f"DROP TABLE IF EXISTS {TARGET_TABLE} ON CLUSTER analytics",
        f"DROP TABLE IF EXISTS {TARGET_SHARD_TABLE} ON CLUSTER analytics",
    ]
    assert "ON CLUSTER analytics" in fake_client.commands[4]
    assert "ON CLUSTER analytics" in distributed_create
    assert "'analytics'" in distributed_create
    assert fake_client.commands[8:12] == [
        f"DROP TABLE IF EXISTS {SOURCE_TABLE}",
        f"DROP TABLE IF EXISTS {SOURCE_SHARD_TABLE}",
        f"DROP TABLE IF EXISTS {SOURCE_TABLE} ON CLUSTER core",
        f"DROP TABLE IF EXISTS {SOURCE_SHARD_TABLE} ON CLUSTER core",
    ]


def test_ch_full_table_move_sharding_key_override_replaces_final_distributed_arg(
    fake_client: FakeClickHouseClient,
) -> None:
    _, distributed_create, _ = _run_move(
        fake_client,
        sharding_key="sipHash64(id)",
    )

    assert "sipHash64(id)" in distributed_create
    assert "cityHash64(id)" not in distributed_create


def test_ch_full_table_move_adds_inferred_cluster_for_uuid_macro_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = FakeClickHouseClient(
        source_shard_ddl=SOURCE_SHARD_DDL_WITH_UUID_MACRO,
        source_distributed_ddl=SOURCE_DISTRIBUTED_DDL_WITHOUT_ON_CLUSTER,
    )
    monkeypatch.setattr(
        ch_move_module,
        "get_sql_connection",
        lambda connection_key: fake_client,
    )

    shard_create, distributed_create, local_distributed_create = _run_move(fake_client)

    assert "ON CLUSTER core" in shard_create
    assert "ON CLUSTER core" in distributed_create
    assert "ON CLUSTER" not in local_distributed_create
    assert "{uuid}" in shard_create
    assert "\nUUID " not in shard_create
    assert "'core'" in distributed_create


def test_ch_full_table_move_uses_shard_name_from_distributed_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = FakeClickHouseClient(
        source_shard_ddl=SOURCE_CUSTOM_SHARD_DDL,
        source_distributed_ddl=SOURCE_DISTRIBUTED_DDL_WITH_CUSTOM_SHARD,
    )
    monkeypatch.setattr(
        ch_move_module,
        "get_sql_connection",
        lambda connection_key: fake_client,
    )

    shard_create, distributed_create, _ = _run_move(fake_client)

    assert fake_client.queries[:2] == [
        f"SHOW CREATE TABLE {SOURCE_TABLE}",
        f"SHOW CREATE TABLE {SOURCE_CUSTOM_SHARD_TABLE}",
    ]
    assert fake_client.commands[8:12] == [
        f"DROP TABLE IF EXISTS {SOURCE_TABLE}",
        f"DROP TABLE IF EXISTS {SOURCE_CUSTOM_SHARD_TABLE}",
        f"DROP TABLE IF EXISTS {SOURCE_TABLE} ON CLUSTER core",
        f"DROP TABLE IF EXISTS {SOURCE_CUSTOM_SHARD_TABLE} ON CLUSTER core",
    ]
    assert f"CREATE TABLE IF NOT EXISTS {TARGET_SHARD_TABLE}" in shard_create
    assert "'events_target_shard'" in distributed_create


def test_ch_full_table_move_quotes_cluster_macro_for_on_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = FakeClickHouseClient(
        source_shard_ddl=SOURCE_SHARD_DDL_WITH_UUID_MACRO,
        source_distributed_ddl=SOURCE_DISTRIBUTED_DDL_WITH_CLUSTER_MACRO,
    )
    monkeypatch.setattr(
        ch_move_module,
        "get_sql_connection",
        lambda connection_key: fake_client,
    )

    shard_create, distributed_create, _ = _run_move(fake_client)

    assert f"DROP TABLE IF EXISTS {TARGET_TABLE} ON CLUSTER '{{cluster}}'" in (
        fake_client.commands
    )
    assert f"DROP TABLE IF EXISTS {SOURCE_TABLE} ON CLUSTER '{{cluster}}'" in (
        fake_client.commands
    )
    assert "ON CLUSTER '{cluster}'" in shard_create
    assert "ON CLUSTER '{cluster}'" in distributed_create
    assert "Distributed(\n    '{cluster}'," in distributed_create


def test_ch_full_table_move_rejects_non_clickhouse_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ch_move_module,
        "get_sql_connection",
        lambda connection_key: pytest.fail("connection should not be opened"),
    )

    with pytest.raises(
        ch_move_module.UnsupportedConnectionTypeError,
        match="requires a ch",
    ):
        ch_move_module.ch_full_table_move("gp", SOURCE_TABLE, TARGET_TABLE)
