from __future__ import annotations

import importlib
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

CURRENT_DT = date.today().strftime("%Y%m%d")
TEST_CH_TABLE = f"test_table_{CURRENT_DT}"
TEST_CH_SHARD_TABLE = f"test_table_{CURRENT_DT}_shard"
TEST_CH_STAGE_TABLE = f"test_table_{CURRENT_DT}__stage__abcd1234"
TEST_CH_SHARD_RELATION = f"test_table_{CURRENT_DT}_shard"

create_sql_table_module = importlib.import_module(
    "analytics_toolkit.sql.ddl.create_sql_table"
)
load_sql_table_module = importlib.import_module(
    "analytics_toolkit.sql.dml.load.load_sql_table"
)
load_df_module = importlib.import_module("analytics_toolkit.sql.dml.load.load_df")
table_ops_module = importlib.import_module("analytics_toolkit.sql.dml.table.table_ops")


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.commands: list[str] = []
        self.queries: list[str] = []
        self.close_calls = 0

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str) -> object:
        self.queries.append(sql)
        return type("FakeResult", (), {"result_rows": [(1,)]})()

    def insert_df(
        self,
        table: str,
        df: pd.DataFrame,
        column_names: list[str],
    ) -> None:
        self.calls.append(
            {
                "table": table,
                "df": df.copy(),
                "column_names": list(column_names),
            }
        )

    def close(self) -> None:
        self.close_calls += 1


def test_insert_table_batch_normalizes_decimal_for_clickhouse() -> None:
    client = FakeClickHouseClient()
    connection_ref = {"connection": client}
    batch = pd.DataFrame(
        {
            "amount": [Decimal("1.20"), None],
            "label": ["ok", None],
            "count": [1, 2],
        }
    )

    inserted_rows = load_sql_table_module.insert_table_batch(
        connection_type="ch",
        connection_ref=connection_ref,
        table_name="schema.stage_table",
        batch=batch,
        retry_fn=lambda **kwargs: kwargs["operation"](1),
        retry_cnt=1,
        timeout_increment=0,
    )

    assert inserted_rows == 2
    assert len(client.calls) == 1

    inserted_df = client.calls[0]["df"]
    assert isinstance(inserted_df, pd.DataFrame)
    assert inserted_df["amount"].tolist() == [1.2, None]
    assert inserted_df["label"].tolist() == ["ok", None]
    assert inserted_df["count"].tolist() == [1, 2]


def test_build_create_table_sql_uses_float64_for_decimal_clickhouse_columns() -> None:
    batch = pd.DataFrame(
        {
            "amount": [Decimal("1.20"), Decimal("2.50"), None],
            "label": ["ok", "still ok", None],
        }
    )

    sql = create_sql_table_module.build_create_table_sql(
        connection_type="ch",
        table_name="schema.stage_table",
        batch=batch,
    )

    assert "`amount` Nullable(Float64)" in sql
    assert "`label` Nullable(String)" in sql


def test_build_create_table_sqls_creates_clickhouse_distributed_pair() -> None:
    batch = pd.DataFrame(
        {
            "min_month_use": [date(2024, 1, 1)],
            "month_date": [date(2024, 2, 1)],
            "users": [10],
        }
    )

    sqls = create_sql_table_module.build_create_table_sqls(
        connection_type="ch",
        table_name=TEST_CH_TABLE,
        batch=batch,
        ch_distributed_table=True,
        ch_partition_by=["month_date"],
        ch_order_by=["month_date", "min_month_use"],
        ch_sharding_key="cityHash64(month_date, min_month_use)",
    )

    assert len(sqls) == 3
    shard_sql, distributed_sql, local_distributed_sql = sqls
    assert "SETTINGS index_granularity" not in "\n".join(sqls)
    assert shard_sql.startswith(
        f"CREATE TABLE IF NOT EXISTS {TEST_CH_SHARD_TABLE}"
    )
    assert "ON CLUSTER core" in shard_sql
    assert "ENGINE = ReplicatedMergeTree" in shard_sql
    assert "PARTITION BY `month_date`" in shard_sql
    assert "ORDER BY (`month_date`, `min_month_use`)" in shard_sql
    assert distributed_sql.startswith(
        f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}"
    )
    assert f"AS {TEST_CH_SHARD_TABLE}" not in distributed_sql
    assert "`min_month_use` Date" in distributed_sql
    assert "`month_date` Date" in distributed_sql
    assert "ENGINE = Distributed(" in distributed_sql
    assert "    'core'," in distributed_sql
    assert "    currentDatabase()," in distributed_sql
    assert f"    '{TEST_CH_SHARD_RELATION}'," in distributed_sql
    assert "    cityHash64(month_date, min_month_use)" in distributed_sql
    assert local_distributed_sql.startswith(
        f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}"
    )
    assert "ON CLUSTER" not in local_distributed_sql
    assert "ENGINE = Distributed(" in local_distributed_sql


def test_load_df_clickhouse_creates_pair_and_loads_distributed_table(monkeypatch) -> None:
    client = FakeClickHouseClient()
    batch = pd.DataFrame(
        {
            "month_date": [date(2024, 2, 1)],
            "min_month_use": [date(2024, 1, 1)],
            "users": [10],
        }
    )

    monkeypatch.setattr(
        load_df_module,
        "get_sql_connection",
        lambda connection_type: client,
    )
    monkeypatch.setattr(load_df_module, "table_exists", lambda *args, **kwargs: False)

    inserted_rows = load_df_module.load_df(
        "ch",
        TEST_CH_TABLE,
        batch,
        retry_cnt=1,
        timeout_increment=0,
        ch_partition_by=["month_date"],
        ch_order_by=["month_date", "min_month_use"],
        sharding_key="cityHash64(month_date, min_month_use)",
    )

    assert inserted_rows == 1
    assert f"DROP TABLE IF EXISTS {TEST_CH_TABLE}" in client.commands
    assert f"DROP TABLE IF EXISTS {TEST_CH_SHARD_TABLE}" in client.commands
    assert (
        f"DROP TABLE IF EXISTS {TEST_CH_TABLE} ON CLUSTER core"
        in client.commands
    )
    assert (
        f"DROP TABLE IF EXISTS {TEST_CH_SHARD_TABLE} ON CLUSTER core"
        in client.commands
    )
    assert "SETTINGS index_granularity" not in "\n".join(client.commands)
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_SHARD_TABLE}")
        for command in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}")
        and "ON CLUSTER core" in command
        for command in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}")
        and "ON CLUSTER" not in command
        for command in client.commands
    )
    assert client.calls[0]["table"] == TEST_CH_TABLE
    assert client.close_calls == 1


def test_finalize_stage_table_clickhouse_recreates_pair_and_inserts_target() -> None:
    client = FakeClickHouseClient()
    batch = pd.DataFrame(
        {
            "month_date": [date(2024, 2, 1)],
            "min_month_use": [date(2024, 1, 1)],
            "users": [10],
        }
    )

    table_ops_module.finalize_stage_table(
        connection_type="ch",
        connection=client,
        stage_table=TEST_CH_STAGE_TABLE,
        target_table=TEST_CH_TABLE,
        replace_target_table=True,
        target_exists=True,
        sample_batch=batch,
        ch_partition_by=["month_date"],
        ch_order_by=["month_date", "min_month_use"],
        ch_sharding_key="cityHash64(month_date, min_month_use)",
    )

    assert f"DROP TABLE IF EXISTS {TEST_CH_TABLE}" in client.commands
    assert f"DROP TABLE IF EXISTS {TEST_CH_SHARD_TABLE}" in client.commands
    assert (
        f"DROP TABLE IF EXISTS {TEST_CH_TABLE} ON CLUSTER core"
        in client.commands
    )
    assert (
        f"DROP TABLE IF EXISTS {TEST_CH_SHARD_TABLE} ON CLUSTER core"
        in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_SHARD_TABLE}")
        for command in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}")
        and "ON CLUSTER core" in command
        for command in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}")
        and "ON CLUSTER" not in command
        for command in client.commands
    )
    assert client.commands[-1] == (
        f"INSERT INTO {TEST_CH_TABLE} "
        f"SELECT * FROM {TEST_CH_STAGE_TABLE}"
    )
