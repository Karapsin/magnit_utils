from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd
import pytest

from tests.sql_fakes import FakeDbapiConnection


capabilities_module = importlib.import_module("analytics_toolkit.sql.capabilities")
identifiers_module = importlib.import_module("analytics_toolkit.sql.identifiers")
config_module = importlib.import_module("analytics_toolkit.sql.connection.config")
load_df_module = importlib.import_module("analytics_toolkit.sql.dml.load.load_df")
read_sql_module = importlib.import_module("analytics_toolkit.sql.dml.io.read_sql")
transfer_api_module = importlib.import_module(
    "analytics_toolkit.sql.dml.transfer.flow.api"
)
create_table_module = importlib.import_module(
    "analytics_toolkit.sql.dml.table.create_table_from_sql"
)
ch_ctas_module = importlib.import_module(
    "analytics_toolkit.sql.dml.table.ch_create_table_as"
)
cli_module = importlib.import_module("analytics_toolkit.cli")


def test_backend_support_matrix_includes_write_modes() -> None:
    rows = capabilities_module.support_matrix_rows()

    assert {row["backend"] for row in rows} == {"gp", "trino", "ch"}
    gp_row = next(row for row in rows if row["backend"] == "gp")
    assert "truncate_insert" in gp_row["write_modes"]
    assert "upsert" not in gp_row["write_modes"]


def test_table_identifier_preserves_qualified_parts_and_quotes() -> None:
    identifier = identifiers_module.parse_table_identifier(
        'sandbox."Target Table"',
        "gp",
    )

    assert identifier.parts == ("sandbox", "Target Table")
    assert identifier.with_relation_suffix("_stage").render("gp") == (
        'sandbox."Target Table_stage"'
    )
    assert identifier.render_quoted("ch") == "`sandbox`.`Target Table`"


def test_load_df_dry_run_returns_ordered_labeled_plan() -> None:
    plan = load_df_module.load_df(
        "gp",
        "sandbox.scores",
        pd.DataFrame({"user_id": [1], "score": [10]}),
        write_mode="truncate_insert",
        dry_run=True,
        query_label="daily scores",
        gp_insert_chunk_size=5000,
    )

    assert plan.operation == "load_df"
    assert plan.target_alias == "gp"
    assert plan.options["gp_insert_chunk_size"] == 5000
    assert [statement.phase for statement in plan.statements] == [
        "clear_target",
        "create_target",
        "load_data",
        "analyze",
        "count_target",
    ]
    assert plan.sqls[0].startswith("/* analytics_toolkit query_label=daily scores */")
    assert "TRUNCATE TABLE sandbox.scores" in plan.sqls[0]


def test_load_df_return_metadata_preserves_rows_default_path(monkeypatch) -> None:
    connection = FakeDbapiConnection()
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})

    monkeypatch.setattr(load_df_module, "get_sql_connection", lambda key: connection)
    monkeypatch.setattr(load_df_module, "table_exists", lambda *args, **kwargs: False)
    monkeypatch.setattr(load_df_module, "create_sql_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(load_df_module, "insert_table_batch", lambda *args, **kwargs: 2)
    monkeypatch.setattr(load_df_module, "analyze_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(load_df_module, "count_table_rows", lambda *args, **kwargs: 5)

    result = load_df_module.load_df(
        "gp",
        "sandbox.target",
        df,
        retry_cnt=1,
        timeout_increment=0,
        return_metadata=True,
    )

    assert result.rows == 2
    assert result.metadata.source_rows == 2
    assert result.metadata.inserted_rows == 2
    assert result.metadata.final_target_rows == 5


def test_unsupported_upsert_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="does not support"):
        load_df_module.load_df(
            "gp",
            "sandbox.target",
            pd.DataFrame({"id": [1]}),
            write_mode="upsert",
            dry_run=True,
        )


def test_load_df_rejects_invalid_gp_insert_chunk_size() -> None:
    with pytest.raises(ValueError, match="gp_insert_chunk_size"):
        load_df_module.load_df(
            "gp",
            "sandbox.target",
            pd.DataFrame({"id": [1]}),
            gp_insert_chunk_size=0,
            dry_run=True,
        )

    with pytest.raises(ValueError, match="connection_type has type 'gp'"):
        load_df_module.load_df(
            "trino",
            "sandbox.target",
            pd.DataFrame({"id": [1]}),
            gp_insert_chunk_size=100,
            dry_run=True,
        )


def test_read_sql_prefixes_query_label(monkeypatch) -> None:
    connection = FakeDbapiConnection(
        rows=[(1,)],
        description=[("value",)],
    )
    monkeypatch.setattr(read_sql_module, "get_sql_connection", lambda key: connection)

    result = read_sql_module.read_sql(
        "gp",
        "select 1 as value",
        print_queries=False,
        retry_cnt=1,
        timeout_increment=0,
        query_label="unit-test",
    )

    assert result["value"].tolist() == [1]
    assert connection.executed[0].startswith(
        "/* analytics_toolkit query_label=unit-test */"
    )


def test_transfer_dry_run_includes_source_stage_and_target_steps() -> None:
    plan = transfer_api_module.transfer_table(
        from_db="gp",
        to_db="trino",
        from_sql="select id from source_table",
        to_table="sandbox.target",
        dry_run=True,
        query_label="copy-target",
    )

    assert plan.operation == "transfer_table"
    assert plan.source_alias == "gp"
    assert plan.target_alias == "trino"
    assert plan.options["adaptive_batch_size"] is True
    assert plan.options["min_batch_size"] == 1_000
    assert plan.options["max_batch_size"] == 400_000
    assert plan.options["target_batch_seconds"] == 10.0
    assert plan.statements[0].phase == "read_source"
    assert "query_label=copy-target" in plan.statements[0].sql
    assert plan.statements[-1].phase == "drop_stage"


def test_load_df_clickhouse_dry_run_preserves_lifecycle_order_and_cluster() -> None:
    plan = load_df_module.load_df(
        "ch",
        "analytics.events",
        pd.DataFrame({"dt": ["2024-01-01"], "id": [1]}),
        write_mode="truncate_insert",
        dry_run=True,
        ch_partition_by=["dt"],
        ch_order_by=["dt", "id"],
        ch_cluster="analytics",
    )

    assert plan.statements[0].phase == "clear_target"
    assert plan.sqls[0] == (
        "TRUNCATE TABLE IF EXISTS analytics.events_shard ON CLUSTER analytics"
    )
    assert plan.sqls[1] == "TRUNCATE TABLE IF EXISTS analytics.events"
    assert plan.statements[2].phase == "create_target"
    assert plan.sqls[2].startswith(
        "CREATE TABLE IF NOT EXISTS analytics.events_shard"
    )
    assert "ON CLUSTER analytics" in plan.sqls[2]


def test_transfer_clickhouse_dry_run_preserves_drop_pair_cluster() -> None:
    plan = transfer_api_module.transfer_table(
        from_db="gp",
        to_db="ch",
        from_sql="select id from source_table",
        to_table="analytics.events",
        dry_run=True,
        ch_cluster="analytics",
    )

    drop_sqls = [
        statement.sql
        for statement in plan.statements
        if statement.phase == "drop_target"
    ]
    assert drop_sqls == [
        "DROP TABLE IF EXISTS analytics.events",
        "DROP TABLE IF EXISTS analytics.events_shard",
        "DROP TABLE IF EXISTS analytics.events ON CLUSTER analytics",
        "DROP TABLE IF EXISTS analytics.events_shard ON CLUSTER analytics",
    ]


def test_create_table_from_sql_clickhouse_dry_run_uses_shared_plan_steps() -> None:
    plan = create_table_module.create_table_from_sql(
        "gp",
        "analytics.events",
        "select id from source_table",
        table_db="ch",
        drop_target_if_exists=True,
        insert_data=True,
        dry_run=True,
        ch_cluster="analytics",
    )

    assert [statement.phase for statement in plan.statements] == [
        "inspect_source_schema",
        "drop_target",
        "drop_target",
        "drop_target",
        "drop_target",
        "create_target",
        "insert_data",
    ]
    assert plan.sqls[3] == "DROP TABLE IF EXISTS analytics.events ON CLUSTER analytics"


def test_ch_create_table_as_dry_run_uses_lifecycle_drop_order() -> None:
    plan = ch_ctas_module.ch_create_table_as(
        "ch",
        "analytics.events",
        "select 1 as id",
        dry_run=True,
        ch_cluster="analytics",
    )

    assert plan.sqls[:4] == [
        "DROP TABLE IF EXISTS analytics.events",
        "DROP TABLE IF EXISTS analytics.events_shard",
        "DROP TABLE IF EXISTS analytics.events ON CLUSTER analytics",
        "DROP TABLE IF EXISTS analytics.events_shard ON CLUSTER analytics",
    ]


def test_validate_connections_and_cli_output(capsys) -> None:
    results = config_module.validate_connections(["gp", "missing"])

    assert results[0].valid is True
    assert results[0].backend == "gp"
    assert results[1].valid is False
    assert results[1].connection_key == "missing"

    exit_code = cli_module.main(["sql", "support-matrix"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Backend" in captured.out
    assert "trino" in captured.out


def test_cli_validate_reports_errors(capsys) -> None:
    exit_code = cli_module.main(["sql", "validate", "missing"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "ERROR missing" in captured.out
