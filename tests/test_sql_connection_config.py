from __future__ import annotations

import importlib
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest


config_module = importlib.import_module("analytics_toolkit.sql.connection.config")
api_module = importlib.import_module("analytics_toolkit.sql.dml.transfer.flow.api")
create_sql_table_module = importlib.import_module(
    "analytics_toolkit.sql.ddl.create_sql_table"
)
load_sql_table_module = importlib.import_module(
    "analytics_toolkit.sql.dml.load.load_sql_table"
)


def test_connection_alias_resolves_backend() -> None:
    config = config_module.get_connection_config("gp_sandbox")

    assert config.connection_key == "gp_sandbox"
    assert config.backend == "gp"
    assert config.database == "sandbox"


def test_unknown_connection_key_raises_config_error() -> None:
    with pytest.raises(config_module.UnsupportedConnectionTypeError):
        config_module.get_connection_config("missing")


def test_malformed_connections_file_raises_config_error(tmp_path: Path) -> None:
    (tmp_path / ".connections").write_text("{not json", encoding="utf-8")

    with pytest.raises(config_module.SqlConfigError):
        config_module.get_connection_config("gp")


def test_missing_connections_file_ignores_legacy_backend_env_vars(
    monkeypatch,
    tmp_path: Path,
) -> None:
    (tmp_path / ".connections").unlink()
    monkeypatch.setenv("GP_HOST", "legacy-host")
    monkeypatch.setenv("GP_USER", "legacy-user")
    monkeypatch.setenv("GP_PASSWORD", "legacy-password")
    monkeypatch.setenv("GP_DATABASE", "legacy-db")
    monkeypatch.setenv(
        "SQL_CONNECTIONS",
        (
            '{"gp":{"type":"gp","host":"legacy","user":"legacy",'
            '"password":"legacy","database":"legacy"}}'
        ),
    )

    with pytest.raises(config_module.SqlConfigError, match=".connections"):
        config_module.get_connection_config("gp")


def test_transfer_options_allow_two_aliases_with_same_backend() -> None:
    options = api_module.build_transfer_options(
        from_db="gp",
        to_db="gp_sandbox",
        from_sql="select 1",
        to_table="schema.target",
    )

    assert options.from_db_key == "gp"
    assert options.from_db_backend == "gp"
    assert options.to_db_key == "gp_sandbox"
    assert options.to_db_backend == "gp"


def test_backend_specific_validation_uses_alias_backend(
    write_sql_connections: Callable[[dict[str, dict[str, object]]], Path],
) -> None:
    write_sql_connections(
        {
            "target_gp": {
                "type": "gp",
                "host": "gp.example",
                "user": "user",
                "password": "password",
                "database": "db",
            },
            "source_trino": {
                "type": "trino",
                "host": "trino.example",
                "user": "user",
            },
        }
    )

    options = api_module.build_transfer_options(
        from_db="source_trino",
        to_db="target_gp",
        from_sql="select 1",
        to_table="schema.target",
        gp_distributed_by_key=["id"],
    )

    assert options.to_db_key == "target_gp"
    assert options.to_db_backend == "gp"
    assert options.gp_distributed_by_key == ["id"]


def test_create_table_sql_accepts_connection_alias() -> None:
    sql = create_sql_table_module.build_create_table_sql(
        connection_type="gp_sandbox",
        table_name="schema.target",
        batch=pd.DataFrame({"id": [1], "value": ["x"]}),
        gp_distributed_by_key=["id"],
    )

    assert '"id" BIGINT' in sql
    assert 'DISTRIBUTED BY ("id")' in sql


def test_trino_insert_chunk_size_comes_from_connection_config(
    write_sql_connections: Callable[[dict[str, dict[str, object]]], Path],
) -> None:
    write_sql_connections(
        {
            "trino_batch": {
                "type": "trino",
                "host": "trino.example",
                "user": "user",
                "insert_chunk_size": 250,
            }
        }
    )

    config = config_module.get_connection_config("trino_batch")

    assert config.insert_chunk_size == 250
    assert (
        load_sql_table_module._get_trino_insert_chunk_size(None, "trino_batch")
        == 250
    )


def test_legacy_trino_insert_chunk_size_env_is_ignored(monkeypatch) -> None:
    monkeypatch.setenv("TRINO_INSERT_CHUNK_SIZE", "2")

    assert (
        load_sql_table_module._get_trino_insert_chunk_size(None, "trino")
        == load_sql_table_module.DEFAULT_TRINO_INSERT_CHUNK_SIZE
    )
