from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest


DEFAULT_SQL_CONNECTIONS = {
    "gp": {
        "type": "gp",
        "host": "gp.example",
        "port": 5432,
        "user": "user",
        "password": "password",
        "database": "db",
    },
    "gp_sandbox": {
        "type": "gp",
        "host": "gp-sandbox.example",
        "port": 5432,
        "user": "user",
        "password": "password",
        "database": "sandbox",
    },
    "trino": {
        "type": "trino",
        "host": "trino.example",
        "port": 8080,
        "user": "user",
        "password": "password",
        "catalog": "iceberg",
        "schema": "sandbox",
    },
    "ch": {
        "type": "ch",
        "host": "ch.example",
        "port": 8123,
        "user": "user",
        "password": "password",
        "database": "default",
    },
}


@pytest.fixture
def write_sql_connections(tmp_path: Path) -> Callable[[dict[str, dict[str, object]]], Path]:
    def write(connections: dict[str, dict[str, object]]) -> Path:
        connections_file = tmp_path / ".connections"
        connections_file.write_text(
            json.dumps(connections, indent=2),
            encoding="utf-8",
        )
        return connections_file

    return write


@pytest.fixture(autouse=True)
def default_sql_connections(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    write_sql_connections: Callable[[dict[str, dict[str, object]]], Path],
) -> None:
    monkeypatch.chdir(tmp_path)
    write_sql_connections(DEFAULT_SQL_CONNECTIONS)
