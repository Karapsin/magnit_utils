from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

schema_module = importlib.import_module("analytics_toolkit.sql.dml.transfer.schema")


class FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def query(self, sql: str) -> FakeResult:
        self.queries.append(sql)
        if sql == "DESCRIBE TABLE target":
            return FakeResult(
                [
                    ("id", "Nullable(Int64)"),
                    ("amount", "Nullable(Decimal(18, 4))"),
                ]
            )
        return FakeResult([])


def test_map_source_schema_to_target_preserves_common_types() -> None:
    source_schema = [
        schema_module.SourceColumn("is_active", "boolean"),
        schema_module.SourceColumn("user_id", "integer"),
        schema_module.SourceColumn("amount", "numeric(12, 2)"),
        schema_module.SourceColumn("created_at", "timestamp"),
        schema_module.SourceColumn("payload", "jsonb"),
    ]

    assert schema_module.map_source_schema_to_target(source_schema, "gp") == {
        "is_active": "BOOLEAN",
        "user_id": "INTEGER",
        "amount": "NUMERIC(12, 2)",
        "created_at": "TIMESTAMP",
        "payload": "TEXT",
    }
    assert schema_module.map_source_schema_to_target(source_schema, "trino") == {
        "is_active": "BOOLEAN",
        "user_id": "INTEGER",
        "amount": "DECIMAL(12, 2)",
        "created_at": "TIMESTAMP",
        "payload": "VARCHAR",
    }
    assert schema_module.map_source_schema_to_target(source_schema, "ch") == {
        "is_active": "Nullable(Bool)",
        "user_id": "Nullable(Int32)",
        "amount": "Nullable(Decimal(12, 2))",
        "created_at": "Nullable(DateTime64(6))",
        "payload": "Nullable(String)",
    }


def test_map_source_schema_to_target_preserves_binary_types() -> None:
    source_schema = [
        schema_module.SourceColumn("cheque_pk", "bytea"),
        schema_module.SourceColumn("raw_payload", "varbinary"),
    ]

    assert schema_module.map_source_schema_to_target(source_schema, "gp") == {
        "cheque_pk": "BYTEA",
        "raw_payload": "BYTEA",
    }
    assert schema_module.map_source_schema_to_target(source_schema, "trino") == {
        "cheque_pk": "VARBINARY",
        "raw_payload": "VARBINARY",
    }
    assert schema_module.map_source_schema_to_target(source_schema, "ch") == {
        "cheque_pk": "Nullable(String)",
        "raw_payload": "Nullable(String)",
    }


def test_map_source_schema_to_target_falls_back_for_invalid_decimal_bounds() -> None:
    source_schema = [
        schema_module.SourceColumn("quantity", "numeric(65535, 0)"),
    ]

    assert schema_module.map_source_schema_to_target(source_schema, "gp") == {
        "quantity": "NUMERIC",
    }
    assert schema_module.map_source_schema_to_target(source_schema, "trino") == {
        "quantity": "DECIMAL(38, 10)",
    }
    assert schema_module.map_source_schema_to_target(source_schema, "ch") == {
        "quantity": "Nullable(Decimal(38, 10))",
    }


def test_existing_target_insert_types_use_target_metadata() -> None:
    client = FakeClickHouseClient()

    result = schema_module.get_existing_target_insert_types(
        "ch",
        client,
        "target",
        {
            "id": "Nullable(Int32)",
            "amount": "Nullable(Float64)",
        },
        connection_key="ch",
    )

    assert result == {
        "id": "Nullable(Int64)",
        "amount": "Nullable(Decimal(18, 4))",
    }


def test_existing_target_insert_types_reject_missing_columns() -> None:
    client = FakeClickHouseClient()

    try:
        schema_module.get_existing_target_insert_types(
            "ch",
            client,
            "target",
            {
                "id": "Nullable(Int32)",
                "missing": "Nullable(String)",
            },
            connection_key="ch",
        )
    except ValueError as exc:
        assert "missing" in str(exc)
    else:
        raise AssertionError("Expected missing target column to raise.")
