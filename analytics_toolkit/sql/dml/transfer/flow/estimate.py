from __future__ import annotations

import json
import math
import re
from typing import Any

import sqlparse
from sqlglot import exp, parse_one

from ....connection.errors import InvalidSqlInputError
from ....labels import apply_query_label
from analytics_toolkit.general import time_print
from ..runtime.models import TransferOptions
from ..runtime.retry import rollback_quietly


_TEXT_PLAN_ROWS_RE = re.compile(r"\brows=(\d+(?:\.\d+)?)\b")


def estimate_source_rows(options: TransferOptions, connection: Any) -> int | None:
    if not options.progress or not options.estimate_total_rows:
        return None

    try:
        source_sql = _single_source_statement(options.source_sql)
        estimated_rows = _estimate_source_rows(
            options.from_db_backend,
            connection,
            source_sql,
            query_label=options.query_label,
        )
    except Exception as exc:
        if options.from_db_backend == "gp":
            rollback_quietly(connection)
        time_print(
            "Could not estimate source row count for transfer from "
            f"{options.from_db_key}; progress total will be unknown: {exc!r}"
        )
        return None

    if estimated_rows is None:
        time_print(
            "Source row estimate is unavailable for transfer from "
            f"{options.from_db_key}; progress total will be unknown."
        )
        return None

    time_print(
        "Using approximate source row estimate for transfer from "
        f"{options.from_db_key}: {estimated_rows} row(s)"
    )
    return estimated_rows


def _estimate_source_rows(
    backend: str,
    connection: Any,
    source_sql: str,
    *,
    query_label: str | None,
) -> int | None:
    if backend == "gp":
        return _estimate_gp_source_rows(
            connection,
            source_sql,
            query_label=query_label,
        )
    if backend == "trino":
        return _estimate_trino_source_rows(
            connection,
            source_sql,
            query_label=query_label,
        )
    if backend == "ch":
        return _estimate_clickhouse_source_rows(
            connection,
            source_sql,
            query_label=query_label,
        )
    return None


def _single_source_statement(source_sql: str) -> str:
    statements = [
        statement.strip()
        for statement in sqlparse.split(source_sql.strip())
        if statement.strip()
    ]
    if len(statements) != 1:
        raise InvalidSqlInputError(
            "estimate_total_rows expects exactly one source SQL statement."
        )
    return statements[0].rstrip(";").rstrip()


def _estimate_gp_source_rows(
    connection: Any,
    source_sql: str,
    *,
    query_label: str | None,
) -> int | None:
    json_sql = apply_query_label(f"EXPLAIN (FORMAT JSON) {source_sql}", query_label)
    try:
        values = _fetch_dbapi_first_column(connection, json_sql)
        estimated_rows = _extract_gp_json_plan_rows(values)
        if estimated_rows is not None:
            return estimated_rows
    except Exception:
        rollback_quietly(connection)

    text_sql = apply_query_label(f"EXPLAIN {source_sql}", query_label)
    try:
        values = _fetch_dbapi_first_column(connection, text_sql)
    except Exception:
        rollback_quietly(connection)
        raise
    return _extract_text_plan_rows(values)


def _estimate_trino_source_rows(
    connection: Any,
    source_sql: str,
    *,
    query_label: str | None,
) -> int | None:
    explain_sql = apply_query_label(
        f"EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) {source_sql}",
        query_label,
    )
    values = _fetch_dbapi_first_column(connection, explain_sql)
    return _extract_trino_output_row_count(values)


def _estimate_clickhouse_source_rows(
    connection: Any,
    source_sql: str,
    *,
    query_label: str | None,
) -> int | None:
    if not _is_simple_clickhouse_select(source_sql):
        return None

    explain_sql = apply_query_label(f"EXPLAIN ESTIMATE {source_sql}", query_label)
    result = connection.query(explain_sql)
    return _extract_clickhouse_estimate_rows(result)


def _fetch_dbapi_first_column(connection: Any, sql: str) -> list[Any]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    finally:
        cursor.close()

    values: list[Any] = []
    for row in rows:
        if isinstance(row, (tuple, list)):
            if row:
                values.append(row[0])
        else:
            values.append(row)
    return values


def _extract_gp_json_plan_rows(values: list[Any]) -> int | None:
    for payload in _json_payloads(values):
        root = payload[0] if isinstance(payload, list) and payload else payload
        if not isinstance(root, dict):
            continue
        plan = root.get("Plan") if isinstance(root.get("Plan"), dict) else root
        estimated_rows = _value_from_keys(
            plan,
            ("Plan Rows", "PlanRows", "plan_rows"),
        )
        if estimated_rows is not None:
            return estimated_rows
    return None


def _extract_trino_output_row_count(values: list[Any]) -> int | None:
    for payload in _json_payloads(values):
        estimated_rows = _trino_output_row_count_from_payload(payload)
        if estimated_rows is not None:
            return estimated_rows
    return None


def _trino_output_row_count_from_payload(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None

    estimated_rows = _value_from_keys(payload, ("outputRowCount",))
    if estimated_rows is not None:
        return estimated_rows

    for key in ("root", "plan", "fragment", "stats"):
        child = payload.get(key)
        if isinstance(child, dict):
            estimated_rows = _value_from_keys(child, ("outputRowCount",))
            if estimated_rows is not None:
                return estimated_rows

    stats_and_costs = payload.get("statsAndCosts")
    if isinstance(stats_and_costs, dict):
        for child in stats_and_costs.values():
            if isinstance(child, dict):
                estimated_rows = _value_from_keys(child, ("outputRowCount",))
                if estimated_rows is not None:
                    return estimated_rows

    return None


def _json_payloads(values: list[Any]) -> list[Any]:
    payloads: list[Any] = []
    for value in values:
        if isinstance(value, (dict, list)):
            payloads.append(value)
            continue
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        if not isinstance(value, str):
            continue
        text = value.strip()
        if not text:
            continue
        try:
            payloads.append(json.loads(text))
        except json.JSONDecodeError:
            continue
    return payloads


def _extract_text_plan_rows(values: list[Any]) -> int | None:
    for value in values:
        match = _TEXT_PLAN_ROWS_RE.search(str(value))
        if not match:
            continue
        estimated_rows = _coerce_estimated_row_count(match.group(1))
        if estimated_rows is not None:
            return estimated_rows
    return None


def _is_simple_clickhouse_select(source_sql: str) -> bool:
    try:
        tree = parse_one(source_sql, read="clickhouse")
    except Exception:
        return False

    if not isinstance(tree, exp.Select):
        return False
    if tree.args.get("with_") is not None:
        return False
    if list(tree.find_all(exp.Join)) or list(tree.find_all(exp.Subquery)):
        return False

    tables = list(tree.find_all(exp.Table))
    from_expression = tree.args.get("from_")
    if (
        len(tables) != 1
        or from_expression is None
        or not isinstance(from_expression.this, exp.Table)
    ):
        return False

    row_changing_args = (
        "where",
        "group",
        "having",
        "qualify",
        "order",
        "limit",
        "offset",
        "distinct",
        "sample",
    )
    return all(tree.args.get(arg) is None for arg in row_changing_args)


def _extract_clickhouse_estimate_rows(result: Any) -> int | None:
    rows = getattr(result, "result_rows", None) or []
    if not rows:
        return None

    column_names = [
        str(column_name).lower()
        for column_name in (getattr(result, "column_names", None) or [])
    ]
    if "rows" in column_names:
        rows_index = column_names.index("rows")
    elif isinstance(rows[0], (tuple, list)) and len(rows[0]) >= 4:
        rows_index = 3
    elif isinstance(rows[0], (tuple, list)) and len(rows[0]) == 1:
        rows_index = 0
    else:
        return None

    counts: list[int] = []
    for row in rows:
        if not isinstance(row, (tuple, list)) or rows_index >= len(row):
            continue
        estimated_rows = _coerce_estimated_row_count(row[rows_index])
        if estimated_rows is not None:
            counts.append(estimated_rows)
    if not counts:
        return None
    return sum(counts)


def _value_from_keys(payload: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        if key not in payload:
            continue
        estimated_rows = _coerce_estimated_row_count(payload[key])
        if estimated_rows is not None:
            return estimated_rows
    return None


def _coerce_estimated_row_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, dict):
        if "value" in value:
            return _coerce_estimated_row_count(value["value"])
        return None
    if isinstance(value, int):
        return value if value >= 0 else None

    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.lower() in {"nan", "infinity", "+infinity", "-infinity", "unknown"}:
            return None
        value = text

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return int(round(number))
