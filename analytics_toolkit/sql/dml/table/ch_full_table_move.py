from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ...connection.config import get_connection_config
from ...connection.errors import UnsupportedConnectionTypeError
from ...connection.get_sql_connection import get_sql_connection
from ...ddl.create_sql_table import (
    build_ch_shard_table_name,
    quote_identifier,
    split_ch_table_name_for_distributed_engine,
)
from ...ddl.create_sql_table import _wait_for_ch_table
from analytics_toolkit.general import time_print
from .table_ops import _execute_ch_command, drop_table, insert_from_table


_SHARD_ENGINE_FOLLOWING_CLAUSES = (
    "PARTITION BY",
    "ORDER BY",
    "PRIMARY KEY",
    "SAMPLE BY",
    "TTL",
    "SETTINGS",
    "COMMENT",
)
_PARTITION_FOLLOWING_CLAUSES = (
    "ORDER BY",
    "PRIMARY KEY",
    "SAMPLE BY",
    "TTL",
    "SETTINGS",
    "COMMENT",
)
_ORDER_FOLLOWING_CLAUSES = (
    "PRIMARY KEY",
    "SAMPLE BY",
    "TTL",
    "SETTINGS",
    "COMMENT",
)


def ch_full_table_move(
    db_key: str,
    move_table: str,
    to_table: str,
    *,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str | None = None,
    ch_cluster: str | None = None,
    sharding_key: str | None = None,
) -> None:
    config = get_connection_config(db_key)
    if config.backend != "ch":
        raise UnsupportedConnectionTypeError(
            f"ch_full_table_move requires a ch connection, got '{config.backend}'."
        )

    source_table = _normalize_required_string(move_table, "move_table")
    target_table = _normalize_required_string(to_table, "to_table")
    source_shard_table = build_ch_shard_table_name(source_table)
    target_shard_table = build_ch_shard_table_name(target_table)

    partition_override = _normalize_ch_expression_or_empty(
        ch_partition_by,
        "ch_partition_by",
        allow_empty_sequence=True,
    )
    order_override = _normalize_ch_expression_or_empty(
        ch_order_by,
        "ch_order_by",
        allow_empty_sequence=False,
    )
    engine_override = (
        None
        if ch_engine is None
        else _normalize_required_string(ch_engine, "ch_engine")
    )
    cluster_override = (
        None
        if ch_cluster is None
        else _normalize_required_string(ch_cluster, "ch_cluster")
    )
    sharding_key_override = (
        None
        if sharding_key is None
        else _normalize_required_string(sharding_key, "sharding_key")
    )

    connection = get_sql_connection(config.connection_key)
    try:
        time_print(
            f"Moving ClickHouse table {source_table} to "
            f"{target_table} on {config.connection_key}"
        )
        time_print(f"Reading ClickHouse DDL for distributed table {source_table}")
        source_distributed_ddl = _show_create_table(connection, source_table)
        source_shard_table = (
            _extract_distributed_shard_table_name(
                source_distributed_ddl,
                source_table,
            )
            or source_shard_table
        )
        time_print(f"Resolved source shard table {source_shard_table}")
        time_print(f"Reading ClickHouse DDL for shard table {source_shard_table}")
        source_shard_ddl = _show_create_table(connection, source_shard_table)
        source_cluster = (
            _extract_on_cluster_name(source_shard_ddl)
            or _extract_on_cluster_name(source_distributed_ddl)
            or _extract_distributed_cluster_name(source_distributed_ddl)
        )
        target_cluster = cluster_override or source_cluster
        if target_cluster is not None:
            time_print(f"Using ClickHouse cluster {target_cluster} for target DDL")

        target_shard_ddl = _build_target_shard_ddl(
            source_shard_ddl,
            target_shard_table,
            ch_partition_by=partition_override,
            ch_order_by=order_override,
            ch_engine=engine_override,
            ch_cluster=target_cluster,
        )
        target_distributed_ddl = _build_target_distributed_ddl(
            source_distributed_ddl,
            target_table,
            target_shard_table,
            ch_cluster=target_cluster,
            sharding_key=sharding_key_override,
        )

        time_print(
            f"Dropping target ClickHouse table pair {target_table} / "
            f"{target_shard_table}"
        )
        _drop_ch_distributed_table_pair(connection, target_table, target_cluster)
        time_print(f"Creating target shard table {target_shard_table}")
        _execute_ch_command(connection, target_shard_ddl)
        time_print(f"Creating target distributed table {target_table}")
        _execute_ch_command(connection, target_distributed_ddl)
        local_distributed_ddl = _remove_on_cluster_clause(target_distributed_ddl)
        if local_distributed_ddl != target_distributed_ddl:
            time_print(f"Creating local distributed table {target_table}")
            _execute_ch_command(connection, local_distributed_ddl)
        time_print(f"Waiting for target table {target_table}")
        _wait_for_ch_table(connection, target_table)
        time_print(f"Inserting data from {source_table} into {target_table}")
        insert_from_table("ch", connection, target_table, source_table)
        time_print(
            f"Dropping source ClickHouse table pair {source_table} / "
            f"{source_shard_table}"
        )
        _drop_ch_distributed_table_pair(
            connection,
            source_table,
            source_cluster,
            shard_table=source_shard_table,
        )
        time_print(f"Finished moving ClickHouse table {source_table} to {target_table}")
    finally:
        time_print(f"Closing {config.connection_key} connection")
        connection.close()


def _build_target_shard_ddl(
    source_ddl: str,
    target_shard_table: str,
    *,
    ch_partition_by: str | None,
    ch_order_by: str | None,
    ch_engine: str | None,
    ch_cluster: str | None,
) -> str:
    ddl = _prepare_target_create_table_ddl(source_ddl, target_shard_table)
    if ch_cluster is not None:
        ddl = _replace_or_add_on_cluster_clause(ddl, ch_cluster)
    if ch_engine is not None:
        ddl = _replace_top_level_clause_expression(
            ddl,
            "ENGINE",
            ch_engine,
            _SHARD_ENGINE_FOLLOWING_CLAUSES,
            separator=" = ",
        )
    if ch_partition_by is not None:
        ddl = _replace_top_level_clause_expression(
            ddl,
            "PARTITION BY",
            ch_partition_by,
            _PARTITION_FOLLOWING_CLAUSES,
        )
    if ch_order_by is not None:
        ddl = _replace_top_level_clause_expression(
            ddl,
            "ORDER BY",
            ch_order_by,
            _ORDER_FOLLOWING_CLAUSES,
        )
    return ddl


def _build_target_distributed_ddl(
    source_ddl: str,
    target_table: str,
    target_shard_table: str,
    *,
    ch_cluster: str | None,
    sharding_key: str | None,
) -> str:
    ddl = _prepare_target_create_table_ddl(source_ddl, target_table)
    if ch_cluster is not None:
        ddl = _replace_or_add_on_cluster_clause(ddl, ch_cluster)
    return _rewrite_distributed_engine_args(
        ddl,
        target_shard_table,
        ch_cluster=ch_cluster,
        sharding_key=sharding_key,
    )


def _prepare_target_create_table_ddl(source_ddl: str, target_table: str) -> str:
    ddl = source_ddl.strip().rstrip(";").rstrip()
    ddl = _replace_create_table_identifier(ddl, target_table)
    ddl = _ensure_create_table_if_not_exists(ddl)
    return _remove_uuid_clause(ddl)


def _show_create_table(connection: Any, table_name: str) -> str:
    result = connection.query(f"SHOW CREATE TABLE {table_name}")
    rows = getattr(result, "result_rows", None)
    if not rows or not rows[0]:
        raise ValueError(f"SHOW CREATE TABLE {table_name} returned no rows.")
    ddl = rows[0][0]
    if not isinstance(ddl, str):
        ddl = str(ddl)
    normalized = ddl.strip()
    if not normalized:
        raise ValueError(f"SHOW CREATE TABLE {table_name} returned an empty DDL.")
    return normalized


def _drop_ch_distributed_table_pair(
    connection: Any,
    table_name: str,
    ch_cluster: str | None,
    *,
    shard_table: str | None = None,
) -> None:
    shard_table = shard_table or build_ch_shard_table_name(table_name)
    drop_table("ch", connection, table_name)
    drop_table("ch", connection, shard_table)
    if ch_cluster is not None:
        drop_table("ch", connection, table_name, ch_cluster=ch_cluster)
        drop_table("ch", connection, shard_table, ch_cluster=ch_cluster)


def _replace_create_table_identifier(ddl: str, target_table: str) -> str:
    table_keyword_start = _find_top_level_keyword(ddl, "TABLE")
    if table_keyword_start < 0:
        raise ValueError("Expected SHOW CREATE DDL to contain CREATE TABLE.")
    table_keyword_end = table_keyword_start + len("TABLE")
    position = _skip_whitespace(ddl, table_keyword_end)
    if _matches_keyword_sequence(ddl, position, ("IF", "NOT", "EXISTS")):
        position = _skip_words(ddl, position, 3)
    identifier_start = _skip_whitespace(ddl, position)
    identifier_end = _scan_qualified_identifier_end(ddl, identifier_start)
    if identifier_end <= identifier_start:
        raise ValueError("Expected SHOW CREATE DDL to contain a table name.")
    return ddl[:identifier_start] + target_table + ddl[identifier_end:]


def _ensure_create_table_if_not_exists(ddl: str) -> str:
    table_keyword_start = _find_top_level_keyword(ddl, "TABLE")
    if table_keyword_start < 0:
        raise ValueError("Expected SHOW CREATE DDL to contain CREATE TABLE.")
    table_keyword_end = table_keyword_start + len("TABLE")
    position = _skip_whitespace(ddl, table_keyword_end)
    if _matches_keyword_sequence(ddl, position, ("IF", "NOT", "EXISTS")):
        return ddl
    return ddl[:table_keyword_end] + " IF NOT EXISTS" + ddl[table_keyword_end:]


def _remove_uuid_clause(ddl: str) -> str:
    uuid_start = _find_top_level_keyword(ddl, "UUID")
    if uuid_start < 0:
        return ddl
    value_start = _skip_whitespace(ddl, uuid_start + len("UUID"))
    value_end = _scan_single_value_end(ddl, value_start)
    return _replace_segment_with_clause(ddl, uuid_start, value_end, "")


def _replace_or_add_on_cluster_clause(ddl: str, ch_cluster: str) -> str:
    clause_start = _find_top_level_keyword(ddl, "ON CLUSTER")
    if clause_start >= 0:
        value_start = _skip_whitespace(ddl, clause_start + len("ON CLUSTER"))
        value_end = _scan_single_value_end(ddl, value_start)
        return ddl[:value_start] + ch_cluster + ddl[value_end:]

    _, table_identifier_end = _find_create_table_identifier_span(ddl)
    return (
        ddl[:table_identifier_end]
        + f" ON CLUSTER {ch_cluster}"
        + ddl[table_identifier_end:]
    )


def _remove_on_cluster_clause(ddl: str) -> str:
    clause_start = _find_top_level_keyword(ddl, "ON CLUSTER")
    if clause_start < 0:
        return ddl
    value_start = _skip_whitespace(ddl, clause_start + len("ON CLUSTER"))
    value_end = _scan_single_value_end(ddl, value_start)
    return _replace_segment_with_clause(ddl, clause_start, value_end, "")


def _extract_on_cluster_name(ddl: str) -> str | None:
    clause_start = _find_top_level_keyword(ddl, "ON CLUSTER")
    if clause_start < 0:
        return None
    value_start = _skip_whitespace(ddl, clause_start + len("ON CLUSTER"))
    value_end = _scan_single_value_end(ddl, value_start)
    return _normalize_extracted_sql_value(ddl[value_start:value_end])


def _extract_distributed_cluster_name(ddl: str) -> str | None:
    args_span = _find_distributed_args_span(ddl)
    if args_span is None:
        return None
    args_start, args_end = args_span
    args_sql = ddl[args_start:args_end]
    spans = _split_top_level_argument_spans(args_sql)
    if not spans:
        return None
    start, end = spans[0]
    return _normalize_extracted_sql_value(args_sql[start:end])


def _extract_distributed_shard_table_name(
    ddl: str,
    distributed_table: str,
) -> str | None:
    args_span = _find_distributed_args_span(ddl)
    if args_span is None:
        return None
    args_start, args_end = args_span
    args_sql = ddl[args_start:args_end]
    spans = _split_top_level_argument_spans(args_sql)
    if len(spans) < 3:
        return None

    database_start, database_end = spans[1]
    relation_start, relation_end = spans[2]
    database_name = _normalize_distributed_database_name(
        args_sql[database_start:database_end],
        distributed_table,
    )
    relation_name = _normalize_extracted_sql_value(
        args_sql[relation_start:relation_end],
    )
    if relation_name is None:
        return None
    if database_name is None:
        return _format_clickhouse_identifier(relation_name)
    return (
        f"{_format_clickhouse_identifier(database_name)}."
        f"{_format_clickhouse_identifier(relation_name)}"
    )


def _normalize_distributed_database_name(
    value: str,
    distributed_table: str,
) -> str | None:
    normalized = _normalize_extracted_sql_value(value)
    if normalized is None:
        return None
    compact = "".join(normalized.split()).lower()
    if compact in {"currentdatabase()", "database()"}:
        return _extract_table_database_name(distributed_table)
    return normalized


def _extract_table_database_name(table_name: str) -> str | None:
    database_sql, _ = split_ch_table_name_for_distributed_engine(table_name)
    if database_sql == "currentDatabase()":
        return None
    return _normalize_extracted_sql_value(database_sql)


def _format_clickhouse_identifier(identifier: str) -> str:
    normalized = identifier.strip()
    if not normalized:
        return normalized
    if normalized[0] in {"`", '"'}:
        return normalized
    if _is_simple_identifier(normalized):
        return normalized
    return quote_identifier(normalized, "ch")


def _rewrite_distributed_engine_args(
    ddl: str,
    target_shard_table: str,
    *,
    ch_cluster: str | None,
    sharding_key: str | None,
) -> str:
    args_span = _find_distributed_args_span(ddl)
    if args_span is None:
        raise ValueError("Expected distributed table DDL to contain Distributed(...).")
    args_start, args_end = args_span
    args_sql = ddl[args_start:args_end]
    spans = _split_top_level_argument_spans(args_sql)
    if len(spans) < 3:
        raise ValueError("Distributed engine must include cluster, database, and table.")

    _, target_shard_relation = split_ch_table_name_for_distributed_engine(
        target_shard_table
    )
    replacements: list[tuple[int, int, str]] = [
        (*spans[2], _sql_string_literal(target_shard_relation)),
    ]
    if ch_cluster is not None:
        replacements.append((*spans[0], _sql_string_literal(ch_cluster)))
    if sharding_key is not None:
        if len(spans) >= 4:
            replacements.append((*spans[-1], sharding_key))
        else:
            insertion = args_end
            return ddl[:insertion] + f", {sharding_key}" + ddl[insertion:]

    rewritten_args_sql = args_sql
    for start, end, replacement in sorted(replacements, reverse=True):
        rewritten_args_sql = (
            rewritten_args_sql[:start] + replacement + rewritten_args_sql[end:]
        )
    return ddl[:args_start] + rewritten_args_sql + ddl[args_end:]


def _find_distributed_args_span(ddl: str) -> tuple[int, int] | None:
    engine_start = _find_top_level_keyword(ddl, "ENGINE")
    if engine_start < 0:
        return None
    expression_start = _skip_whitespace(ddl, engine_start + len("ENGINE"))
    if expression_start < len(ddl) and ddl[expression_start] == "=":
        expression_start = _skip_whitespace(ddl, expression_start + 1)
    expression_end = _find_next_top_level_clause(
        ddl,
        expression_start,
        _SHARD_ENGINE_FOLLOWING_CLAUSES,
    )
    if expression_end < 0:
        expression_end = len(ddl)

    distributed_start = _find_keyword_in_range(
        ddl,
        "Distributed",
        expression_start,
        expression_end,
    )
    if distributed_start < 0:
        return None
    open_paren = _skip_whitespace(ddl, distributed_start + len("Distributed"))
    if open_paren >= len(ddl) or ddl[open_paren] != "(":
        return None
    close_paren = _find_matching_paren(ddl, open_paren)
    return open_paren + 1, close_paren


def _replace_top_level_clause_expression(
    ddl: str,
    clause: str,
    expression: str,
    following_clauses: Sequence[str],
    *,
    separator: str = " ",
) -> str:
    clause_start = _find_top_level_keyword(ddl, clause)
    if clause_start >= 0:
        expression_start = _skip_whitespace(ddl, clause_start + len(clause))
        if separator.strip() == "=":
            if expression_start < len(ddl) and ddl[expression_start] == "=":
                expression_start = _skip_whitespace(ddl, expression_start + 1)
        expression_end = _find_next_top_level_clause(
            ddl,
            expression_start,
            following_clauses,
        )
        if expression_end < 0:
            expression_end = len(ddl)
        return _replace_segment_with_clause(
            ddl,
            clause_start,
            expression_end,
            "" if expression == "" else f"{clause}{separator}{expression}",
        )

    if expression == "":
        return ddl

    insertion_index = _find_next_top_level_clause(ddl, 0, following_clauses)
    if insertion_index < 0:
        insertion_index = len(ddl)
    return _insert_clause(ddl, insertion_index, f"{clause}{separator}{expression}")


def _find_next_top_level_clause(
    ddl: str,
    start: int,
    clauses: Sequence[str],
) -> int:
    positions = [
        position
        for clause in clauses
        if (position := _find_top_level_keyword(ddl, clause, start=start)) >= 0
    ]
    return min(positions) if positions else -1


def _replace_segment_with_clause(
    ddl: str,
    start: int,
    end: int,
    replacement: str,
) -> str:
    prefix = ddl[:start].rstrip()
    suffix = ddl[end:].lstrip()
    if not replacement:
        if not suffix:
            return prefix
        return f"{prefix}\n{suffix}" if prefix else suffix
    if not suffix:
        return f"{prefix}\n{replacement}" if prefix else replacement
    if prefix:
        return f"{prefix}\n{replacement}\n{suffix}"
    return f"{replacement}\n{suffix}"


def _insert_clause(ddl: str, insertion_index: int, clause_sql: str) -> str:
    prefix = ddl[:insertion_index].rstrip()
    suffix = ddl[insertion_index:].lstrip()
    if not suffix:
        return f"{prefix}\n{clause_sql}" if prefix else clause_sql
    return f"{prefix}\n{clause_sql}\n{suffix}" if prefix else f"{clause_sql}\n{suffix}"


def _find_create_table_identifier_span(ddl: str) -> tuple[int, int]:
    table_keyword_start = _find_top_level_keyword(ddl, "TABLE")
    if table_keyword_start < 0:
        raise ValueError("Expected SHOW CREATE DDL to contain CREATE TABLE.")
    position = _skip_whitespace(ddl, table_keyword_start + len("TABLE"))
    if _matches_keyword_sequence(ddl, position, ("IF", "NOT", "EXISTS")):
        position = _skip_words(ddl, position, 3)
    identifier_start = _skip_whitespace(ddl, position)
    identifier_end = _scan_qualified_identifier_end(ddl, identifier_start)
    if identifier_end <= identifier_start:
        raise ValueError("Expected SHOW CREATE DDL to contain a table name.")
    return identifier_start, identifier_end


def _find_top_level_keyword(sql: str, keyword: str, start: int = 0) -> int:
    words = tuple(keyword.split())
    depth = 0
    quote: str | None = None
    index = start
    while index < len(sql):
        char = sql[index]
        if quote is not None:
            if char == quote:
                if (
                    quote in {"'", '"', "`"}
                    and index + 1 < len(sql)
                    and sql[index + 1] == quote
                ):
                    index += 2
                    continue
                quote = None
            elif char == "\\" and quote == "'" and index + 1 < len(sql):
                index += 2
                continue
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            continue
        if char == "(":
            depth += 1
            index += 1
            continue
        if char == ")":
            depth = max(depth - 1, 0)
            index += 1
            continue
        if depth == 0 and _matches_keyword_sequence(sql, index, words):
            return index
        index += 1
    return -1


def _find_keyword_in_range(sql: str, keyword: str, start: int, end: int) -> int:
    index = start
    while index < end:
        if _matches_keyword_sequence(sql, index, (keyword,)):
            return index
        index += 1
    return -1


def _matches_keyword_sequence(sql: str, position: int, words: Sequence[str]) -> bool:
    if position < 0 or position >= len(sql):
        return False
    if position > 0 and _is_identifier_char(sql[position - 1]):
        return False

    index = position
    for word_index, word in enumerate(words):
        if word_index:
            next_index = _skip_whitespace(sql, index)
            if next_index == index:
                return False
            index = next_index
        end = index + len(word)
        if sql[index:end].upper() != word.upper():
            return False
        if end < len(sql) and _is_identifier_char(sql[end]):
            return False
        index = end
    return True


def _skip_words(sql: str, position: int, count: int) -> int:
    index = position
    for _ in range(count):
        index = _skip_whitespace(sql, index)
        while index < len(sql) and _is_identifier_char(sql[index]):
            index += 1
    return index


def _skip_whitespace(sql: str, position: int) -> int:
    while position < len(sql) and sql[position].isspace():
        position += 1
    return position


def _scan_qualified_identifier_end(sql: str, position: int) -> int:
    index = position
    while index < len(sql):
        char = sql[index]
        if char in {"`", '"'}:
            index = _scan_quoted_value_end(sql, index)
            if index < len(sql) and sql[index] == ".":
                index += 1
                continue
            return index
        while index < len(sql) and not sql[index].isspace() and sql[index] != "(":
            index += 1
        return index
    return index


def _scan_single_value_end(sql: str, position: int) -> int:
    if position >= len(sql):
        return position
    if sql[position] in {"'", '"', "`"}:
        return _scan_quoted_value_end(sql, position)
    index = position
    while index < len(sql) and not sql[index].isspace() and sql[index] != "(":
        index += 1
    return index


def _scan_quoted_value_end(sql: str, position: int) -> int:
    quote = sql[position]
    index = position + 1
    while index < len(sql):
        if sql[index] == quote:
            if index + 1 < len(sql) and sql[index + 1] == quote:
                index += 2
                continue
            return index + 1
        if sql[index] == "\\" and quote == "'" and index + 1 < len(sql):
            index += 2
            continue
        index += 1
    raise ValueError("Unterminated quoted value in ClickHouse DDL.")


def _find_matching_paren(sql: str, open_paren: int) -> int:
    depth = 0
    quote: str | None = None
    index = open_paren
    while index < len(sql):
        char = sql[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            elif char == "\\" and quote == "'" and index + 1 < len(sql):
                index += 2
                continue
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ValueError("Unbalanced parentheses in ClickHouse DDL.")


def _split_top_level_argument_spans(args_sql: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    depth = 0
    quote: str | None = None
    start = 0
    index = 0
    while index < len(args_sql):
        char = args_sql[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(args_sql) and args_sql[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            elif char == "\\" and quote == "'" and index + 1 < len(args_sql):
                index += 2
                continue
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(depth - 1, 0)
        elif char == "," and depth == 0:
            spans.append(_trim_span(args_sql, start, index))
            start = index + 1
        index += 1
    spans.append(_trim_span(args_sql, start, len(args_sql)))
    return [
        (span_start, span_end)
        for span_start, span_end in spans
        if span_start < span_end
    ]


def _trim_span(text: str, start: int, end: int) -> tuple[int, int]:
    while start < end and text[start].isspace():
        start += 1
    while end > start and text[end - 1].isspace():
        end -= 1
    return start, end


def _normalize_ch_expression_or_empty(
    value: Sequence[str] | str | None,
    option_name: str,
    *,
    allow_empty_sequence: bool,
) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return _normalize_required_string(value, option_name)

    columns = [_normalize_required_string(column, option_name) for column in value]
    if not columns:
        if allow_empty_sequence:
            return ""
        raise ValueError(f"{option_name} must not be empty when provided.")
    if len(set(columns)) != len(columns):
        raise ValueError(f"{option_name} must not contain duplicate column names.")
    quoted_columns = [quote_identifier(column, "ch") for column in columns]
    if len(quoted_columns) == 1:
        return quoted_columns[0]
    return f"({', '.join(quoted_columns)})"


def _normalize_required_string(value: str, option_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{option_name} must not be empty.")
    return normalized


def _normalize_extracted_sql_value(value: str) -> str | None:
    normalized = value.strip()
    if not normalized:
        return None
    if normalized[0] == "'" and normalized[-1:] == "'":
        return normalized[1:-1].replace("''", "'")
    return normalized


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _is_identifier_char(char: str) -> bool:
    return char.isalnum() or char == "_"


def _is_simple_identifier(identifier: str) -> bool:
    if not identifier:
        return False
    if not (identifier[0].isalpha() or identifier[0] == "_"):
        return False
    return all(_is_identifier_char(char) for char in identifier)
