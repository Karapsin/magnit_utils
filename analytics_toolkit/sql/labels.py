from __future__ import annotations

import re


_LABEL_SAFE_RE = re.compile(r"[^A-Za-z0-9_.:=,@/+ -]+")


def normalize_query_label(query_label: str | None) -> str | None:
    if query_label is None:
        return None
    normalized = " ".join(str(query_label).strip().split())
    if not normalized:
        return None
    normalized = _LABEL_SAFE_RE.sub("_", normalized)
    return normalized[:200]


def query_label_comment(query_label: str | None) -> str:
    normalized = normalize_query_label(query_label)
    if normalized is None:
        return ""
    return f"/* analytics_toolkit query_label={normalized} */"


def apply_query_label(sql: str, query_label: str | None) -> str:
    comment = query_label_comment(query_label)
    if not comment:
        return sql
    stripped = sql.lstrip()
    if stripped.startswith("/* analytics_toolkit query_label="):
        return sql
    return f"{comment}\n{sql}"
