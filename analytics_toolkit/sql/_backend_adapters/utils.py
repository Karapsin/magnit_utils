from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def extract_row_count(executed: Any) -> int:
    row_count = _coerce_row_count(getattr(executed, "rowcount", None))
    if row_count is not None:
        return row_count

    if isinstance(executed, Mapping):
        row_count = _extract_row_count_from_mapping(executed)
        if row_count is not None:
            return row_count

    summary = getattr(executed, "summary", None)
    if isinstance(summary, Mapping):
        row_count = _extract_row_count_from_mapping(summary)
        if row_count is not None:
            return row_count

    for attribute in ("written_rows", "writtenRows", "processed_rows", "rows"):
        row_count = _coerce_row_count(getattr(executed, attribute, None))
        if row_count is not None:
            return row_count

    return 0


def _extract_row_count_from_mapping(value: Mapping[str, Any]) -> int | None:
    for key in (
        "rowcount",
        "row_count",
        "written_rows",
        "writtenRows",
        "processedRows",
        "rows",
    ):
        row_count = _coerce_row_count(value.get(key))
        if row_count is not None:
            return row_count
    return None


def _coerce_row_count(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        row_count = int(value)
    except (TypeError, ValueError):
        return None
    if row_count < 0:
        return None
    return row_count
