from __future__ import annotations

from pathlib import Path
from typing import Any

from ..connection.errors import InvalidSqlInputError
from .logging import time_print


def parse_sql(file_path: str, params_dict: dict[str, Any] | None = None) -> str:
    path = Path(file_path).expanduser()
    if not path.exists():
        raise InvalidSqlInputError(f"SQL file does not exist: {file_path}")

    time_print(f"Parsing SQL file {path}")
    sql = path.read_text(encoding="utf-8")

    if params_dict is None:
        return sql

    return sql.format(**params_dict)
