from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

from analytics_toolkit.sql.connection.errors import InvalidSqlInputError


def here(filename: str) -> str:
    caller_frame = inspect.stack()[1]
    base_dir = caller_frame.filename.replace("\\", "/").rsplit("/", 1)[0]
    return f"{base_dir}/{filename}"


def read_file(file_path: str, params_dict: dict[str, Any] | None = None) -> str:
    path = Path(file_path).expanduser()
    if not path.exists():
        raise InvalidSqlInputError(f"SQL file does not exist: {file_path}")

    from .logging import time_print

    time_print(f"Reading file {path}")
    text = path.read_text(encoding="utf-8")

    if params_dict is None:
        return text

    return text.format(**params_dict)
