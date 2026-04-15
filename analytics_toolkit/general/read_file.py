from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

from analytics_toolkit.sql.connection.errors import InvalidSqlInputError


def here(filename: str) -> str:
    normalized_name = filename.replace("\\", "/")

    for frame_info in inspect.stack()[1:]:
        frame_path = Path(frame_info.filename).expanduser()
        frame_name = frame_info.filename.replace("\\", "/")
        if frame_name.startswith("<"):
            continue
        if "/ipykernel_" in frame_name or "/tmp/" in frame_name or "/var/folders/" in frame_name:
            continue

        candidate = frame_path.resolve().parent / normalized_name
        return str(candidate)

    cwd_candidate = Path.cwd() / normalized_name
    if cwd_candidate.exists():
        return str(cwd_candidate)

    matches = sorted(Path.cwd().rglob(Path(normalized_name).name))
    if len(matches) == 1:
        return str(matches[0])

    return str(cwd_candidate)


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
