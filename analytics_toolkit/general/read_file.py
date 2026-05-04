from __future__ import annotations

import inspect
import sys
import sysconfig
from pathlib import Path
from typing import Any

from analytics_toolkit.sql.connection.errors import InvalidSqlInputError


def here(filename: str) -> str:
    normalized_name = Path(filename.replace("\\", "/"))

    base_dir = _resolve_main_file_dir()
    if base_dir is not None:
        return str(base_dir / normalized_name)

    cwd_candidate = Path.cwd() / normalized_name
    if cwd_candidate.exists():
        return str(cwd_candidate)

    matches = sorted(Path.cwd().rglob(normalized_name.name))
    if len(matches) == 1:
        return str(matches[0])

    return str(cwd_candidate)


def _resolve_base_dir() -> Path | None:
    main_dir = _resolve_main_file_dir()
    if main_dir is not None:
        return main_dir

    main_module = sys.modules.get("__main__")
    main_file = getattr(main_module, "__file__", None)
    if main_file and not str(main_file).startswith("<"):
        main_path = Path(main_file).expanduser().resolve()
        if _is_runtime_path(main_path):
            return None

    module_path = Path(__file__).expanduser().resolve()
    for frame_info in inspect.stack()[1:]:
        frame_name = frame_info.filename
        if frame_name.startswith("<"):
            continue

        frame_path = Path(frame_name).expanduser().resolve()
        if frame_path == module_path or _is_runtime_path(frame_path):
            continue
        return frame_path.parent

    return None


def _resolve_main_file_dir() -> Path | None:
    main_module = sys.modules.get("__main__")
    main_file = getattr(main_module, "__file__", None)
    if main_file and not str(main_file).startswith("<"):
        main_path = Path(main_file).expanduser().resolve()
        if not _is_runtime_path(main_path):
            return main_path.parent
    return None


def _is_runtime_path(path: Path) -> bool:
    normalized = path.as_posix()
    runtime_fragments = (
        "/IPython/",
        "/ipykernel_",
        "/site-packages/",
        "/dist-packages/",
        "/Contents/Resources/app/extensions/",
        "/tmp/",
        "/var/folders/",
    )
    if any(fragment in normalized for fragment in runtime_fragments):
        return True

    runtime_prefixes = {
        Path(prefix).expanduser().resolve()
        for prefix in (
            sys.prefix,
            sys.base_prefix,
            sys.exec_prefix,
            sysconfig.get_paths().get("stdlib"),
        )
        if prefix
    }
    return any(path == prefix or prefix in path.parents for prefix in runtime_prefixes)


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
