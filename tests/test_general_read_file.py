from __future__ import annotations

import __main__
from collections import namedtuple
from pathlib import Path

import analytics_toolkit.general as general
from analytics_toolkit.general import here
from analytics_toolkit.general.read_file import _resolve_base_dir


FrameInfo = namedtuple("FrameInfo", ["filename"])
RUNTIME_STACK = [
    FrameInfo(filename="/Users/test/project/utils_dev/analytics_toolkit/general/read_file.py"),
    FrameInfo(filename="/private/var/folders/vq/zns5cfbd6zd64jw8hfgzzczr0000gq/T/ipykernel_99706/123.py"),
    FrameInfo(filename="/Users/test/.venv/lib/python3.11/site-packages/IPython/core/interactiveshell.py"),
    FrameInfo(filename="/opt/homebrew/Cellar/python@3.14/3.14.3_1/Frameworks/Python.framework/Versions/3.14/lib/python3.14/asyncio/events.py"),
]


def _mock_stack(monkeypatch, frames: list[FrameInfo]) -> None:
    monkeypatch.setattr("analytics_toolkit.general.read_file.inspect.stack", lambda: frames)


def test_here_prefers_main_module_directory_for_new_paths(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(__main__, "__file__", str(Path(__file__).resolve()), raising=False)

    resolved = here("new_output.xlsx")

    assert resolved == str(Path(__file__).resolve().parent / "new_output.xlsx")


def test_here_falls_back_to_cwd_without_main_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delattr(__main__, "__file__", raising=False)
    _mock_stack(monkeypatch, RUNTIME_STACK)

    resolved = here("new_output.xlsx")

    assert resolved == str(tmp_path / "new_output.xlsx")


def test_here_uses_real_caller_when_main_file_is_ipykernel(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        __main__,
        "__file__",
        "/private/var/folders/vq/zns5cfbd6zd64jw8hfgzzczr0000gq/T/ipykernel_99706/123.py",
        raising=False,
    )
    _mock_stack(
        monkeypatch,
        [
            *RUNTIME_STACK,
            FrameInfo(filename="/Users/test/project/notebooks/analysis.py"),
        ],
    )

    resolved = here("new_output.xlsx")

    assert resolved == "/Users/test/project/notebooks/new_output.xlsx"


def test_resolve_base_dir_uses_first_real_caller_file(monkeypatch) -> None:
    fake_stack = [
        FrameInfo(filename="/Users/test/project/utils_dev/analytics_toolkit/general/read_file.py"),
        FrameInfo(filename="/private/var/folders/vq/zns5cfbd6zd64jw8hfgzzczr0000gq/T/ipykernel_99706/123.py"),
        FrameInfo(filename="/opt/homebrew/Cellar/python@3.14/3.14.3_1/Frameworks/Python.framework/Versions/3.14/lib/python3.14/asyncio/events.py"),
        FrameInfo(filename="/Users/test/project/tickets/april_2026/MAL-3657/compute_metrics.py"),
    ]
    monkeypatch.delattr(__main__, "__file__", raising=False)
    monkeypatch.setattr("analytics_toolkit.general.read_file.inspect.stack", lambda: fake_stack)

    resolved = _resolve_base_dir()

    assert resolved == Path("/Users/test/project/tickets/april_2026/MAL-3657")


def test_here_uses_first_real_caller_after_ide_runtime_frames(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delattr(__main__, "__file__", raising=False)
    _mock_stack(
        monkeypatch,
        [
            FrameInfo(filename="/Users/test/project/utils_dev/analytics_toolkit/general/read_file.py"),
            FrameInfo(filename="/Users/test/project/utils_dev/analytics_toolkit/general/read_file.py"),
            FrameInfo(filename="/Users/test/.vscode/extensions/ms-python.python/pythonFiles/lib/python/debugpy/launcher/__main__.py"),
            FrameInfo(filename="/Applications/PyCharm.app/Contents/plugins/python/helpers/pydev/pydevd.py"),
            FrameInfo(filename="/Users/test/.venv/lib/python3.11/site-packages/pydevd.py"),
            FrameInfo(filename="/opt/homebrew/Cellar/python@3.14/3.14.3_1/Frameworks/Python.framework/Versions/3.14/lib/python3.14/runpy.py"),
            FrameInfo(filename="/Users/test/project/reports/build_report.py"),
            FrameInfo(filename="/Users/test/project/reports/helpers.py"),
        ],
    )

    resolved = here("new_output.xlsx")

    assert resolved == "/Users/test/project/reports/new_output.xlsx"


def test_here_returns_existing_cwd_file_without_base_dir(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delattr(__main__, "__file__", raising=False)
    _mock_stack(monkeypatch, RUNTIME_STACK)
    expected = tmp_path / "existing.sql"
    expected.write_text("select 1", encoding="utf-8")

    resolved = here("existing.sql")

    assert resolved == str(expected)


def test_here_returns_cwd_path_for_missing_output_without_base_dir(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delattr(__main__, "__file__", raising=False)
    _mock_stack(monkeypatch, RUNTIME_STACK)

    resolved = here("new_output.xlsx")

    assert resolved == str(tmp_path / "new_output.xlsx")


def test_here_recursively_resolves_unique_relative_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delattr(__main__, "__file__", raising=False)
    _mock_stack(monkeypatch, RUNTIME_STACK)
    expected = tmp_path / "project_a" / "sql" / "query.sql"
    expected.parent.mkdir(parents=True)
    expected.write_text("select 1", encoding="utf-8")

    resolved = here("sql/query.sql")

    assert resolved == str(expected)


def test_here_does_not_match_unrelated_unique_basename_for_relative_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delattr(__main__, "__file__", raising=False)
    _mock_stack(monkeypatch, RUNTIME_STACK)
    unrelated = tmp_path / "other" / "query.sql"
    unrelated.parent.mkdir()
    unrelated.write_text("select 1", encoding="utf-8")

    resolved = here("sql/query.sql")

    assert resolved == str(tmp_path / "sql" / "query.sql")


def test_here_ambiguous_recursive_relative_matches_fall_back_to_cwd(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delattr(__main__, "__file__", raising=False)
    _mock_stack(monkeypatch, RUNTIME_STACK)
    for directory in ("project_a", "project_b"):
        path = tmp_path / directory / "sql" / "query.sql"
        path.parent.mkdir(parents=True)
        path.write_text("select 1", encoding="utf-8")

    resolved = here("sql/query.sql")

    assert resolved == str(tmp_path / "sql" / "query.sql")


def test_here_keeps_unique_basename_recursive_lookup_for_compatibility(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delattr(__main__, "__file__", raising=False)
    _mock_stack(monkeypatch, RUNTIME_STACK)
    expected = tmp_path / "nested" / "query.sql"
    expected.parent.mkdir()
    expected.write_text("select 1", encoding="utf-8")

    resolved = here("query.sql")

    assert resolved == str(expected)


def test_general_here_export_and_read_file_inspect_are_compatible() -> None:
    assert general.here is here
    assert general.read_file.inspect is not None
