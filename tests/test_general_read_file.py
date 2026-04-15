from __future__ import annotations

from pathlib import Path

from analytics_toolkit.general import here


def test_here_prefers_caller_directory_for_new_paths(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)

    resolved = here("new_output.xlsx")

    assert resolved == str(Path(__file__).resolve().parent / "new_output.xlsx")
