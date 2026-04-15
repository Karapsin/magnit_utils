from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from openpyxl import load_workbook

from analytics_toolkit.excel import break_table, pivot_and_break_table


def test_pivot_and_break_table_writes_multiple_sheets_and_tables(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "metric": ["users", "users", "arpu", "arpu", "users", "users", "arpu", "arpu"],
            "ab_group": ["control", "test_1", "control", "test_1", "control", "test_1", "control", "test_1"],
            "qr_group": ["ALL", "ALL", "ALL", "ALL", "1", "1", "1", "1"],
            "start_dt": ["2026-03-30", "2026-03-30", "2026-03-30", "2026-03-30", "2026-04-01", "2026-04-01", "2026-04-01", "2026-04-01"],
            "value": [100, 110, 2.5, 2.7, 50, 55, 1.1, 1.2],
        }
    )

    output = tmp_path / "report.xlsx"
    tables = pivot_and_break_table(
        df=df,
        rows="metric",
        value="value",
        output=output,
        columns="ab_group",
        break_by="qr_group",
        sheet_by="start_dt",
    )

    assert list(tables) == ["2026-03-30", "2026-04-01"]
    assert len(tables["2026-03-30"]) == 1
    assert tables["2026-03-30"][0].columns.tolist() == ["metric", "control", "test_1"]

    workbook = load_workbook(output, read_only=True, data_only=True)
    try:
        assert workbook.sheetnames == ["2026-03-30", "2026-04-01"]
        first_sheet = workbook["2026-03-30"]
        rows = list(first_sheet.iter_rows(values_only=True))
        assert rows[:4] == [
            ("qr_group = ALL", None, None),
            ("metric", "control", "test_1"),
            ("users", 100, 110),
            ("arpu", 2.5, 2.7),
        ]
    finally:
        workbook.close()


def test_pivot_and_break_table_sanitizes_and_deduplicates_sheet_names(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "metric": ["users", "users", "users", "users"],
            "ab_group": ["control", "test_1", "control", "test_1"],
            "sheet_bucket": [
                "Report/Name:One*?",
                "Report/Name:One*?",
                "X" * 40,
                "X" * 40 + " trailing",
            ],
            "value": [1, 2, 3, 4],
        }
    )

    output = tmp_path / "sanitized.xlsx"
    pivot_and_break_table(
        df=df,
        rows="metric",
        value="value",
        output=output,
        columns="ab_group",
        sheet_by="sheet_bucket",
    )

    workbook = load_workbook(output, read_only=True, data_only=True)
    try:
        assert workbook.sheetnames == [
            "Report_Name_One__",
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            "XXXXXXXXXXXXXXXXXXXXXXXXXXX (2)",
        ]
    finally:
        workbook.close()


def test_pivot_and_break_table_rejects_duplicates_within_group_slices(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "metric": ["users", "users"],
            "ab_group": ["control", "control"],
            "start_dt": ["2026-03-30", "2026-03-30"],
            "value": [100, 120],
        }
    )

    with pytest.raises(ValueError, match="not unique"):
        pivot_and_break_table(
            df=df,
            rows="metric",
            value="value",
            output=tmp_path / "duplicates.xlsx",
            columns="ab_group",
            sheet_by="start_dt",
        )


def test_break_table_writes_grouped_raw_tables_without_uniqueness_checks(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "metric": ["users", "users", "users", "users"],
            "ab_group": ["control", "control", "test_1", "test_1"],
            "qr_group": ["ALL", "ALL", "ALL", "ALL"],
            "start_dt": ["2026-03-30"] * 4,
            "value": [100, 120, 110, 130],
        }
    )

    output = tmp_path / "raw_tables.xlsx"
    tables = break_table(
        df=df,
        output=output,
        break_by="qr_group",
        sheet_by="start_dt",
    )

    assert list(tables) == ["2026-03-30"]
    assert tables["2026-03-30"][0].to_dict(orient="records") == df.to_dict(orient="records")

    workbook = load_workbook(output, read_only=True, data_only=True)
    try:
        assert workbook.sheetnames == ["2026-03-30"]
        rows = list(workbook["2026-03-30"].iter_rows(values_only=True))
        assert rows[:6] == [
            ("qr_group = ALL", None, None, None, None),
            ("metric", "ab_group", "qr_group", "start_dt", "value"),
            ("users", "control", "ALL", "2026-03-30", 100),
            ("users", "control", "ALL", "2026-03-30", 120),
            ("users", "test_1", "ALL", "2026-03-30", 110),
            ("users", "test_1", "ALL", "2026-03-30", 130),
        ]
    finally:
        workbook.close()
