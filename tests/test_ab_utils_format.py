from __future__ import annotations

import importlib

import numpy as np
import pandas as pd
import pytest

from analytics_toolkit.ab_utils import format_ab_metrics


def _build_metric_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "segment": ["first", "first"],
            "group_1": ["test", "test"],
            "group_2": ["control", "control"],
            "metric_name": ["orders", "gmv"],
            "metric_control": [10.0, 100.0],
            "metric_test": [12.0, 110.0],
            "n0": [3, 3],
            "n1": [4, 4],
            "outliers_cutoff": [99.0, 999.0],
            "outliers_n_control": [0, 1],
            "outliers_n_test": [1, 2],
            "variance_control": [1.5, 10.0],
            "variance_test": [2.5, 12.0],
            "delta_abs": [2.0, 10.0],
            "delta_relative": [0.2, 0.1],
            "mde_abs": [3.0, 30.0],
            "mde_relative": [0.3, 0.3],
            "s.e.": [0.5, 5.0],
            "p-value": [0.04, 0.2],
            "s.e. CUPED": [0.4, 4.0],
            "p-value CUPED": [0.03, 0.15],
            "s.e. bootstrap": [0.6, 6.0],
            "bootstrap_adj_p": [0.08, 0.3],
        }
    )


def test_format_ab_metrics_defaults_to_metric_value_table() -> None:
    result = format_ab_metrics(_build_metric_rows())

    expected = pd.DataFrame(
        {
            "metric": ["orders", "gmv"],
            "control": [10.0, 100.0],
            "test": [12.0, 110.0],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_format_ab_metrics_accepts_consistent_repeated_group_values() -> None:
    df = pd.DataFrame(
        {
            "group_1": ["test_1", "test_2"],
            "group_2": ["control", "control"],
            "metric_name": ["orders", "orders"],
            "metric_control": [10.0, 10.0],
            "metric_test": [12.0, 13.0],
        }
    )

    result = format_ab_metrics(df)

    expected = pd.DataFrame(
        {
            "metric": ["orders"],
            "control": [10.0],
            "test_1": [12.0],
            "test_2": [13.0],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_format_ab_metrics_keeps_labels_and_first_seen_order() -> None:
    df = pd.DataFrame(
        {
            "segment": ["B", "A", "B"],
            "country": ["RU", "KZ", "RU"],
            "group_1": ["variant_b", "variant_a", "variant_b"],
            "group_2": ["control", "control", "control"],
            "metric_name": ["orders", "orders", "gmv"],
            "metric_control": [10.0, 20.0, 100.0],
            "metric_test": [12.0, 22.0, 115.0],
        }
    )

    result = format_ab_metrics(df, label_cols=["segment", "country"])

    expected = pd.DataFrame(
        {
            "segment": ["B", "A", "B"],
            "country": ["RU", "KZ", "RU"],
            "metric": ["orders", "orders", "gmv"],
            "control": [10.0, 20.0, 100.0],
            "variant_b": [12.0, np.nan, 115.0],
            "variant_a": [np.nan, 22.0, np.nan],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_format_ab_metrics_supports_multiple_output_types() -> None:
    result = format_ab_metrics(
        _build_metric_rows().iloc[[0]],
        output_type=["metric_values", "variance", "n", "p_values", "delta_abs", "se"],
    )

    expected = pd.DataFrame(
        {
            "metric": ["orders"],
            "control_metric_value": [10.0],
            "test_metric_value": [12.0],
            "control_variance": [1.5],
            "test_variance": [2.5],
            "control_n": [3],
            "test_n": [4],
            "test_vs_control_p_value": [0.04],
            "test_vs_control_delta_abs": [2.0],
            "test_vs_control_se": [0.5],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_format_ab_metrics_accepts_single_output_type_string() -> None:
    df = _build_metric_rows()

    result = format_ab_metrics(df, output_type="delta_relative")
    expected = format_ab_metrics(df, output_type=["delta_relative"])

    pd.testing.assert_frame_equal(result, expected)


def test_format_ab_metrics_supports_significant_delta_outputs() -> None:
    result = format_ab_metrics(
        _build_metric_rows(),
        output_type=["delta_relative_significant", "delta_absolute_significant"],
        significance_alpha=0.05,
        significance_p_value="p_values",
    )

    expected = pd.DataFrame(
        {
            "metric": ["orders", "gmv"],
            "test_vs_control_delta_relative_significant": [0.2, np.nan],
            "test_vs_control_delta_absolute_significant": [2.0, np.nan],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    ("significance_p_value", "expected_values"),
    [
        ("p_values", [0.2, np.nan]),
        ("p_values_cuped", [np.nan, 0.1]),
        ("p_values_adj", [0.2, np.nan]),
    ],
)
def test_format_ab_metrics_uses_configured_significance_p_value_source(
    significance_p_value: str,
    expected_values: list[float],
) -> None:
    df = _build_metric_rows()
    df["p-value"] = [0.04, 0.2]
    df["p-value CUPED"] = [0.2, 0.04]
    df["bootstrap_adj_p"] = [0.01, 0.2]

    result = format_ab_metrics(
        df,
        output_type=["delta_relative_significant"],
        significance_alpha=0.05,
        significance_p_value=significance_p_value,
    )

    expected = pd.DataFrame(
        {
            "metric": ["orders", "gmv"],
            "test_vs_control_delta_relative_significant": expected_values,
        }
    )
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"significance_p_value": "p_values"},
        {"significance_alpha": 0.05},
    ],
)
def test_format_ab_metrics_requires_significance_configuration(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="required"):
        format_ab_metrics(
            _build_metric_rows(),
            output_type=["delta_relative_significant"],
            **kwargs,
        )


def test_format_ab_metrics_validates_significance_configuration() -> None:
    with pytest.raises(ValueError, match="significance_alpha"):
        format_ab_metrics(
            _build_metric_rows(),
            output_type=["delta_relative_significant"],
            significance_alpha=1.0,
            significance_p_value="p_values",
        )

    with pytest.raises(ValueError, match="significance_p_value"):
        format_ab_metrics(
            _build_metric_rows(),
            output_type=["delta_relative_significant"],
            significance_alpha=0.05,
            significance_p_value="unknown",
        )


def test_format_ab_metrics_raises_for_missing_significance_p_value_source() -> None:
    df = _build_metric_rows().drop(columns=["p-value CUPED"])

    with pytest.raises(ValueError, match="Missing source column"):
        format_ab_metrics(
            df,
            output_type=["delta_relative_significant"],
            significance_alpha=0.05,
            significance_p_value="p_values_cuped",
        )


def test_format_ab_metrics_raises_for_duplicate_output_cells() -> None:
    df = pd.concat(
        [_build_metric_rows().iloc[[0]], _build_metric_rows().iloc[[0]]],
        ignore_index=True,
    )

    with pytest.raises(ValueError, match="Duplicate formatted output cell"):
        format_ab_metrics(df)


def test_format_ab_metrics_raises_for_missing_required_columns() -> None:
    df = _build_metric_rows().drop(columns=["group_1"])

    with pytest.raises(ValueError, match="Missing required column"):
        format_ab_metrics(df)


def test_format_ab_metrics_raises_for_missing_requested_optional_columns() -> None:
    df = _build_metric_rows().drop(columns=["p-value CUPED"])

    with pytest.raises(ValueError, match="Missing source column"):
        format_ab_metrics(df, output_type=["p_values_cuped"])


def test_format_ab_metrics_validates_output_type() -> None:
    with pytest.raises(ValueError, match="Unsupported output_type"):
        format_ab_metrics(_build_metric_rows(), output_type=["metric_values", "unknown"])


def test_format_ab_metrics_is_publicly_reexported() -> None:
    ab_utils_module = importlib.import_module("analytics_toolkit.ab_utils")
    metrics_module = importlib.import_module("analytics_toolkit.ab_utils.metrics")
    formatter_module = importlib.import_module("analytics_toolkit.ab_utils.formatter")

    assert ab_utils_module.format_ab_metrics is formatter_module.format_ab_metrics
    assert metrics_module.format_ab_metrics is formatter_module.format_ab_metrics
