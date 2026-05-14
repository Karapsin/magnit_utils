from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .ratio import _build_ratio_valid_mask
from .stats import _get_numeric_metric_series


def _build_outlier_contexts(
    df: pd.DataFrame,
    metric_definitions: list[dict[str, object]],
    outliers_quantile: float,
    outliers_policy: str,
    *,
    allow_missing: bool = False,
) -> dict[str, dict[str, object]]:
    contexts: dict[str, dict[str, object]] = {}
    for metric_definition in metric_definitions:
        context = _build_outlier_context(
            df=df,
            metric_definition=metric_definition,
            outliers_quantile=outliers_quantile,
            outliers_policy=outliers_policy,
            allow_missing=allow_missing,
        )
        if context is not None:
            contexts[str(metric_definition["metric_key"])] = context
    return contexts


def _build_outlier_context(
    df: pd.DataFrame,
    metric_definition: dict[str, object],
    outliers_quantile: float,
    outliers_policy: str,
    *,
    allow_missing: bool = False,
) -> dict[str, object] | None:
    metric_key = str(metric_definition["metric_key"])
    if metric_definition["kind"] == "mean":
        metric_name = str(metric_definition["column"])
        if metric_name not in df.columns:
            if allow_missing:
                return None
            raise KeyError(metric_name)
        values = _get_numeric_metric_series(df, metric_name)
        return {
            "kind": "mean",
            "metric_key": metric_key,
            "cutoff": _compute_outlier_cutoff(values, outliers_quantile),
            "policy": outliers_policy,
        }

    ratio_spec = dict(metric_definition["ratio_spec"])
    numerator_column = ratio_spec["numerator"]
    denominator_column = ratio_spec["denominator"]
    missing_columns = [
        column for column in (numerator_column, denominator_column) if column not in df.columns
    ]
    if missing_columns:
        if allow_missing:
            return None
        raise KeyError(missing_columns[0])

    numerator = _get_numeric_metric_series(df, numerator_column)
    denominator = _get_numeric_metric_series(df, denominator_column)
    if ratio_spec["level"] == "user":
        valid_mask = _build_ratio_valid_mask(
            numerator=numerator,
            denominator=denominator,
            level=ratio_spec["level"],
        )
        values = pd.Series(np.nan, index=df.index, dtype=float)
        values.loc[valid_mask] = numerator.loc[valid_mask] / denominator.loc[valid_mask]
    else:
        valid_mask = numerator.notna() & denominator.notna() & (denominator > 0)
        values = pd.Series(np.nan, index=df.index, dtype=float)
        values.loc[valid_mask] = numerator.loc[valid_mask] / denominator.loc[valid_mask]

    return {
        "kind": "ratio",
        "level": ratio_spec["level"],
        "metric_key": metric_key,
        "cutoff": _compute_outlier_cutoff(values, outliers_quantile),
        "policy": outliers_policy,
    }


def _compute_outlier_cutoff(values: pd.Series, outliers_quantile: float) -> float:
    nonmissing_values = values.dropna()
    if nonmissing_values.empty:
        return math.nan
    return float(nonmissing_values.quantile(outliers_quantile))


def _apply_outliers_to_values(
    values: pd.Series,
    outlier_context: dict[str, object] | None,
) -> tuple[pd.Series, pd.Series]:
    transformed = values.astype(float).copy()
    outlier_mask = _build_value_outlier_mask(transformed, outlier_context)
    if outlier_mask.any() and outlier_context is not None:
        cutoff = float(outlier_context["cutoff"])
        policy = str(outlier_context["policy"])
        if policy == "truncate":
            transformed.loc[outlier_mask] = cutoff
        else:
            transformed.loc[outlier_mask] = np.nan
    return transformed, outlier_mask


def _apply_outliers_to_agg_ratio_components(
    numerator: pd.Series,
    denominator: pd.Series,
    outlier_context: dict[str, object] | None,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    transformed_numerator = numerator.astype(float).copy()
    transformed_denominator = denominator.astype(float).copy()
    outlier_mask = _build_agg_ratio_outlier_mask(
        numerator=numerator,
        denominator=denominator,
        outlier_context=outlier_context,
    )
    if outlier_mask.any() and outlier_context is not None:
        cutoff = float(outlier_context["cutoff"])
        policy = str(outlier_context["policy"])
        if policy == "truncate":
            transformed_numerator.loc[outlier_mask] = cutoff * denominator.loc[outlier_mask]
        else:
            transformed_numerator.loc[outlier_mask] = np.nan
            transformed_denominator.loc[outlier_mask] = np.nan
    return transformed_numerator, transformed_denominator, outlier_mask


def _build_value_outlier_mask(
    values: pd.Series,
    outlier_context: dict[str, object] | None,
) -> pd.Series:
    outlier_mask = pd.Series(False, index=values.index)
    if outlier_context is None:
        return outlier_mask
    cutoff = float(outlier_context["cutoff"])
    if math.isnan(cutoff):
        return outlier_mask
    return values.notna() & (values > cutoff)


def _build_agg_ratio_outlier_mask(
    numerator: pd.Series,
    denominator: pd.Series,
    outlier_context: dict[str, object] | None,
) -> pd.Series:
    outlier_mask = pd.Series(False, index=numerator.index)
    if outlier_context is None:
        return outlier_mask
    cutoff = float(outlier_context["cutoff"])
    if math.isnan(cutoff):
        return outlier_mask

    candidate_mask = numerator.notna() & denominator.notna() & (denominator > 0)
    row_ratios = numerator.loc[candidate_mask] / denominator.loc[candidate_mask]
    outlier_mask.loc[candidate_mask] = row_ratios > cutoff
    return outlier_mask


def _count_outliers_by_group(
    outlier_mask: pd.Series,
    group_values: pd.Series,
    baseline_group: str,
    test_group: str,
) -> tuple[int, int]:
    baseline_count = int(outlier_mask[group_values == baseline_group].sum())
    test_count = int(outlier_mask[group_values == test_group].sum())
    return baseline_count, test_count


def _get_outlier_cutoff(outlier_context: dict[str, object] | None) -> float:
    if outlier_context is None:
        return math.nan
    return float(outlier_context["cutoff"])
