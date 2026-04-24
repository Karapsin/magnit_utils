from __future__ import annotations

import math
import warnings

import numpy as np
import pandas as pd
from scipy.stats import norm, ttest_ind

from .constants import DEFAULT_ALPHA, DEFAULT_POWER


def _get_numeric_metric_series(df: pd.DataFrame, metric_name: str) -> pd.Series:
    original = df[metric_name]
    numeric = pd.to_numeric(original, errors="coerce")

    if numeric.notna().sum() != original.notna().sum():
        raise TypeError(f"Metric column '{metric_name}' contains non-numeric values.")

    numeric.name = metric_name
    return numeric


def _safe_mean(values: pd.Series) -> float:
    if values.empty:
        return math.nan
    return float(values.mean())


def _compute_ttest_stat_and_p_value(
    baseline_values: pd.Series,
    test_values: pd.Series,
) -> tuple[float, float]:
    if baseline_values.shape[0] < 2 or test_values.shape[0] < 2:
        return math.nan, math.nan

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        result = ttest_ind(test_values, baseline_values, equal_var=False, nan_policy="omit")
    return float(result.statistic), float(result.pvalue)


def _compute_mde_abs(
    baseline_values: pd.Series,
    test_values: pd.Series,
    alpha: float = DEFAULT_ALPHA,
    power: float = DEFAULT_POWER,
) -> float:
    n0 = baseline_values.shape[0]
    n1 = test_values.shape[0]
    if n0 < 2 or n1 < 2:
        return math.nan

    variance0 = float(baseline_values.var(ddof=1))
    variance1 = float(test_values.var(ddof=1))
    if math.isnan(variance0) or math.isnan(variance1):
        return math.nan

    z_alpha = float(norm.ppf(1 - alpha / 2))
    z_power = float(norm.ppf(power))
    return (z_alpha + z_power) * math.sqrt((variance0 / n0) + (variance1 / n1))


def _compute_normal_p_value(delta_abs: float, standard_error: float) -> float:
    statistic = _compute_studentized_statistic(delta_abs, standard_error)
    if math.isnan(statistic):
        return math.nan
    return float(2 * norm.sf(abs(statistic)))


def _compute_mde_from_standard_error(
    standard_error: float,
    alpha: float,
    power: float,
) -> float:
    if math.isnan(standard_error) or standard_error <= 0:
        return math.nan
    z_alpha = float(norm.ppf(1 - alpha / 2))
    z_power = float(norm.ppf(power))
    return (z_alpha + z_power) * standard_error


def _compute_studentized_statistic(delta_abs: float, standard_error: float) -> float:
    if math.isnan(delta_abs) or math.isnan(standard_error) or standard_error <= 0:
        return math.nan
    return delta_abs / standard_error


def _compute_ttest_stat_and_p_value_arrays(
    baseline_values: np.ndarray,
    test_values: np.ndarray,
) -> tuple[float, float]:
    return _compute_ttest_stat_and_p_value(
        pd.Series(baseline_values),
        pd.Series(test_values),
    )


def _safe_relative(numerator: float, denominator: float) -> float:
    if math.isnan(numerator) or math.isnan(denominator) or denominator == 0:
        return math.nan
    return numerator / denominator


def _both_present(left: float, right: float) -> bool:
    return not math.isnan(left) and not math.isnan(right)
