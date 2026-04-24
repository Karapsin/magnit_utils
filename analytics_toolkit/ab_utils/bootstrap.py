from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import math

import numpy as np
import pandas as pd
from tqdm import tqdm

from .ratio import (
    _build_ratio_frame_from_arrays,
    _build_ratio_valid_mask_from_arrays,
    _compute_agg_ratio_diff_standard_error,
    _compute_agg_ratio_group_stats_arrays,
)
from .stats import (
    _both_present,
    _compute_studentized_statistic,
    _compute_ttest_stat_and_p_value_arrays,
    _get_numeric_metric_series,
)


def _apply_multiple_comparisons_adjustment(
    rows: list[dict[str, object]],
    df: pd.DataFrame,
    group_column: str,
    metric_definitions: list[dict[str, object]],
    comparisons: list[tuple[str, str]],
    resamples: int,
    random_state: int | None,
    n_jobs: int,
    show_progress: bool,
) -> None:
    if not rows:
        return

    bootstrap_context = _prepare_bootstrap_context(
        df=df,
        group_column=group_column,
        metric_definitions=metric_definitions,
        comparisons=comparisons,
    )
    family_max_statistics = _compute_bootstrap_family_max_statistics(
        bootstrap_context=bootstrap_context,
        resamples=resamples,
        random_state=random_state,
        n_jobs=n_jobs,
        show_progress=show_progress,
    )

    for row in rows:
        observed_stat = row.get("_test_stat")
        metric_key = str(row.get("_metric_key"))
        if not isinstance(observed_stat, (int, float)) or math.isnan(float(observed_stat)):
            row["bootstrap_adj_p"] = math.nan
            continue

        bootstrap_stats = [
            value
            for value in family_max_statistics.get(metric_key, [])
            if not math.isnan(value)
        ]
        if not bootstrap_stats:
            row["bootstrap_adj_p"] = math.nan
            continue

        observed_abs_stat = abs(float(observed_stat))
        exceedances = sum(value >= observed_abs_stat for value in bootstrap_stats)
        row["bootstrap_adj_p"] = exceedances / len(bootstrap_stats)


def _prepare_bootstrap_context(
    df: pd.DataFrame,
    group_column: str,
    metric_definitions: list[dict[str, object]],
    comparisons: list[tuple[str, str]],
) -> dict[str, object]:
    group_values = df[group_column].to_numpy()
    group_names = list(dict.fromkeys(group_values.tolist()))
    group_code_by_name = {name: code for code, name in enumerate(group_names)}
    group_codes = np.array([group_code_by_name[value] for value in group_values], dtype=np.int16)

    metric_contexts: list[dict[str, object]] = []
    for metric_definition in metric_definitions:
        metric_key = str(metric_definition["metric_key"])
        if metric_definition["kind"] == "mean":
            metric_contexts.append(
                {
                    "kind": "mean",
                    "metric_key": metric_key,
                    "values": _get_numeric_metric_series(
                        df, str(metric_definition["column"])
                    ).to_numpy(dtype=float),
                }
            )
            continue

        ratio_spec = dict(metric_definition["ratio_spec"])
        numerator = _get_numeric_metric_series(df, ratio_spec["numerator"]).to_numpy(dtype=float)
        denominator = _get_numeric_metric_series(df, ratio_spec["denominator"]).to_numpy(
            dtype=float
        )
        valid_mask = _build_ratio_valid_mask_from_arrays(
            numerator=numerator,
            denominator=denominator,
            level=ratio_spec["level"],
        )

        ratio_context: dict[str, object] = {
            "kind": "ratio",
            "metric_key": metric_key,
            "level": ratio_spec["level"],
            "numerator": numerator,
            "denominator": denominator,
            "valid_mask": valid_mask,
        }
        if ratio_spec["level"] == "user":
            ratio_values = np.full(numerator.shape[0], np.nan, dtype=float)
            ratio_values[valid_mask] = numerator[valid_mask] / denominator[valid_mask]
            ratio_context["values"] = ratio_values
        metric_contexts.append(ratio_context)

    return {
        "group_codes": group_codes,
        "metric_contexts": metric_contexts,
        "comparisons": [
            (group_code_by_name[test_group], group_code_by_name[baseline_group])
            for test_group, baseline_group in comparisons
        ],
    }


def _compute_bootstrap_family_max_statistics(
    bootstrap_context: dict[str, object],
    resamples: int,
    random_state: int | None,
    n_jobs: int,
    show_progress: bool,
) -> dict[str, list[float]]:
    metric_keys = [
        str(metric_context["metric_key"])
        for metric_context in list(bootstrap_context["metric_contexts"])
    ]
    family_max_statistics: dict[str, list[float]] = {metric_key: [] for metric_key in metric_keys}

    batch_sizes = _split_resamples_into_batches(resamples, n_jobs=n_jobs)
    if not batch_sizes:
        return family_max_statistics

    if n_jobs == 1 or len(batch_sizes) == 1:
        rng = np.random.default_rng(random_state)
        batch_result = _compute_bootstrap_family_max_statistics_batch(
            bootstrap_context=bootstrap_context,
            resamples=batch_sizes[0],
            rng_or_seed=rng,
            progress_position=0 if show_progress else None,
        )
        for metric_key, values in batch_result.items():
            family_max_statistics[metric_key].extend(values)
        return family_max_statistics

    seed_sequence = np.random.SeedSequence(random_state)
    child_sequences = seed_sequence.spawn(len(batch_sizes))
    try:
        batch_results = _compute_bootstrap_family_max_statistics_in_executor(
            executor_cls=ProcessPoolExecutor,
            bootstrap_context=bootstrap_context,
            batch_sizes=batch_sizes,
            child_sequences=child_sequences,
            n_jobs=n_jobs,
            show_progress=show_progress,
        )
    except (NotImplementedError, PermissionError, OSError):
        batch_results = _compute_bootstrap_family_max_statistics_in_executor(
            executor_cls=ThreadPoolExecutor,
            bootstrap_context=bootstrap_context,
            batch_sizes=batch_sizes,
            child_sequences=child_sequences,
            n_jobs=n_jobs,
            show_progress=show_progress,
        )

    for batch_result in batch_results:
        for metric_key, values in batch_result.items():
            family_max_statistics[metric_key].extend(values)

    return family_max_statistics


def _compute_bootstrap_family_max_statistics_in_executor(
    executor_cls: type[ProcessPoolExecutor] | type[ThreadPoolExecutor],
    bootstrap_context: dict[str, object],
    batch_sizes: list[int],
    child_sequences: list[np.random.SeedSequence],
    n_jobs: int,
    show_progress: bool,
) -> list[dict[str, list[float]]]:
    with executor_cls(max_workers=n_jobs) as executor:
        futures = [
            executor.submit(
                _compute_bootstrap_family_max_statistics_batch,
                bootstrap_context,
                batch_size,
                child_sequence,
                index if show_progress else None,
            )
            for index, (batch_size, child_sequence) in enumerate(
                zip(batch_sizes, child_sequences, strict=True)
            )
        ]
        return [future.result() for future in futures]


def _split_resamples_into_batches(resamples: int, n_jobs: int) -> list[int]:
    batch_count = min(resamples, max(1, n_jobs))
    base_batch_size, remainder = divmod(resamples, batch_count)
    return [
        base_batch_size + (1 if batch_index < remainder else 0)
        for batch_index in range(batch_count)
        if base_batch_size + (1 if batch_index < remainder else 0) > 0
    ]


def _compute_bootstrap_family_max_statistics_batch(
    bootstrap_context: dict[str, object],
    resamples: int,
    rng_or_seed: np.random.Generator | np.random.SeedSequence,
    progress_position: int | None = None,
) -> dict[str, list[float]]:
    rng = (
        rng_or_seed
        if isinstance(rng_or_seed, np.random.Generator)
        else np.random.default_rng(rng_or_seed)
    )
    group_codes = np.asarray(bootstrap_context["group_codes"], dtype=np.int16)
    comparisons = list(bootstrap_context["comparisons"])
    metric_contexts = list(bootstrap_context["metric_contexts"])

    family_max_statistics: dict[str, list[float]] = {
        str(metric_context["metric_key"]): [] for metric_context in metric_contexts
    }
    sample_size = group_codes.shape[0]

    iterator = range(resamples)
    if progress_position is not None:
        iterator = tqdm(
            iterator,
            total=resamples,
            desc="bootstrap",
            position=progress_position,
            leave=(progress_position == 0),
        )

    for _ in iterator:
        sample_indices = rng.integers(0, sample_size, size=sample_size)
        sampled_group_codes = group_codes[sample_indices]
        iteration_max_stats = _compute_metric_family_max_statistics_from_indices(
            metric_contexts=metric_contexts,
            sampled_group_codes=sampled_group_codes,
            sample_indices=sample_indices,
            comparisons=comparisons,
        )
        for metric_key, max_stat in iteration_max_stats.items():
            family_max_statistics[metric_key].append(max_stat)

    return family_max_statistics


def _compute_metric_family_max_statistics_from_indices(
    metric_contexts: list[dict[str, object]],
    sampled_group_codes: np.ndarray,
    sample_indices: np.ndarray,
    comparisons: list[tuple[int, int]],
) -> dict[str, float]:
    max_statistics: dict[str, float] = {}
    for metric_context in metric_contexts:
        metric_key = str(metric_context["metric_key"])
        comparison_statistics: list[float] = []
        for test_group_code, baseline_group_code in comparisons:
            statistic = _compute_metric_test_statistic_from_indices(
                metric_context=metric_context,
                sampled_group_codes=sampled_group_codes,
                sample_indices=sample_indices,
                baseline_group_code=baseline_group_code,
                test_group_code=test_group_code,
            )
            if not math.isnan(statistic):
                comparison_statistics.append(abs(statistic))
        if comparison_statistics:
            max_statistics[metric_key] = max(comparison_statistics)
        else:
            max_statistics[metric_key] = math.nan
    return max_statistics


def _compute_metric_test_statistic_from_indices(
    metric_context: dict[str, object],
    sampled_group_codes: np.ndarray,
    sample_indices: np.ndarray,
    baseline_group_code: int,
    test_group_code: int,
) -> float:
    baseline_mask = sampled_group_codes == baseline_group_code
    test_mask = sampled_group_codes == test_group_code

    if metric_context["kind"] == "mean":
        sampled_values = np.asarray(metric_context["values"], dtype=float)[sample_indices]
        baseline_values = sampled_values[baseline_mask & ~np.isnan(sampled_values)]
        test_values = sampled_values[test_mask & ~np.isnan(sampled_values)]
        statistic, _ = _compute_ttest_stat_and_p_value_arrays(baseline_values, test_values)
        return statistic

    if metric_context["level"] == "user":
        sampled_values = np.asarray(metric_context["values"], dtype=float)[sample_indices]
        baseline_values = sampled_values[baseline_mask & ~np.isnan(sampled_values)]
        test_values = sampled_values[test_mask & ~np.isnan(sampled_values)]
        statistic, _ = _compute_ttest_stat_and_p_value_arrays(baseline_values, test_values)
        return statistic

    sampled_numerator = np.asarray(metric_context["numerator"], dtype=float)[sample_indices]
    sampled_denominator = np.asarray(metric_context["denominator"], dtype=float)[sample_indices]
    sampled_valid_mask = np.asarray(metric_context["valid_mask"], dtype=bool)[sample_indices]

    baseline_valid_mask = baseline_mask & sampled_valid_mask
    test_valid_mask = test_mask & sampled_valid_mask

    baseline_stats = _compute_agg_ratio_group_stats_arrays(
        sampled_numerator[baseline_valid_mask],
        sampled_denominator[baseline_valid_mask],
    )
    test_stats = _compute_agg_ratio_group_stats_arrays(
        sampled_numerator[test_valid_mask],
        sampled_denominator[test_valid_mask],
    )
    if not _both_present(test_stats["ratio"], baseline_stats["ratio"]):
        return math.nan
    delta_abs = test_stats["ratio"] - baseline_stats["ratio"]
    se_diff = _compute_agg_ratio_diff_standard_error(
        baseline_frame=_build_ratio_frame_from_arrays(
            sampled_numerator[baseline_valid_mask], sampled_denominator[baseline_valid_mask]
        ),
        baseline_ratio=baseline_stats["ratio"],
        test_frame=_build_ratio_frame_from_arrays(
            sampled_numerator[test_valid_mask], sampled_denominator[test_valid_mask]
        ),
        test_ratio=test_stats["ratio"],
    )
    return _compute_studentized_statistic(delta_abs, se_diff)
