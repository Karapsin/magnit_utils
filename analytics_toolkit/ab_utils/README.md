# analytics_toolkit.ab_utils

Helpers for AB-test related workflows.

## Available Helpers

- `compute_test_metrics`: compare experiment groups across all metric columns in a dataframe
- `parallel_compute_metrics`: run named `compute_test_metrics` tasks concurrently
- `parallel_compute_metrics_from_sql`: load named SQL-backed tasks, then run metric computation concurrently

## Notes

`compute_test_metrics` expects:

- one row per user
- a group-label column
- a unique user id column
- all remaining columns to be numeric metrics

Missing metric values are ignored on a per-metric basis.

Outlier handling is enabled by default. For each metric, an upper-tail cutoff is
computed once across all experiment groups from `outliers_quantile=0.999`.
Values above the cutoff are handled according to `outliers_policy`:

- `"truncate"` (default): cap values at the cutoff
- `"drop"`: treat values above the cutoff as missing

Mean metrics use their non-missing metric values for the cutoff. `level="user"`
ratio metrics use valid per-user ratios after denominator filtering. `level="agg"`
ratio metrics identify outlier rows from `numerator / denominator` only when the
row denominator is positive; denominator values `<= 0` keep the existing aggregate
ratio behavior and are not classified as row-ratio outliers. For aggregate ratios,
`"drop"` excludes outlier rows from numerator/denominator sums and variance, while
`"truncate"` replaces an outlier row numerator with `cutoff * denominator`.

The reported `mde_abs` and `mde_relative` use a normal approximation based on the
observed group variances and sample sizes.

The output also reports `variance_control`, `variance_test`, and `s.e.` for each
comparison. Mean metrics and `level="user"` ratio metrics use sample variances
with `ddof=1`; `level="agg"` ratio metrics use delta-method ratio variances.

Ratio metrics can be passed via `ratio_metrics`, for example:

```python
ratio_metrics = [
    {"name": "ctr", "numerator": "clicks", "denominator": "impressions"},
    {"name": "ctr_user", "numerator": "clicks", "denominator": "impressions", "level": "user"},
]
```

Supported ratio options:

- `level`: `"agg"` (default) or `"user"`
- `invalid_denominator`: `"ignore"` (default)

Ratio row filtering:

- `level="user"`: rows with missing numerator/denominator or `denominator <= 0` are ignored
- `level="agg"`: rows with missing numerator/denominator are ignored; zero denominators are kept and contribute to the aggregate sums

Other function options:

- `mde_alpha=0.05`
- `mde_power=0.80`
- `outliers_quantile=0.999`: upper-tail quantile used for the per-metric outlier cutoff
- `outliers_policy="truncate"`: either `"truncate"` or `"drop"`
- `pre_exp_metrics_df=None`: optional pre-experiment dataframe used to compute CUPED-adjusted standard errors and p-values
- `test_vs_test=True`: when `False`, only compare each test group against control
- `multiple_comparisons_adjustment=False`: when `True`, add `s.e. bootstrap` and `bootstrap_adj_p`
- `multiple_comparisons_adjustment_resamples=2000`: number of bootstrap resamples for `s.e. bootstrap` and `bootstrap_adj_p`
- `bootstrap_random_state=0`: bootstrap RNG seed; set `None` for non-deterministic resampling
- `bootstrap_n_jobs=1`: number of worker executors for bootstrap batches
- `bootstrap_progress=False`: when `True`, show a `tqdm` progress bar for bootstrap resamples

Run independent metric jobs in parallel with `parallel_compute_metrics`:

```python
result = parallel_compute_metrics(
    {
        "segment_1": {
            "df": segment_1_df,
            "pre_exp_df": segment_1_pre_df,
            "labels": {"segment": "segment1"},
            "test_vs_test": False,
        },
        "segment_2": {
            "df": segment_2_df,
            "labels": {"segment": "segment2"},
            "test_vs_test": False,
        },
    },
    concurrency=2,
)
```

Load each task dataframe from the same SQL connection alias with
`parallel_compute_metrics_from_sql`:

```python
result = parallel_compute_metrics_from_sql(
    {
        "segment_1": {
            "sql": "select * from mart.ab_segment_1",
            "pre_exp_sql": "select * from mart.ab_segment_1_pre",
            "labels": {"segment": "segment1"},
            "test_vs_test": False,
        },
        "segment_2": {
            "sql": "select * from mart.ab_segment_2",
            "labels": {"segment": "segment2"},
            "test_vs_test": False,
        },
    },
    db="analytics_prod",
    concurrency=2,
)
```

Output notes:

- ratio metric names use the provided ratio metric `name` directly
- `metric_type` is `"mean"` for regular metrics and `"ratio"` for ratio metrics
- `group_1` and `group_2` are included when there are more than two experiment groups
- `metric_control` and `metric_test` contain the metric value in the baseline and test groups
- `outliers_cutoff` contains the global metric cutoff used for the comparison
- `outliers_n_control` and `outliers_n_test` count values or rows above the cutoff in the baseline and test groups
- `variance_control` and `variance_test` contain the uncertainty variance inputs for each group
- `s.e.` is the standard error of `delta_abs`
- `delta_relative` and `mde_relative` are raw relative changes, e.g. `0.05` for 5%
- when `pre_exp_metrics_df` is provided, `s.e. CUPED` and `p-value CUPED` are added after `p-value`
- when `multiple_comparisons_adjustment=True`, `s.e. bootstrap` and `bootstrap_adj_p` are added after CUPED columns, if any

`pre_exp_metrics_df` requirements:

- it must contain the same `group` and `user_id` columns used for the main call
- it must contain the control label in the same group column
- overlapping `user_id` values must map to the same experiment group in both dataframes
- if a metric cannot be built from the pre-experiment dataframe, `s.e. CUPED` and `p-value CUPED` are set to `NaN` and a warning is emitted

`bootstrap_adj_p` is computed per metric using a bootstrap max-statistic procedure:

- rows are resampled with replacement from the observed dataframe
- each sampled row keeps its original group label
- for each metric, the maximum absolute comparison statistic across enabled comparisons is collected
- `bootstrap_adj_p` is the share of bootstrap max-statistics at least as large as the observed absolute statistic
- `s.e. bootstrap` is the sample standard deviation of bootstrapped `delta_abs` estimates for the same metric/comparison

This is a bootstrap-based empirical adjustment on the observed grouped data. It should
be interpreted as a heuristic bootstrap-adjusted significance/stability measure rather
than a strict null-calibrated multiple-testing p-value.
