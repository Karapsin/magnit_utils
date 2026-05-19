# analytics_toolkit.ab_utils

Helpers for AB-test related workflows.

## Available Helpers

- `compute_test_metrics`: compare experiment groups across all metric columns in a dataframe
- `do_split`: deterministically sample users and assign AB groups, with optional exact-value stratification
- `format_ab_metrics`: reshape metric comparison output into a wide presentation table
- `parallel_compute_metrics`: run named `compute_test_metrics` tasks concurrently
- `parallel_compute_metrics_from_sql`: load named SQL-backed tasks, then run metric computation concurrently

## Notes

`do_split` creates deterministic AB group assignments:

```python
split_df = do_split(
    users_df,
    split_col="user_id",
    stratification_cols=["country", "platform"],
    target_sample_size=100_000,
    test_groups_num=2,
    test_group_ratios=[100 / 6, (100 / 6) * 2, (100 / 6) * 3],
    random_state=42,
)
```

The result contains sampled input rows plus `group_name` and
`is_mandatory_user`. Groups are named `control`, `test_1`, ..., `test_N`.
When `test_group_ratios` is omitted, all groups are equal size. Custom ratios
must be ordered as `[control, test_1, ...]`, contain positive numeric values,
and sum to `100`.

`mandatory_users_df` can be used to guarantee selected users are included. It
must contain the same id column as `split_col`; ids missing from `df` are
warned about and ignored. `mandatory_users_group` supports:

- `"any"`: mandatory users are included and assigned like regular sampled users
- `"control"`: mandatory users are forced into control
- `"test_any"`: mandatory users are split across test groups
- `"test_1"`, `"test_2"`, etc.: mandatory users are forced into that exact test group

With `compensate_mandatory_users=False`, group ratios are applied to randomized
users and forced mandatory users are added afterward. With
`compensate_mandatory_users=True`, group ratios are applied to final counts
including mandatory users; impossible final quotas raise `ValueError`.

Stratification uses exact tuples of `stratification_cols`; missing values share
a stable missing bucket. Rare strata are still assigned randomly while global
group quotas are preserved.

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
            "start_comment": "/* segment_2 metrics */",
        },
    },
    db="analytics_prod",
    concurrency=2,
    start_comment="/* ab metrics batch */",
)
```

The top-level `start_comment` is passed to the SQL reads created for each task.
A task-level `start_comment` overrides it and applies to both `sql` and
`pre_exp_sql` for that metrics task.

Format metric comparison output for presentation with `format_ab_metrics`:

```python
formatted = format_ab_metrics(
    result["segment_1"],
    label_cols=["segment"],
    output_type=["metric_values", "p_values", "delta_relative_significant"],
    significance_alpha=0.05,
    significance_p_value="p_values",
)
```

With the default `output_type`, the result is a wide table with label columns,
`metric`, and one metric-value column per experiment group. Additional output
types add comparison columns such as `test_vs_control_p_value` and
`test_vs_control_delta_relative`. `output_type` accepts either one output name
or a list of output names. Significant delta outputs add columns such as
`test_vs_control_delta_relative_significant` and keep the delta only when the
configured p-value is below `significance_alpha`; otherwise they return `NaN`.
Use `significance_p_value="p_values"`, `"p_values_cuped"`, or `"p_values_adj"`
to choose the p-value source.

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
- `delta_relative_significant` and `delta_absolute_significant` format `delta_relative`
  and `delta_abs` only when the configured p-value is significant
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
