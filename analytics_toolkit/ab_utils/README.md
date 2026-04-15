# analytics_toolkit.ab_utils

Helpers for AB-test related workflows.

## Available Helpers

- `compute_test_metrics`: compare experiment groups across all metric columns in a dataframe

## Notes

`compute_test_metrics` expects:

- one row per user
- a group-label column
- a unique user id column
- all remaining columns to be numeric metrics

Missing metric values are ignored on a per-metric basis.

The reported `mde_abs` and `mde_percentage` use a normal approximation based on the
observed group variances and sample sizes.

Ratio metrics can be passed via `ratio_metrics`, for example:

```python
ratio_metrics = [
    {"name": "ctr", "numerator": "clicks", "denominator": "impressions"},
    {"name": "ctr_agg", "numerator": "clicks", "denominator": "impressions", "level": "agg"},
]
```

Supported ratio options:

- `level`: `"user"` (default) or `"agg"`
- `invalid_denominator`: `"ignore"` (default)

Ratio row filtering:

- `level="user"`: rows with missing numerator/denominator or `denominator <= 0` are ignored
- `level="agg"`: rows with missing numerator/denominator are ignored; zero denominators are kept and contribute to the aggregate sums

Other function options:

- `mde_alpha=0.05`
- `mde_power=0.80`
- `test_vs_test=True`: when `False`, only compare each test group against control
- `multiple_comparisons_adjustment=False`: when `True`, add `bootstrap_adj_p`
- `multiple_comparisons_adjustment_resamples=2000`: number of bootstrap resamples for `bootstrap_adj_p`

Output notes:

- ratio metric names are prefixed as `"[ratio] metric_name"`
- `groups` is included when there are more than two experiment groups

`bootstrap_adj_p` is computed per metric using a bootstrap max-statistic procedure:

- rows are resampled with replacement from the observed dataframe
- each sampled row keeps its original group label
- for each metric, the maximum absolute comparison statistic across enabled comparisons is collected
- `bootstrap_adj_p` is the share of bootstrap max-statistics at least as large as the observed absolute statistic

This is a bootstrap-based empirical adjustment on the observed grouped data. It should
be interpreted as a heuristic bootstrap-adjusted significance/stability measure rather
than a strict null-calibrated multiple-testing p-value.
