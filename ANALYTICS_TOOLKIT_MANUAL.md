# analytics_toolkit Manual

This manual describes only the public functions exported by the main modules:

- `analytics_toolkit.sql`
- `analytics_toolkit.dates`
- `analytics_toolkit.general`
- `analytics_toolkit.excel`
- `analytics_toolkit.ab_utils`

Examples were aligned to real usage patterns found in `../tickets/april_2026`. There is no `tickets/april_2024` directory in this workspace.

## Module: `sql`

Typical import:

```python
from analytics_toolkit import sql
```

Supported database keys:

- `'trino'`
- `'gp'`
- `'ch'`

### `sql.read(connection_type, query, print_queries=True, retry_cnt=5, timeout_increment=5)`

Runs one SQL query and returns a dataframe.

Inputs:

- `connection_type`: database key
- `query`: SQL query string
- `print_queries`: if `True`, prints the SQL before execution
- `retry_cnt`: number of retry attempts
- `timeout_increment`: wait time multiplier between retries

Returns:

- `pandas.DataFrame`

When to use:

- When you want to load query results into pandas

Real usage patterns from tickets:

```python
df = sql.read("gp", read_file(here("read_user_table.sql"), params_dict=params_dict))
```

```python
sql.read("trino", "select * from iceberg.pa_core_sandbox.some_table").to_excel(here("load.xlsx"))
```

### `sql.execute(connection_type, query, random_sleep_seconds=5, print_queries=True, gp_break_query=False, gp_commit_each_statement=False, retry_cnt=5, timeout_increment=5)`

Runs SQL that does not need to return a dataframe.

Inputs:

- `connection_type`: database key
- `query`: SQL string
- `random_sleep_seconds`: average pause between statements when a query is split into multiple statements; set `None` to disable
- `print_queries`: if `True`, prints the SQL before execution
- `gp_break_query`: for Greenplum, if `True`, splits multi-statement SQL into separate statements
- `gp_commit_each_statement`: for Greenplum, if `True`, commits after each statement
- `retry_cnt`: number of retry attempts
- `timeout_increment`: wait time multiplier between retries

Returns:

- Nothing useful

When to use:

- For `CREATE`, `DROP`, `INSERT`, `DELETE`, maintenance queries, or SQL scripts

Real usage patterns from tickets:

```python
sql.execute("gp", read_file(here("create_table.sql")), gp_break_query=True, random_sleep_seconds=None)
```

```python
sql.execute("trino", read_file(here("final_agg.sql")))
```

### `sql.transfer(from_db, to_db, from_sql, to_table, replace_target_table=True, batch_size=100_000, retry_cnt=5, timeout_increment=5, full_retry_cnt=5, full_timeout_increment=600, key_columns=None, gp_distributed_by_key=None)`

Transfers query results from one database to another.

Inputs:

- `from_db`: source database key
- `to_db`: target database key
- `from_sql`: source SQL query
- `to_table`: target table name
- `replace_target_table`: if `True`, replaces the target on the first load
- `batch_size`: rows per transfer batch
- `retry_cnt`: retry count for read and insert steps
- `timeout_increment`: wait time multiplier between retries
- `full_retry_cnt`: retry count for full transfer restarts
- `full_timeout_increment`: wait time multiplier for full transfer restarts
- `key_columns`: optional key columns used for duplicate protection
- `gp_distributed_by_key`: optional Greenplum distribution key list

Returns:

- Number of transferred rows

When to use:

- When data should be copied directly from one backend to another

Real usage patterns from tickets:

```python
sql.transfer(
    from_db="ch",
    to_db="trino",
    to_table="iceberg.pa_core_sandbox.karapsin_pal_4423_ch_load3",
    from_sql=read_query,
    replace_target_table=create_flg,
    batch_size=100_000,
)
```

```python
sql.transfer(
    from_db="ch",
    to_db="gp",
    from_sql=read_file(here("get_push_data.sql"), params_dict={"start_dt": start_dt, "end_dt": end_dt}),
    replace_target_table=replace_flg,
    to_table="cvm_sbx.karapsin_mal3657_marketing_push_load",
    batch_size=1_000_000,
)
```

### `sql.load_df(connection_type, destination_table, df, append=False, gp_distributed_by_key=None, key_columns=None, retry_cnt=5, timeout_increment=5)`

Loads a pandas dataframe into a database table.

Inputs:

- `connection_type`: target database key
- `destination_table`: target table name
- `df`: dataframe to upload
- `append`: if `True`, appends instead of replacing
- `gp_distributed_by_key`: optional Greenplum distribution key list
- `key_columns`: optional key columns used to protect against duplicates during append
- `retry_cnt`: retry count
- `timeout_increment`: wait time multiplier between retries

Returns:

- Number of inserted rows

When to use:

- When your data already exists in pandas and should be written to SQL

Real usage pattern from tickets:

```python
sql.load_df("trino", "iceberg.pa_core_sandbox.karapsin_test_table", final_df)
```

### `sql.gp_vacuum(table_name, analyze=False, full=False, verbose=True)`

Runs Greenplum `VACUUM` on a table.

Inputs:

- `table_name`: table to vacuum
- `analyze`: if `True`, also updates statistics
- `full`: if `True`, runs `VACUUM FULL`
- `verbose`: if `True`, includes verbose output

Returns:

- Nothing

When to use:

- After heavy Greenplum inserts or maintenance work

Real usage pattern from tickets:

```python
sql.gp_vacuum("cvm_sbx.karapsin_mal3657_fctx_sales_subset", full=True, analyze=True, verbose=True)
```

### `sql.with_sql_connection(connection_type)`

Decorator that opens a connection, passes it into your function, and closes it afterwards.

Inputs:

- `connection_type`: database key

Returns:

- Decorator

When to use:

- When you want your own helper function to receive a managed connection

Example:

```python
@sql.with_sql_connection("gp")
def run_check(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"select count(*) from {table_name}")
        return cur.fetchone()[0]
```

## Module: `dates`

Typical import:

```python
from analytics_toolkit import dates as dt
```

Accepted date inputs:

- ISO date string like `"2026-03-15"`
- `datetime.date`
- `datetime.datetime`

Most functions return a string by default. Set `output_string=False` to get a `datetime`.

### `dt.gen_dates_list(start_dt, end_dt, interval="days", output_string=True)`

Builds a list of dates between two boundaries, inclusive.

Inputs:

- `start_dt`: first date
- `end_dt`: last date
- `interval`: `"days"`, `"weeks"`, or `"months"`
- `output_string`: if `True`, returns strings; otherwise returns datetimes

Returns:

- List of dates

Notes:

- For `weeks`, dates are aligned to week starts
- For `months`, dates are aligned to month starts
- Returns an empty list if `end_dt < start_dt`

Example:

```python
days_list = dt.gen_dates_list("2026-03-01", "2026-03-31", interval="days")
```

### `dt.first_day(dt_value, period="month", output_string=True)`

Returns the first day of the week or month.

Inputs:

- `dt_value`: input date
- `period`: `"week"` or `"month"`
- `output_string`: output format flag

Returns:

- First day of the selected period

Example:

```python
month_start = dt.first_day("2026-03-18", "month")
```

### `dt.last_day(dt_value, period="month", output_string=True)`

Returns the last day of the week or month.

Inputs:

- `dt_value`: input date
- `period`: `"week"` or `"month"`
- `output_string`: output format flag

Returns:

- Last day of the selected period

Example:

```python
week_end = dt.last_day("2026-03-18", "week")
```

### `dt.add_days(dt_value, n, output_string=True)`

Adds or subtracts days.

Inputs:

- `dt_value`: input date
- `n`: number of days, can be negative
- `output_string`: output format flag

Returns:

- Shifted date

Example:

```python
prev_day = dt.add_days("2026-03-18", -1)
```

### `dt.add_weeks(dt_value, n, output_string=True)`

Adds or subtracts weeks.

Inputs:

- `dt_value`: input date
- `n`: number of weeks
- `output_string`: output format flag

Returns:

- Shifted date

Notes:

- The function aligns the input to the start of the week first

Example:

```python
next_week = dt.add_weeks("2026-03-18", 1)
```

### `dt.add_months(dt_value, n, output_string=True)`

Adds or subtracts months.

Inputs:

- `dt_value`: input date
- `n`: number of months
- `output_string`: output format flag

Returns:

- Shifted date

Notes:

- The function aligns the input to the first day of the month first

Example:

```python
next_month = dt.add_months("2026-03-18", 1)
```

### `dt.get_today(output_string=True)`

Returns today's date.

Inputs:

- `output_string`: output format flag

Returns:

- Today's date

Example:

```python
today = dt.get_today()
```

### `dt.get_random_day(start_dt, end_dt, output_string=True)`

Returns a random day inside a date range.

Inputs:

- `start_dt`: range start
- `end_dt`: range end
- `output_string`: output format flag

Returns:

- Random date from the inclusive range

Example:

```python
random_day = dt.get_random_day("2026-03-01", "2026-03-20")
```

## Module: `general`

Typical import:

```python
from analytics_toolkit.general import here, read_file, time_print
```

### `here(filename)`

Builds a path relative to the current script location.

Inputs:

- `filename`: file name or relative path, for example `"query.sql"` or `"sql/report.sql"`

Returns:

- String path to the file

When to use:

- When your script and SQL files live in the same working folder
- When you want `read_file(here(...))` instead of hardcoding absolute paths

Important:

- First call `os.chdir(...)` to switch into the ticket folder, then use `here(...)`

Example:

```python
import os

os.chdir("/path/to/dir")

# reads file /path/to/dir/get_push_data.sql
query = read_file(here("get_push_data.sql"))
df = sql.read('gp', query)

# saves df to /path/to/dir/output.xlsx
df.to_excel(here("output.xlsx"))
```

### `read_file(file_path, params_dict=None)`

Reads a text file as UTF-8. If `params_dict` is passed, placeholders are filled with `str.format(...)`.

Inputs:

- `file_path`: path to the file
- `params_dict`: optional dictionary for template substitution

Returns:

- File content as a string

Notes:

- Raises an error if the file does not exist
- Useful for SQL templates with `{start_dt}`-style placeholders

Example:

```python
query = read_file(
    here("get_push_data.sql"),
    params_dict={"start_dt": "2026-03-15", "end_dt": "2026-03-21"},
)
```

### `time_print(message)`

Prints a message with a timestamp.

Inputs:

- `message`: text to print

Returns:

- Nothing

Example:

```python
time_print("starting load")
```

## Module: `excel`

Typical import:

```python
from analytics_toolkit import excel
```

### `excel.pivot_and_break_table(df, rows, output, value=None, columns=None, break_by=None, sheet_by=None, append=False, enforce_same_row_order=False)`

Writes one or more pivot-style tables to Excel.

Inputs:

- `df`: a dataframe or a list of dataframes
- `rows`: column that becomes table rows
- `output`: output Excel file path
- `value`: one value column or a list of value columns; if omitted, remaining columns are used
- `columns`: optional column used for pivoted columns
- `break_by`: optional column that splits data into separate tables
- `sheet_by`: optional column that splits data into separate sheets
- `append`: if `True`, appends to an existing workbook
- `enforce_same_row_order`: if `True`, aligns row order across multiple input dataframes

Returns:

- Dictionary with written tables

When to use:

- When you want report-like Excel output from long-format data

Real usage pattern from tickets:

```python
excel.pivot_and_break_table(
    [res_df, stats_uplifts_df, uplifts_df],
    rows="metric",
    columns="ab_group",
    break_by="qr_group",
    sheet_by="start_dt",
    enforce_same_row_order=True,
    output=here("prepared_results.xlsx"),
    append=True,
)
```

### `excel.break_table(df, output, break_by=None, sheet_by=None, append=False)`

Writes dataframe slices to Excel without pivoting.

Inputs:

- `df`: a dataframe or a list of dataframes
- `output`: output Excel file path
- `break_by`: optional column that splits data into separate tables
- `sheet_by`: optional column that splits data into separate sheets
- `append`: if `True`, appends to an existing workbook

Returns:

- Dictionary with written tables

When to use:

- When the dataframe is already in final shape and only needs to be split into sheets or blocks

Real usage pattern from tickets:

```python
excel.break_table(
    metrics_df.assign(start_dt=lambda x: x["start_dt"].apply(lambda v: f"{v}_metrics")),
    sheet_by="start_dt",
    output=here("prepared_results.xlsx"),
    append=True,
)
```

## Module: `ab_utils`

Typical import:

```python
from analytics_toolkit import ab_utils as ab
```

### `ab.compute_test_metrics(df, group="group_name", control="control", user_id="user_id", mde_alpha=0.05, mde_power=0.80, ratio_metrics=None, test_vs_test=True, multiple_comparisons_adjustment=False, multiple_comparisons_adjustment_resamples=2000, bootstrap_random_state=0, bootstrap_n_jobs=1, bootstrap_progress=True, pre_exp_metrics_df=None)`

Computes A/B test metrics and statistical comparisons.

Inputs:

- `df`: main dataframe with one row per user
- `group`: column with test/control labels
- `control`: control group label
- `user_id`: unique user id column
- `mde_alpha`: significance level for MDE calculation
- `mde_power`: power for MDE calculation
- `ratio_metrics`: optional list of ratio metric definitions
- `test_vs_test`: if `True`, also compares test groups with each other
- `multiple_comparisons_adjustment`: enables bootstrap-adjusted p-values
- `multiple_comparisons_adjustment_resamples`: bootstrap iterations
- `bootstrap_random_state`: random seed or `None`
- `bootstrap_n_jobs`: parallel workers for bootstrap
- `bootstrap_progress`: show bootstrap progress bar
- `pre_exp_metrics_df`: optional pre-experiment dataframe for CUPED p-values

Returns:

- Dataframe with metric results

Expected input structure:

- `df` must contain one row per user
- All columns except `group` and `user_id` are treated as metrics
- `ratio_metrics` entries should look like this:

```python
{
    "name": "ARPPU",
    "numerator": "revenue",
    "denominator": "trn_flg"
}
```

Important output columns:

- `metric_name`
- `group_1`
- `group_2`
- `metric_control`
- `metric_test`
- `delta_abs`
- `delta_relative`
- `mde_abs`
- `mde_relative`
- `p-value`
- `p-value CUPED` if `pre_exp_metrics_df` is provided

Real usage pattern from tickets:

```python
ratio_metrics = [
    {"name": "ARPPU", "numerator": "revenue", "denominator": "trn_flg"},
    {"name": "AOV", "numerator": "revenue", "denominator": "trn"},
]

metrics_df = ab.compute_test_metrics(
    user_df,
    pre_exp_metrics_df=pre_exp_df,
    ratio_metrics=ratio_metrics,
    test_vs_test=False,
)
```
