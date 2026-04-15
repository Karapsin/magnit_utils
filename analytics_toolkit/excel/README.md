# analytics_toolkit.excel

Excel helpers for converting long-format data into one or more report tables.

## Available Helpers

- `pivot_and_break_table`: pivot a dataframe and write the resulting table set to Excel
- `break_table`: split a dataframe into sheets and stacked tables without pivoting

Both helpers accept either a single dataframe or a list of dataframes.

## Example

```python
from analytics_toolkit.excel import break_table, pivot_and_break_table

pivoted_tables = pivot_and_break_table(
    df=dataframe,
    rows="metric",
    value="value",
    output="report.xlsx",
    columns="ab_group",
    break_by="qr_group",
    sheet_by="start_dt",
)

By default both helpers replace an existing `output` workbook. Pass
`append=True` to keep the existing file and add new sheets using the current
sheet-deduplication behavior.

When writing multiple dataframes with `pivot_and_break_table`, pass
`enforce_same_row_order=True` to align each later dataframe's pivoted row-label
order to the first dataframe. Missing row labels are written as blank rows; extra
row labels in later dataframes raise a `ValueError`.

wide_tables = pivot_and_break_table(
    df=wide_dataframe,
    rows="metric",
    value=["users", "arpu"],
    output="wide_report.xlsx",
    columns="ab_group",
    break_by="qr_group",
    sheet_by="start_dt",
)

auto_value_tables = pivot_and_break_table(
    df=wide_dataframe,
    rows="metric",
    output="auto_value_report.xlsx",
    columns="ab_group",
    break_by="qr_group",
    sheet_by="start_dt",
)

raw_tables = break_table(
    df=dataframe,
    output="raw_report.xlsx",
    break_by="qr_group",
    sheet_by="start_dt",
)

combined_tables = pivot_and_break_table(
    df=[dataframe_a, dataframe_b],
    rows="metric",
    value="value",
    output="combined_report.xlsx",
    columns="ab_group",
    break_by="qr_group",
    sheet_by="start_dt",
)
```

Both helpers return the written dataframes grouped by the original `sheet_by`
values, which makes them convenient for tests or for callers that need both the
Excel file and the transformed tables. When a list of dataframes is passed, each
sheet places the first dataframe's tables in the left block, the second
dataframe's tables in the next block to the right with one blank column between
blocks, and so on.
