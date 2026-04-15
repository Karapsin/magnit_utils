# analytics_toolkit.excel

Excel helpers for converting long-format data into one or more report tables.

## Available Helpers

- `pivot_and_break_table`: pivot a dataframe and write the resulting table set to Excel
- `break_table`: split a dataframe into sheets and stacked tables without pivoting

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
```

Both helpers return the written dataframes grouped by the original `sheet_by`
values, which makes them convenient for tests or for callers that need both the
Excel file and the transformed tables.
