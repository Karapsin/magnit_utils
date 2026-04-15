# analytics_toolkit.dates

Date helpers for common reporting workflows.

## Available Helpers

- `gen_dates_list`: build a daily, weekly, or monthly sequence
- `first_day`: first day of a week or month
- `last_day`: last day of a week or month
- `add_days`
- `add_weeks`
- `add_months`

## Example

```python
from analytics_toolkit.dates.dates import first_day, gen_dates_list

first_day("2026-04-10")
gen_dates_list("2026-04-01", "2026-04-10")
```

Inputs accept ISO date strings, `date`, or `datetime` values.
