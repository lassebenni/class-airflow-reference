{% docs trip_grain %}

One row per completed taxi trip. "Completed" means the TLC submitted the
trip record to the public dataset; cancellations and trips in progress
are not included. Duplicates exist in the source data (roughly 4 rows in
January 2024 where every column is identical) and are kept as-is; see
the `dbt_utils.unique_combination_of_columns` test results for the
current count.

{% enddocs %}
