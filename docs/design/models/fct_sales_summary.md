---
artifacts:
  - fct_sales_summary
---

# fct_sales_summary

Sales rollup by region.

- Materialization: table.
- Grain: one row per sales region.
- Columns: `region`, `sale_count` (count of sales), `revenue` (sum of `total_amount`).
- Source: `int_sales_regional`.

## Change in this intent

A comment-only edit to the model file. No change to grain, materialization, columns, tests or upstream references.
