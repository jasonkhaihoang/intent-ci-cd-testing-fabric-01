---
artifacts:
  - stg_sales_data
---

# stg_sales_data

Staging view over the `sales_data` seed.

- Materialization: view.
- Grain: one row per sale transaction (`sale_id`).
- Columns: `sale_id`, `customer_id`, `product_id`, `sale_date`, `quantity`, `unit_price`, `total_amount`, `region`, `sales_rep`.
- Tests: `sale_id` is not null and unique.

## Change in this intent

A comment-only edit to the model file. No change to grain, materialization, columns, tests or upstream references.
