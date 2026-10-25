# Design: VD-5657 defer-workspace-resolution fixture

## Model Inventory

| Model | Layer | Grain | Change |
| --- | --- | --- | --- |
| `mart_sales_summary` | marts | one row per region | New table-materialized model, `SELECT ... FROM {{ ref('stg_sales_data') }} GROUP BY region`. Added to exercise `--defer` against the unmodified, unselected `stg_sales_data` upstream — the scenario VD-5657's fix targets.
