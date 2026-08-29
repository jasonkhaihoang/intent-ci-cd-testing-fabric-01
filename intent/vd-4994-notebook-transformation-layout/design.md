# Design: VD-4994 validation bump

## Model Inventory

| Model | Layer | Grain | Change |
| --- | --- | --- | --- |
| `stg_sales_data` | staging | one row per sale transaction | Comment-only bump (`-- VD-4994 validation bump`) to mark it `state:modified+` and exercise the gate ladder end-to-end. No column, grain, or materialization change.

