# Design

## Transformation

### stg_sales_data

**Kind:** model  
**Materialization:** view (staging layer)  
**Source:** `sales_data` seed (dbt seed)

**Grain:** One row per sale transaction.

**Columns:**
- `sale_id` — Unique sale transaction identifier (primary key)
- `customer_id` — Customer identifier
- `product_id` — Product identifier
- `sale_date` — Date of the sale transaction
- `quantity` — Number of units sold
- `unit_price` — Price per unit
- `total_amount` — Total sale amount (quantity × unit_price)
- `region` — Sales region (North, South, East, West)
- `sales_rep` — Name of the sales representative

**Tests:**
- `sale_id`: not_null, unique

**Purpose:** Staging layer that loads the fake sales seed data with consistent column naming and applies basic data quality tests.

## Change Impact

`stg_sales_data` has no downstream consumers — the repo has no intermediate or marts models — so the `state:modified+` closure for a change to it is the single node `stg_sales_data`.
