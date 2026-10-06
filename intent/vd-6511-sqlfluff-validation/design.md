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

### int_sales_enriched

**Kind:** model  
**Materialization:** view (overrides the intermediate layer's ephemeral default, so the chain has real relations)  
**Source:** `ref('stg_sales_data')`

**Grain:** One row per sale transaction.

**Columns:** `sale_id`, `customer_id`, `product_id`, `sale_date`, `quantity`, `unit_price`, `total_amount`, `line_amount` (quantity × unit_price), `region`, `sales_rep`

**Tests:**
- `sale_id`: not_null, unique

### int_sales_valid

**Kind:** model  
**Materialization:** view  
**Source:** `ref('int_sales_enriched')`

**Grain:** One row per sale transaction — the sales downstream models may rely on.

**Columns:** `sale_id`, `customer_id`, `product_id`, `sale_date`, `quantity`, `unit_price`, `total_amount`, `line_amount`, `region`, `sales_rep`

**Tests:**
- `sale_id`: not_null, unique

### int_sales_regional

**Kind:** model  
**Materialization:** view  
**Source:** `ref('int_sales_valid')`

**Grain:** One row per sale transaction, with its sales region.

**Columns:** `sale_id`, `sale_date`, `region`, `total_amount`, `line_amount`

**Tests:**
- `sale_id`: not_null, unique

### fct_sales_summary

**Kind:** model  
**Materialization:** table (marts layer default)  
**Source:** `ref('int_sales_regional')`

**Grain:** One row per sales region.

**Columns:**
- `region` — Sales region (North, South, East, West)
- `sale_count` — Number of sales in the region
- `revenue` — Sum of the sales' `total_amount`

**Tests:**
- `region`: not_null, unique

## Change Impact

`stg_sales_data` feeds a linear chain: `stg_sales_data` → `int_sales_enriched` → `int_sales_valid` → `int_sales_regional` → `fct_sales_summary`. A change to `stg_sales_data` therefore puts all five models in the `state:modified+` closure.
