-- Model: int_sales_enriched
-- Grain: one row per sale transaction.
-- Adds the computed line amount next to the recorded total.
-- Materialized as a view on purpose: the intermediate layer defaults to ephemeral, and an ephemeral model is
-- inlined into its consumers, so a change to it could never leave a stale relation behind. VD-6391's model
-- chain needs real relations at each step.
{{ config(materialized='view') }}

SELECT
    sale_id,
    customer_id,
    product_id,
    sale_date,
    quantity,
    unit_price,
    total_amount,
    quantity * unit_price AS line_amount,
    region,
    sales_rep
FROM {{ ref('stg_sales_data') }}
