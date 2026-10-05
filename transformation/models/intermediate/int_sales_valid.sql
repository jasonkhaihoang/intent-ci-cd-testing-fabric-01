-- Model: int_sales_valid
-- Grain: one row per sale transaction.
-- The sales that downstream models may rely on.
{{ config(materialized='view') }}

SELECT
    sale_id,
    customer_id,
    product_id,
    sale_date,
    quantity,
    unit_price,
    total_amount,
    line_amount,
    region,
    sales_rep
FROM {{ ref('int_sales_enriched') }}
