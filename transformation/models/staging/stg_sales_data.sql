-- Model: stg_sales_data
-- Grain: one row per sale transaction.
-- Source: {{ ref('sales_data') }} seed

SELECT
    sale_id,
    customer_id,
    product_id,
    sale_date,
    quantity,
    unit_price,
    total_amount,
    region,
    sales_rep
FROM {{ ref('sales_data') }}
-- VD-4994 validation bump
