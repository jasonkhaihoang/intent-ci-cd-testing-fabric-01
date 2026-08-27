-- Model: stg_sales_data
-- Grain: one row per sale transaction.
-- Source: {{ ref('sales_data') }} seed

SELECT
    sale_id,
    customer_id,
    product_id,
    CAST(sale_date AS DATE) AS sale_date,
    quantity,
    unit_price,
    total_amount,
    region,
    sales_rep,
    payment_method
FROM {{ ref('sales_data') }}
