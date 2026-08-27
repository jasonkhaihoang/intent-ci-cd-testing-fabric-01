-- Model: stg_fake_sale
-- Grain: one row per sale order.
-- Source: {{ ref('fake_sale') }} seed

SELECT
    order_id,
    customer_name,
    product_category,
    product_name,
    CAST(order_date AS DATE) AS order_date,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    region,
    sales_channel
FROM {{ ref('fake_sale') }}
