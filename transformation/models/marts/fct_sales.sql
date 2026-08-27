-- Model: fct_sales
-- Grain: one row per sale transaction with enriched dimensions
-- Source: stg_sales_data

WITH sales_base AS (
    SELECT
        sale_id,
        customer_id,
        product_id,
        sale_date,
        quantity,
        unit_price,
        total_amount,
        region,
        sales_rep,
        payment_method,
        CAST(sale_date AS DATE) AS sale_date_key,
        EXTRACT(YEAR FROM sale_date) AS sale_year,
        EXTRACT(MONTH FROM sale_date) AS sale_month,
        EXTRACT(QUARTER FROM sale_date) AS sale_quarter
    FROM {{ ref('stg_sales_data') }}
),

sales_enriched AS (
    SELECT
        *,
        SUM(total_amount) OVER (
            PARTITION BY customer_id
            ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS customer_cumulative_revenue,
        COUNT(*) OVER (
            PARTITION BY customer_id
            ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS customer_order_sequence,
        AVG(total_amount) OVER (
            PARTITION BY region
            ORDER BY sale_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS region_3day_avg_revenue
    FROM sales_base
)

SELECT
    sale_id,
    customer_id,
    product_id,
    sale_date_key,
    sale_year,
    sale_month,
    sale_quarter,
    quantity,
    unit_price,
    total_amount,
    region,
    sales_rep,
    payment_method,
    customer_cumulative_revenue,
    customer_order_sequence,
    region_3day_avg_revenue,
    CASE
        WHEN total_amount >= 500 THEN 'High Value'
        WHEN total_amount >= 200 THEN 'Medium Value'
        ELSE 'Standard'
    END AS sale_tier
FROM sales_enriched
