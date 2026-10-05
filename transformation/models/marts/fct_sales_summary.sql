-- Model: fct_sales_summary
-- Grain: one row per sales region.
SELECT
    region,
    COUNT(*) AS sale_count,
    SUM(total_amount) AS revenue
FROM {{ ref('int_sales_regional') }}
GROUP BY region
