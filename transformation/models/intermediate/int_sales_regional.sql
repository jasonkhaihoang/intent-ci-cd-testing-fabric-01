-- Model: int_sales_regional
-- Grain: one row per sale transaction, with its sales region.
{{ config(materialized='view') }}

SELECT
    sale_id,
    sale_date,
    region,
    total_amount,
    line_amount
FROM {{ ref('int_sales_valid') }}
