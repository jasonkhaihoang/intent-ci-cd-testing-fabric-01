-- Model: mart_sales_summary
-- VD-5657 defer-workspace-resolution fixture: a real (table-materialized)
-- downstream model referencing stg_sales_data, so a later Intent that
-- modifies only this model exercises --defer against the unmodified,
-- unselected stg_sales_data upstream.

SELECT
    region,
    COUNT(*) AS n_sales,
    SUM(total_amount) AS total_amount
FROM {{ ref('stg_sales_data') }}
GROUP BY region
