# Design

## Models

### stg_sales_data
- **Type**: staging model
- **Source**: `sales_data` seed
- **Description**: One row per sale transaction. Staging layer that loads raw sales data with date casting and adds payment_method field.
- **Key columns**: sale_id, customer_id, product_id, sale_date, quantity, unit_price, total_amount, region, sales_rep, payment_method

### fct_sales
- **Type**: mart model
- **Source**: `stg_sales_data`
- **Description**: One row per sale transaction with enriched analytics dimensions. Adds time breakdowns (year, month, quarter), customer behavior metrics (cumulative revenue, order sequence), region analytics (3-day rolling average), and value tier classification.
- **Key columns**: sale_id, customer_id, product_id, sale_date_key, sale_year, sale_month, sale_quarter, quantity, unit_price, total_amount, region, sales_rep, payment_method, customer_cumulative_revenue, customer_order_sequence, region_3day_avg_revenue, sale_tier
