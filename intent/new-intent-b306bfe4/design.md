# Design

## Models

### stg_fake_sale
- **Type**: staging model
- **Source**: `fake_sale` seed
- **Description**: One row per sale order from the fake_sale seed dataset
- **Key columns**: order_id, customer_name, product_category, product_name, order_date, quantity, unit_price, total_amount, payment_method, region, sales_channel

### stg_sales_data
- **Type**: staging model
- **Source**: `sales_data` seed
- **Description**: One row per sale transaction from the sales_data seed dataset
- **Key columns**: sale_id, customer_id, product_id, sale_date, quantity, unit_price, total_amount, region, sales_rep
