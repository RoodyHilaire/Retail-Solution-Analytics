CREATE OR REPLACE TABLE core_sol.fact_sales_solution AS
SELECT
    order_id,
    order_date,
    customer_id,
    customer_name,
    product_name,
    sales,
    region,
    profit,
    quantity,
    discount
FROM raw_sol.sales_raw_solution_min_check;





