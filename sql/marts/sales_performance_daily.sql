CREATE OR REPLACE TABLE mart_sol.mart_sales_solution AS
SELECT
    f.order_date,
    f.region,
    c.segment,
    p.category,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    SUM(f.quantity) AS total_quantity,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM core_sol.fact_sales_solution f
LEFT JOIN core_sol.dim_customer_solution c
    ON f.customer_id = c.customer_id
LEFT JOIN core_sol.dim_product_solution p
    ON f.product_name = p.product_name
GROUP BY
    f.order_date,
    f.region,
    c.segment,
    p.category;