CREATE OR REPLACE TABLE mart_sol.mart_product_solution AS
SELECT
    p.category,
    p.sub_category,
    p.product_name,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    SUM(f.quantity) AS total_quantity,
    AVG(f.sales) AS avg_sales_per_order
FROM core_sol.fact_sales_solution f
JOIN core_sol.dim_product_solution p
    ON f.product_name = p.product_name
GROUP BY p.category, p.sub_category, p.product_name
ORDER BY total_sales DESC;
