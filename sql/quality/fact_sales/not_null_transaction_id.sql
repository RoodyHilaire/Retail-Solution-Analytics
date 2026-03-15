SELECT COUNT(*) AS invalid_rows
FROM core_sol.fact_sales_solution
WHERE order_id IS NULL;
