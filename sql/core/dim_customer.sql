CREATE OR REPLACE TABLE core_sol.dim_customer_solution AS
SELECT
    DISTINCT
    customer_id,
    customer_name,
    segment,
FROM raw_sol.sales_raw_solution_min_check;


