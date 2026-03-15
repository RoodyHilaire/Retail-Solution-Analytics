CREATE OR REPLACE TABLE core_sol.dim_product_solution AS
SELECT
    DISTINCT
    product_name,
    category,
    sub_category
FROM raw_sol.sales_raw_solution_min_check;
