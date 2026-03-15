CREATE OR REPLACE TABLE core_sol.dim_date_solution AS
SELECT
    DISTINCT
    order_date AS date,
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    EXTRACT(DAY FROM order_date) AS day,
    EXTRACT(DAYOFWEEK FROM order_date) AS day_of_week,
    FORMAT_DATE('%Y-%m', order_date) AS year_month,
    FORMAT_DATE('%Y-%U', order_date) AS year_week
FROM raw_sol.sales_raw_solution_min_check;
