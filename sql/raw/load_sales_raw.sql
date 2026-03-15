CREATE OR REPLACE TABLE raw_sol.sales_raw_solution_min_check AS
SELECT
    -- Order info
    SAFE_CAST(order_id AS STRING) AS order_id,
    SAFE_CAST(order_date AS DATE) AS order_date,

    -- Customer info
    SAFE_CAST(customer_id AS STRING) AS customer_id,
    SAFE_CAST(customer_name AS STRING) AS customer_name,
    SAFE_CAST(segment AS STRING) AS segment,
    SAFE_CAST(region AS STRING) AS region,

    -- Product info
    SAFE_CAST(category AS STRING) AS category,
    SAFE_CAST(sub_category AS STRING) AS sub_category,
    SAFE_CAST(product_name AS STRING) AS product_name,

    -- Metrics
    SAFE_CAST(sales AS FLOAT64) AS sales,
    SAFE_CAST(profit AS FLOAT64) AS profit,
    SAFE_CAST(quantity AS INT64) AS quantity,
    SAFE_CAST(discount AS FLOAT64) AS discount,

    -- Metadata (very important in real companies)
    CURRENT_TIMESTAMP() AS ingestion_timestamp,
    DATE(CURRENT_TIMESTAMP()) AS ingestion_date

FROM raw_sol.sales_raw_solution
WHERE order_id IS NOT NULL;
