CREATE OR REPLACE TABLE mart_sol.mart_customer_solution AS
WITH rfm AS (
    SELECT
        customer_id,
        MAX(order_date) AS last_purchase,
        COUNT(order_id) AS frequency,
        SUM(sales) AS monetary
    FROM core_sol.fact_sales_solution
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT
        customer_id,
        NTILE(5) OVER (ORDER BY last_purchase DESC) AS recency_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS monetary_score
    FROM rfm
)
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    rfm.last_purchase,
    rfm.frequency,
    rfm.monetary,
    rfm_scores.recency_score,
    rfm_scores.frequency_score,
    rfm_scores.monetary_score
FROM rfm
JOIN rfm_scores USING(customer_id)
JOIN core_sol.dim_customer_solution c USING(customer_id);
