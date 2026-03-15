from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    dag_id="sales_pipeline_marts",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    template_searchpath=["/opt/airflow/sql"],
    catchup=False,
    tags=["marts", "dashboard"]
) as dag:

    # Sales mart for overall performance
    build_sales_mart = BigQueryInsertJobOperator(
        task_id="build_sales_mart",
        configuration={
            "query": {
                "query": "{% include 'marts/sales_performance_daily.sql' %}",
                "useLegacySql": False
            }
        },
    )

    # Customer mart for RFM and churn
    build_customer_mart = BigQueryInsertJobOperator(
        task_id="build_customer_mart",
        configuration={
            "query": {
                "query": "{% include 'marts/customer_behavior.sql' %}",
                "useLegacySql": False
            }
        },
    )

    # Product mart for category/subcategory analysis
    build_product_mart = BigQueryInsertJobOperator(
        task_id="build_product_mart",
        configuration={
            "query": {
                "query": "{% include 'marts/product_performance.sql' %}",
                "useLegacySql": False
            }
        },
    )

    # Dependencies (they can run in parallel)
    [build_sales_mart, build_customer_mart, build_product_mart]
