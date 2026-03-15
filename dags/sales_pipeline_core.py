from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    dag_id="sales_pipeline_core",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
    tags=["core", "datamart", "sales"],
) as dag:

    # Build date dimension
    build_dim_date = BigQueryInsertJobOperator(
        task_id="build_dim_date",
        configuration={
            "query": {
                "query": "{% include 'core/dim_date.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # Build customer dimension
    build_dim_customer = BigQueryInsertJobOperator(
        task_id="build_dim_customer",
        configuration={
            "query": {
                "query": "{% include 'core/dim_customer.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # Build product dimension
    build_dim_product = BigQueryInsertJobOperator(
        task_id="build_dim_product",
        configuration={
            "query": {
                "query": "{% include 'core/dim_product.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # Load fact_sales incremental
    load_fact_sales = BigQueryInsertJobOperator(
        task_id="load_fact_sales_incremental",
        configuration={
            "query": {
                "query": "{% include 'core/fact_sales_incremental.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # Dependencies
    [build_dim_date, build_dim_customer, build_dim_product] >> load_fact_sales
