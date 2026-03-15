from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    dag_id="sales_pipeline_raw",
    start_date=datetime(2014, 1, 1),
    schedule="@daily",
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
    tags=["raw"]
) as dag:

    load_sales_raw = BigQueryInsertJobOperator(
        task_id="load_sales_raw",
        configuration={
            "query": {
                "query": "{% include 'raw/load_sales_raw.sql' %}",
                "useLegacySql": False,
            }
        },
    )
