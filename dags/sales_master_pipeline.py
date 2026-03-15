from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import storage
from datetime import datetime
import pandas as pd
import numpy as np
import os
from google.oauth2 import service_account


# ------------------------------------------
# Python callable: extract from PostgreSQL
# ------------------------------------------
def extract_postgres_data(**kwargs):
    # Path inside the container
    file_path = "/opt/airflow/data/sales_data.csv"

    # Ensure the folder exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Connect to PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id="postgres_retail")

    query = """
    SELECT *
    FROM superstore_raw
    --WHERE order_date >= CURRENT_DATE - INTERVAL '1 day'
    """

    # Fetch data
    df = postgres_hook.get_pandas_df(query)

    # Save to CSV
    df.to_csv(file_path, index=False)
    print(f"Extracted PostgreSQL data to {file_path}")

    # Push file path to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='sales_file_path', value=file_path)

# ------------------------------------------
# Python callable: upload CSV to GCS
# ------------------------------------------
'''def upload_to_gcs(bucket_name, destination_blob, **kwargs):
    ti = kwargs['ti']
    source_file = ti.xcom_pull(task_ids='extract_postgres_data', key='sales_file_path')
    # Path to the JSON key inside the container
    key_path = "/opt/airflow/keys/airflow-sa.json"

    credentials = service_account.Credentials.from_service_account_file(key_path)

    #client = storage.Client(project=os.environ["GCP_PROJECT"])
    client = storage.Client(credentials=credentials, project=os.environ["GCP_PROJECT"])
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"Uploaded {source_file} to gs://{bucket_name}/{destination_blob}")'''

def upload_to_gcs(bucket_name, destination_blob, **kwargs):
    ti = kwargs['ti']
    source_file = ti.xcom_pull(task_ids='extract_postgres_data', key='sales_file_path')

    if not source_file or not os.path.exists(source_file):
        raise FileNotFoundError(f"CSV file not found at {source_file}")

    # Path to your GCP service account JSON key inside the container
    key_path = "/opt/airflow/keys/airflow-sa.json"
    if not os.path.exists(key_path):
        raise FileNotFoundError(f"GCP key file not found at {key_path}")

    # Authenticate with GCS
    credentials = service_account.Credentials.from_service_account_file(key_path)
    project_id = os.environ.get("GCP_PROJECT")
    if not project_id:
        raise EnvironmentError("Environment variable GCP_PROJECT is not set")

    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    # Upload file
    blob.upload_from_filename(source_file)
    print(f"Uploaded {source_file} to gs://{bucket_name}/{destination_blob}")

# ------------------------------------------
# DAG definition
# ------------------------------------------
with DAG(
    dag_id="sales_master_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["master"]
) as dag:

    # 1️⃣ Extract from postgre
    extract_task = PythonOperator(
        task_id="extract_postgres_data",
        python_callable=extract_postgres_data,
        provide_context=True
    )

    # 2️⃣ Upload CSV to GCS
    upload_task = PythonOperator(
        task_id="upload_sales_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": "my_sales_frand_solution",
            "destination_blob": "sales_data_solution/sales_data_solution.csv"
        },
        provide_context=True
    )

    # 3️⃣ Load CSV from GCS to BigQuery
    load_to_bq = GCSToBigQueryOperator(
    task_id="load_sales_to_bq",
    bucket="my_sales_frand_solution",
    source_objects=["sales_data_solution/sales_data_solution.csv"],
    destination_project_dataset_table="retail-airflow-project.raw_sol.sales_raw_solution",
    source_format="CSV",
    skip_leading_rows=1,
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_default"  # <--- important
    )

    # 4️⃣ Trigger existing DAGs in order
    trigger_raw = TriggerDagRunOperator(
        task_id="trigger_sales_pipeline_raw",
        trigger_dag_id="sales_pipeline_raw",
        wait_for_completion=True
    )

    trigger_core = TriggerDagRunOperator(
        task_id="trigger_sales_pipeline_core",
        trigger_dag_id="sales_pipeline_core",
        wait_for_completion=True
    )

    trigger_marts = TriggerDagRunOperator(
        task_id="trigger_sales_pipeline_marts",
        trigger_dag_id="sales_pipeline_marts",
        wait_for_completion=True
    )

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality_checks",
        trigger_dag_id="data_quality_checks",
        wait_for_completion=True
    )

    # ------------------------------------------
    # DAG dependencies
    # ------------------------------------------
    extract_task >> upload_task >> load_to_bq
    load_to_bq >> trigger_raw >> trigger_core >> trigger_marts >> trigger_quality
