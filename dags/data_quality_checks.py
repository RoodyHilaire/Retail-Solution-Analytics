from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, timezone


def run_check(sql_file: str, check_type: str):
    """
    Execute a data quality check in BigQuery.

    Parameters:
    - sql_file: Path to SQL file containing the check logic
    - check_type: Type of check (not_null, unique, freshness)
    """
    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
    )

    with open(sql_file) as f:
        sql = f.read()

    result = hook.get_pandas_df(sql)

    if check_type == "not_null":
        if result.iloc[0]["invalid_rows"] > 0:
            raise AirflowFailException("❌ NOT NULL check failed")

    elif check_type == "unique":
        if result.iloc[0]["duplicate_rows"] > 0:
            raise AirflowFailException("❌ Uniqueness check failed")

    elif check_type == "freshness":
        max_date = result.iloc[0]["max_date"]
        # Convert safely
        if hasattr(max_date, "date"):
            max_date = max_date.date()

        #today = datetime.now(timezone.utc).date()
        today = datetime.now(timezone.utc).date()
        if max_date < (today - timedelta(days= 425)):
            raise AirflowFailException("❌ Freshness check failed")


with DAG(
    dag_id="data_quality_checks",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["quality", "enterprise"],
    description="Enterprise data quality checks for sales fact table",
) as dag:

    check_not_null = PythonOperator(
        task_id="fact_sales_not_null_transaction_id",
        python_callable=run_check,
        op_kwargs={
            "sql_file": "/opt/airflow/sql/quality/fact_sales/not_null_transaction_id.sql",
            "check_type": "not_null",
        },
    )
    '''
    check_unique = PythonOperator(
        task_id="fact_sales_unique_transaction_id",
        python_callable=run_check,
        op_kwargs={
            "sql_file": "/opt/airflow/sql/quality/fact_sales/unique_transaction_id.sql",
            "check_type": "unique",
        },
    )

    check_freshness = PythonOperator(
        task_id="fact_sales_freshness_transaction_date",
        python_callable=run_check,
        op_kwargs={
            "sql_file": "/opt/airflow/sql/quality/fact_sales/freshness_transaction_date.sql",
            "check_type": "freshness",
        },
    )'''

    check_not_null 
    #>> check_unique >> check_freshness
