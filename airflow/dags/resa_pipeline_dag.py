from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/retailnexus"

default_args = {
    "owner": "sanjana",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="resa_bronze_to_gold",
    description="ReSA pipeline: Bronze -> Silver -> Data Quality -> Gold",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["resa", "retailnexus"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"PYTHONPATH=processing/spark_jobs python "
            f"processing/spark_jobs/bronze_to_silver/sales_audit_transform.py"
        ),
    )

    validate_silver = BashOperator(
        task_id="validate_silver_gx",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"PYTHONPATH=processing/spark_jobs python "
            f"data_quality/validate_silver.py"
        ),
    )

    bronze_to_silver >> validate_silver
