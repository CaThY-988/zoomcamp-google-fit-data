from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="google_fit_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["zoomcamp", "google-fit"],
) as dag:

    ingest_to_s3 = BashOperator(
        task_id="ingest_to_s3",
        bash_command="cd /workspace && python ingest.py",
    )

    load_raw_google_fit = BashOperator(
        task_id="load_raw_google_fit",
        bash_command="cd /workspace && python load_to_databricks.py",
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="""
        set -a
        source /workspace/.env
        set +a
        cd /workspace/dbt_googlefit
        uv run dbt run --profiles-dir /workspace/dbt_googlefit
        """,
    )

    ingest_to_s3 >> load_raw_google_fit >> run_dbt