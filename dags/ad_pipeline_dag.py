"""Daily ad campaign pipeline.

generate_data -> clean_events -> quality_check -> build_metrics

If quality_check fails, build_metrics does not run, so bad data never reaches
the reporting table.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.getenv("PIPELINE_DIR", "/opt/airflow/project")
AWS_MODE = os.getenv("PIPELINE_MODE", "local") == "aws"

default_args = {
    "owner": "vrezh",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ad_campaign_pipeline",
    description="Clean ad events and build daily campaign metrics (PySpark + Iceberg)",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spark", "iceberg", "ads"],
) as dag:

    # {{ ds }} is Airflow's run date (YYYY-MM-DD)
    upload_flag = "--upload" if AWS_MODE else ""

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"cd {PROJECT_DIR} && python -m data_generator.generate_data --date {{{{ ds }}}} {upload_flag}",
    )

    clean_events = BashOperator(
        task_id="clean_events",
        bash_command=f"cd {PROJECT_DIR} && python -m jobs.clean_events --date {{{{ ds }}}}",
    )

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command=f"cd {PROJECT_DIR} && python -m jobs.quality_check --date {{{{ ds }}}}",
    )

    build_metrics = BashOperator(
        task_id="build_metrics",
        bash_command=f"cd {PROJECT_DIR} && python -m jobs.build_metrics --date {{{{ ds }}}}",
    )

    generate_data >> clean_events >> quality_check >> build_metrics
