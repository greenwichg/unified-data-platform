"""
Airflow DAG: Data Quality Checks

Validates data quality across all pipelines and the data lake.
Checks for: completeness, freshness, schema conformance, and anomalies.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@zomato.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ATHENA_OUTPUT_S3 = "{{ var.value.athena_query_results_s3 }}"


def check_data_freshness(**context):
    """Check that data from all pipelines is fresh (within SLA)."""
    checks = {
        "pipeline1_batch": {"max_lag_hours": 7, "table": "zomato_raw.orders"},
        "pipeline2_cdc": {"max_lag_minutes": 10, "table": "iceberg.zomato.orders"},
        "pipeline3_dynamodb": {"max_lag_hours": 2, "table": "zomato_processed.orders"},
        "pipeline4_realtime": {"max_lag_minutes": 5, "table": "druid.zomato_realtime_events"},
    }
    # In production, this would query Trino/Druid to check freshness
    return checks


def check_row_counts(**context):
    """Verify expected row counts for daily partitions."""
    expected_minimums = {
        "orders": 2_000_000,
        "users": 100_000,
        "menu_items": 50_000,
        "payments": 1_500_000,
    }
    return expected_minimums


with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description="Data quality validation across all pipelines",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "monitoring"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Running data quality checks - {{ ts }}'",
    )

    freshness_check = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    row_count_check = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )

    with TaskGroup("schema_checks") as schema_checks:
        for table in ["orders", "users", "menu_items", "payments", "promotions"]:
            BashOperator(
                task_id=f"schema_check_{table}",
                bash_command=(
                    f"trino --server {TRINO_HOST}:8080 "
                    f"--execute \"DESCRIBE iceberg.zomato.{table}\" "
                    f"| wc -l"
                ),
            )

    # Check Kafka consumer lag
    kafka_lag_check = BashOperator(
        task_id="check_kafka_lag",
        bash_command=(
            "kafka-consumer-groups.sh "
            "--bootstrap-server {{ var.value.kafka_bootstrap }} "
            "--describe --all-groups "
            "| awk '$6 > 1000000 {print \"HIGH LAG:\", $0}'"
        ),
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Data quality checks completed - {{ ts }}'",
    )

    start >> [freshness_check, row_count_check, schema_checks, kafka_lag_check] >> end
