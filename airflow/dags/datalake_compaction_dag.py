"""
Airflow DAG: Data Lake Compaction (Iceberg Maintenance)

Runs daily Iceberg table maintenance via Trino SQL:
  - rewrite_data_files: compact small files into optimally-sized ORC files
  - expire_snapshots: remove snapshots older than retention period
  - remove_orphan_files: clean up unreferenced data files in S3

Targets all core Iceberg tables in the zomato schema.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from common.trino_operator import TrinoOperator
from common.slack_alerts import on_failure_callback

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@zomato.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=4),
    "on_failure_callback": on_failure_callback,
}

TRINO_ETL_HOST = "{{ var.value.trino_etl_host }}"
TRINO_ETL_PORT = 8080

# Iceberg tables to maintain, with per-table compaction thresholds
ICEBERG_TABLES = {
    "orders": {
        "target_file_size_mb": 512,
        "snapshot_retention_days": 7,
        "orphan_retention_days": 3,
    },
    "users": {
        "target_file_size_mb": 256,
        "snapshot_retention_days": 7,
        "orphan_retention_days": 3,
    },
    "menu_items": {
        "target_file_size_mb": 128,
        "snapshot_retention_days": 7,
        "orphan_retention_days": 3,
    },
    "payments": {
        "target_file_size_mb": 512,
        "snapshot_retention_days": 5,
        "orphan_retention_days": 2,
    },
    "promotions": {
        "target_file_size_mb": 128,
        "snapshot_retention_days": 14,
        "orphan_retention_days": 5,
    },
}

REWRITE_DATA_FILES_SQL = """
ALTER TABLE iceberg.zomato.{table}
EXECUTE optimize(file_size_threshold => '{target_file_size_mb}MB')
"""

EXPIRE_SNAPSHOTS_SQL = """
ALTER TABLE iceberg.zomato.{table}
EXECUTE expire_snapshots(retention_threshold => '{retention_days}d')
"""

REMOVE_ORPHAN_FILES_SQL = """
ALTER TABLE iceberg.zomato.{table}
EXECUTE remove_orphan_files(retention_threshold => '{retention_days}d')
"""


def log_compaction_metrics(**context):
    """Log compaction run summary for monitoring and alerting."""
    dag_run = context["dag_run"]
    ti = context["ti"]
    tables_processed = list(ICEBERG_TABLES.keys())
    print(f"Compaction completed for {len(tables_processed)} tables: {tables_processed}")
    print(f"DAG run: {dag_run.run_id}, execution_date: {context['ds']}")


with DAG(
    dag_id="datalake_compaction",
    default_args=default_args,
    description="Daily Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "maintenance", "compaction", "datalake"],
) as dag:

    for table_name, config in ICEBERG_TABLES.items():
        with TaskGroup(f"maintain_{table_name}") as table_group:
            rewrite = TrinoOperator(
                task_id=f"rewrite_data_files_{table_name}",
                trino_host=TRINO_ETL_HOST,
                trino_port=TRINO_ETL_PORT,
                sql=REWRITE_DATA_FILES_SQL.format(
                    table=table_name,
                    target_file_size_mb=config["target_file_size_mb"],
                ),
                catalog="iceberg",
                schema="zomato",
            )

            expire = TrinoOperator(
                task_id=f"expire_snapshots_{table_name}",
                trino_host=TRINO_ETL_HOST,
                trino_port=TRINO_ETL_PORT,
                sql=EXPIRE_SNAPSHOTS_SQL.format(
                    table=table_name,
                    retention_days=config["snapshot_retention_days"],
                ),
                catalog="iceberg",
                schema="zomato",
            )

            orphan = TrinoOperator(
                task_id=f"remove_orphan_files_{table_name}",
                trino_host=TRINO_ETL_HOST,
                trino_port=TRINO_ETL_PORT,
                sql=REMOVE_ORPHAN_FILES_SQL.format(
                    table=table_name,
                    retention_days=config["orphan_retention_days"],
                ),
                catalog="iceberg",
                schema="zomato",
            )

            rewrite >> expire >> orphan

    log_metrics = PythonOperator(
        task_id="log_compaction_metrics",
        python_callable=log_compaction_metrics,
    )

    # All table maintenance groups run in parallel, then log metrics
    for table_name in ICEBERG_TABLES:
        dag.get_task(f"maintain_{table_name}.remove_orphan_files_{table_name}") >> log_metrics
