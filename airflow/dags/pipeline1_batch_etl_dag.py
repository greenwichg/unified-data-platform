"""
Airflow DAG: Pipeline 1 - Batch ETL
Aurora MySQL → Sqoop → S3 (ORC)

Runs every 6 hours to import data from Aurora MySQL into the S3 data lake.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@zomato.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

TABLES = ["orders", "users", "restaurants", "menu_items", "payments", "promotions"]
EMR_CLUSTER_ID = "{{ var.value.emr_cluster_id }}"
S3_BUCKET = "{{ var.value.s3_raw_bucket }}"
AURORA_JDBC = "{{ var.value.aurora_jdbc_url }}"

with DAG(
    dag_id="pipeline1_batch_etl",
    default_args=default_args,
    description="Batch ETL: Aurora MySQL → Sqoop → S3 (ORC)",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline1", "batch", "sqoop", "etl"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting Pipeline 1 Batch ETL - {{ ds }}'",
    )

    with TaskGroup("sqoop_imports") as sqoop_imports:
        for table in TABLES:
            sqoop_step = {
                "Name": f"sqoop-import-{table}",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "sqoop", "import",
                        "--connect", AURORA_JDBC,
                        "--table", table,
                        "--target-dir", f"s3://{S3_BUCKET}/pipeline1-batch-etl/sqoop-output/{table}/{{{{ ds }}}}/",
                        "--as-orcfile",
                        "--compress",
                        "--compression-codec", "org.apache.hadoop.io.compress.SnappyCodec",
                        "--num-mappers", "8",
                        "--incremental", "lastmodified",
                        "--check-column", "updated_at",
                        "--last-value", "{{ prev_ds }}",
                    ],
                },
            }

            add_step = EmrAddStepsOperator(
                task_id=f"sqoop_import_{table}",
                job_flow_id=EMR_CLUSTER_ID,
                steps=[sqoop_step],
            )

            wait_step = EmrStepSensor(
                task_id=f"wait_sqoop_{table}",
                job_flow_id=EMR_CLUSTER_ID,
                step_id="{{ task_instance.xcom_pull(task_ids='sqoop_imports." + f"sqoop_import_{table}" + "')[0] }}",
                poke_interval=30,
                timeout=3600,
            )

            add_step >> wait_step

    with TaskGroup("orc_optimization") as orc_optimization:
        for table in TABLES:
            optimize_step = {
                "Name": f"optimize-orc-{table}",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "hive", "-e",
                        f"INSERT OVERWRITE TABLE zomato_processed.{table} "
                        f"PARTITION (dt='{{{{ ds }}}}') "
                        f"SELECT * FROM zomato_raw.{table} WHERE dt='{{{{ ds }}}}'",
                    ],
                },
            }

            EmrAddStepsOperator(
                task_id=f"optimize_orc_{table}",
                job_flow_id=EMR_CLUSTER_ID,
                steps=[optimize_step],
            )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline 1 Batch ETL completed - {{ ds }}'",
    )

    start >> sqoop_imports >> orc_optimization >> end
