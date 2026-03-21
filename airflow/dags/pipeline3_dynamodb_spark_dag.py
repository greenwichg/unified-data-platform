"""
Airflow DAG: Pipeline 3 - DynamoDB Streams → Spark (EMR) → ORC → S3

Runs hourly to convert DynamoDB stream JSON data to optimized ORC format.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@zomato.com"],
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

EMR_CLUSTER_ID = "{{ var.value.emr_cluster_id }}"
RAW_BUCKET = "{{ var.value.s3_raw_bucket }}"
PROCESSED_BUCKET = "{{ var.value.s3_processed_bucket }}"

with DAG(
    dag_id="pipeline3_dynamodb_spark",
    default_args=default_args,
    description="DynamoDB Streams → Spark → S3 (ORC)",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=["pipeline3", "dynamodb", "spark", "emr"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting Pipeline 3 DynamoDB-Spark ETL - {{ ts }}'",
    )

    spark_step = {
        "Name": "dynamodb-stream-to-orc",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.sql.orc.enabled=true",
                "--conf", "spark.sql.adaptive.enabled=true",
                "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                "--py-files", f"s3://{RAW_BUCKET}/scripts/pipeline3/spark_orc_converter.py",
                f"s3://{RAW_BUCKET}/scripts/pipeline3/spark_orc_converter.py",
                "--raw-bucket", RAW_BUCKET,
                "--output-bucket", PROCESSED_BUCKET,
                "--date", "{{ execution_date.strftime('%Y/%m/%d/%H') }}",
            ],
        },
    }

    submit_spark = EmrAddStepsOperator(
        task_id="submit_spark_job",
        job_flow_id=EMR_CLUSTER_ID,
        steps=[spark_step],
    )

    wait_spark = EmrStepSensor(
        task_id="wait_spark_job",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_job')[0] }}",
        poke_interval=30,
        timeout=3600,
    )

    # Trigger Trino table refresh after new ORC data lands
    refresh_trino = BashOperator(
        task_id="refresh_trino_tables",
        bash_command=(
            "trino --server trino-etl:8080 --catalog iceberg --schema zomato "
            "--execute \"CALL system.sync_partition_metadata('zomato', 'orders', 'FULL')\""
        ),
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline 3 DynamoDB-Spark ETL completed - {{ ts }}'",
    )

    start >> submit_spark >> wait_spark >> refresh_trino >> end
