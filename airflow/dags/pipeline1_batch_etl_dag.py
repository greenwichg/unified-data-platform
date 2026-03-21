"""
Airflow DAG: Pipeline 1 - Batch ETL
Aurora MySQL → Spark JDBC → Iceberg/ORC → S3

Runs every 6 hours to import data from Aurora MySQL into the S3 data lake
using Spark JDBC reads with direct Iceberg/ORC writes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
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

# Spark resource configuration
SPARK_CONF = {
    "driver_memory": "4g",
    "executor_memory": "8g",
    "executor_cores": "4",
    "num_executors": "8",
}

# S3 path to the batch_etl.py script (deployed via CI/CD)
ETL_SCRIPT_S3 = "{{ var.value.s3_scripts_bucket }}/pipeline1/batch_etl.py"
MYSQL_CONNECTOR_JAR = "/usr/share/java/mysql-connector-java.jar"
ICEBERG_JAR = "/usr/lib/iceberg/lib/iceberg-spark-runtime.jar"


def build_spark_step(table: str) -> dict:
    """Build an EMR Spark step for importing a single table."""
    return {
        "Name": f"spark-jdbc-import-{table}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master", "yarn",
                "--deploy-mode", "cluster",
                "--name", f"Pipeline1-{table}",
                "--driver-memory", SPARK_CONF["driver_memory"],
                "--executor-memory", SPARK_CONF["executor_memory"],
                "--executor-cores", SPARK_CONF["executor_cores"],
                "--num-executors", SPARK_CONF["num_executors"],
                "--jars", f"{MYSQL_CONNECTOR_JAR},{ICEBERG_JAR}",
                "--conf", "spark.sql.adaptive.enabled=true",
                "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
                "--conf", "spark.sql.adaptive.skewJoin.enabled=true",
                "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                "--conf", "spark.sql.orc.impl=native",
                "--conf", "spark.sql.orc.enableVectorizedReader=true",
                "--conf", "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
                "--conf", "spark.sql.catalog.iceberg.type=hive",
                "--conf", "spark.sql.catalog.iceberg.uri=thrift://hive-metastore.zomato-data.internal:9083",
                "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider",
                ETL_SCRIPT_S3,
                "--jdbc-url", AURORA_JDBC,
                "--s3-bucket", S3_BUCKET,
                "--table", table,
            ],
        },
    }


with DAG(
    dag_id="pipeline1_batch_etl",
    default_args=default_args,
    description="Batch ETL: Aurora MySQL → Spark JDBC → Iceberg/ORC → S3",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline1", "batch", "spark", "etl", "iceberg"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting Pipeline 1 Spark JDBC ETL - {{ ds }}'",
    )

    with TaskGroup("spark_jdbc_imports") as spark_imports:
        for table in TABLES:
            add_step = EmrAddStepsOperator(
                task_id=f"spark_import_{table}",
                job_flow_id=EMR_CLUSTER_ID,
                steps=[build_spark_step(table)],
            )

            wait_step = EmrStepSensor(
                task_id=f"wait_spark_{table}",
                job_flow_id=EMR_CLUSTER_ID,
                step_id=(
                    "{{ task_instance.xcom_pull("
                    f"task_ids='spark_jdbc_imports.spark_import_{table}'"
                    ")[0] }}"
                ),
                poke_interval=30,
                timeout=3600,
            )

            add_step >> wait_step

    data_quality = BashOperator(
        task_id="log_quality_metrics",
        bash_command=(
            "echo 'Data quality checks are inline in Spark jobs. "
            "Check CloudWatch for metrics - {{ ds }}'"
        ),
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline 1 Spark JDBC ETL completed - {{ ds }}'",
    )

    start >> spark_imports >> data_quality >> end
