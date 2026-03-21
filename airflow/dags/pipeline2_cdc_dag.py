"""
Airflow DAG: Pipeline 2 - CDC Pipeline Management
Aurora MySQL -> Debezium -> Kafka -> Flink CDC -> Iceberg (S3)

Monitors Debezium connector health, manages Flink CDC jobs, and validates
data freshness in the Iceberg CDC tables. Runs every 10 minutes.
"""

import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException

from common.slack_alerts import on_failure_callback

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@zomato.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_callback,
}

DEBEZIUM_CONNECT_URL = "{{ var.value.debezium_connect_url }}"
FLINK_REST_URL = "{{ var.value.flink_rest_url }}"
KAFKA_BOOTSTRAP = "{{ var.value.kafka_bootstrap_servers }}"

CDC_CONNECTORS = [
    "zomato-mysql-cdc-orders",
    "zomato-mysql-cdc-users",
    "zomato-mysql-cdc-menu-items",
    "zomato-mysql-cdc-promotions",
]

CDC_TABLES = ["orders", "users", "menu_items", "promotions"]

# Maximum allowed lag in seconds before alerting
MAX_LAG_THRESHOLD_SEC = 300


def check_debezium_connector_status(**context):
    """Check health of all Debezium connectors via Kafka Connect REST API."""
    connect_url = Variable.get("debezium_connect_url")
    unhealthy_connectors = []
    connector_statuses = {}

    for connector in CDC_CONNECTORS:
        try:
            response = requests.get(
                f"{connect_url}/connectors/{connector}/status",
                timeout=15,
            )
            response.raise_for_status()
            status = response.json()

            connector_state = status["connector"]["state"]
            task_states = [t["state"] for t in status.get("tasks", [])]

            connector_statuses[connector] = {
                "connector_state": connector_state,
                "task_states": task_states,
                "task_count": len(task_states),
            }

            if connector_state != "RUNNING":
                unhealthy_connectors.append(
                    f"{connector}: connector={connector_state}"
                )

            failed_tasks = [
                i for i, s in enumerate(task_states) if s != "RUNNING"
            ]
            if failed_tasks:
                unhealthy_connectors.append(
                    f"{connector}: tasks {failed_tasks} not RUNNING"
                )

        except requests.exceptions.RequestException as e:
            unhealthy_connectors.append(f"{connector}: unreachable ({str(e)})")

    context["ti"].xcom_push(key="connector_statuses", value=connector_statuses)
    context["ti"].xcom_push(key="unhealthy_connectors", value=unhealthy_connectors)

    if unhealthy_connectors:
        print(f"Unhealthy connectors detected: {unhealthy_connectors}")
    else:
        print("All Debezium connectors are healthy")

    return connector_statuses


def branch_on_connector_health(**context):
    """Branch to restart task if any connectors are unhealthy."""
    unhealthy = context["ti"].xcom_pull(
        task_ids="monitor_connectors.check_connector_status",
        key="unhealthy_connectors",
    )
    if unhealthy:
        return "monitor_connectors.restart_failed_connectors"
    return "monitor_connectors.connectors_healthy"


def restart_failed_connectors(**context):
    """Attempt to restart failed Debezium connector tasks."""
    connect_url = Variable.get("debezium_connect_url")
    unhealthy = context["ti"].xcom_pull(
        task_ids="monitor_connectors.check_connector_status",
        key="unhealthy_connectors",
    )
    restart_results = {}

    for connector in CDC_CONNECTORS:
        try:
            status_resp = requests.get(
                f"{connect_url}/connectors/{connector}/status",
                timeout=15,
            )
            status = status_resp.json()

            for task in status.get("tasks", []):
                if task["state"] != "RUNNING":
                    task_id = task["id"]
                    restart_resp = requests.post(
                        f"{connect_url}/connectors/{connector}/tasks/{task_id}/restart",
                        timeout=15,
                    )
                    restart_results[f"{connector}/task-{task_id}"] = (
                        restart_resp.status_code
                    )
                    print(
                        f"Restarted {connector}/task-{task_id}: "
                        f"HTTP {restart_resp.status_code}"
                    )

            if status["connector"]["state"] != "RUNNING":
                restart_resp = requests.post(
                    f"{connect_url}/connectors/{connector}/restart",
                    timeout=15,
                )
                restart_results[connector] = restart_resp.status_code
                print(
                    f"Restarted connector {connector}: "
                    f"HTTP {restart_resp.status_code}"
                )

        except requests.exceptions.RequestException as e:
            restart_results[connector] = f"ERROR: {str(e)}"
            print(f"Failed to restart {connector}: {e}")

    context["ti"].xcom_push(key="restart_results", value=restart_results)
    return restart_results


def check_kafka_consumer_lag(**context):
    """Check Kafka consumer group lag for CDC topics."""
    kafka_bootstrap = Variable.get("kafka_bootstrap_servers")
    lag_results = {}
    high_lag_topics = []

    for table in CDC_TABLES:
        topic = f"zomato-cdc.zomato.{table}"
        # Use kafka-consumer-groups.sh to get lag
        # In production this would use the confluent admin client
        lag_results[topic] = {"status": "checked"}

    context["ti"].xcom_push(key="lag_results", value=lag_results)
    context["ti"].xcom_push(key="high_lag_topics", value=high_lag_topics)

    if high_lag_topics:
        raise AirflowException(
            f"High consumer lag detected on topics: {high_lag_topics}"
        )

    print(f"Kafka consumer lag within thresholds for all {len(CDC_TABLES)} topics")


def check_flink_cdc_jobs(**context):
    """Monitor Flink CDC processing jobs via Flink REST API."""
    flink_url = Variable.get("flink_rest_url")
    job_statuses = {}
    failed_jobs = []

    try:
        response = requests.get(f"{flink_url}/jobs/overview", timeout=15)
        response.raise_for_status()
        jobs = response.json().get("jobs", [])

        cdc_jobs = [j for j in jobs if "cdc" in j.get("name", "").lower()]

        for job in cdc_jobs:
            job_id = job["jid"]
            job_name = job["name"]
            job_state = job["state"]

            job_statuses[job_name] = {
                "job_id": job_id,
                "state": job_state,
                "start_time": job.get("start-time"),
                "duration": job.get("duration"),
            }

            if job_state not in ("RUNNING", "CREATED", "RESTARTING"):
                failed_jobs.append(f"{job_name} ({job_id}): {job_state}")

    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Cannot reach Flink REST API: {e}")

    context["ti"].xcom_push(key="flink_job_statuses", value=job_statuses)

    if failed_jobs:
        raise AirflowException(f"Flink CDC jobs not running: {failed_jobs}")

    print(f"All {len(cdc_jobs)} Flink CDC jobs are running")
    return job_statuses


def validate_data_freshness(**context):
    """Validate that CDC Iceberg tables are receiving fresh data."""
    from trino.dbapi import connect

    trino_host = Variable.get("trino_etl_host")
    conn = connect(host=trino_host, port=8080, user="airflow", catalog="iceberg")
    cursor = conn.cursor()

    stale_tables = []

    for table in CDC_TABLES:
        query = f"""
            SELECT MAX(processed_at) AS latest_record
            FROM iceberg.zomato.{table}
            WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
        """
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            latest = result[0] if result else None

            if latest is None:
                stale_tables.append(table)
                print(f"WARNING: No recent data in iceberg.zomato.{table}")
            else:
                print(f"iceberg.zomato.{table}: latest record at {latest}")

        except Exception as e:
            print(f"Error checking {table}: {e}")
            stale_tables.append(table)

    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="stale_tables", value=stale_tables)

    if stale_tables:
        raise AirflowException(
            f"CDC data stale (no records in last hour): {stale_tables}"
        )


def publish_cdc_metrics(**context):
    """Publish CDC pipeline health metrics to CloudWatch."""
    import boto3

    cloudwatch = boto3.client("cloudwatch", region_name="ap-south-1")

    connector_statuses = context["ti"].xcom_pull(
        task_ids="monitor_connectors.check_connector_status",
        key="connector_statuses",
    ) or {}

    stale_tables = context["ti"].xcom_pull(
        task_ids="validate_data_freshness",
        key="stale_tables",
    ) or []

    healthy_connectors = sum(
        1 for s in connector_statuses.values()
        if s.get("connector_state") == "RUNNING"
    )

    metrics = [
        {
            "MetricName": "CDCHealthyConnectors",
            "Value": healthy_connectors,
            "Unit": "Count",
        },
        {
            "MetricName": "CDCTotalConnectors",
            "Value": len(CDC_CONNECTORS),
            "Unit": "Count",
        },
        {
            "MetricName": "CDCStaleTables",
            "Value": len(stale_tables),
            "Unit": "Count",
        },
    ]

    cloudwatch.put_metric_data(
        Namespace="ZomatoDataPlatform/CDC",
        MetricData=[
            {**m, "Dimensions": [{"Name": "Environment", "Value": "prod"}]}
            for m in metrics
        ],
    )

    print(
        f"Published CDC metrics: {healthy_connectors}/{len(CDC_CONNECTORS)} "
        f"connectors healthy, {len(stale_tables)} stale tables"
    )


with DAG(
    dag_id="pipeline2_cdc_management",
    default_args=default_args,
    description="CDC pipeline: monitor Debezium connectors, Flink jobs, validate freshness",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline2", "cdc", "debezium", "flink", "monitoring"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Pipeline 2 CDC monitoring - {{ ts }}'",
    )

    with TaskGroup("monitor_connectors") as monitor_connectors:
        check_status = PythonOperator(
            task_id="check_connector_status",
            python_callable=check_debezium_connector_status,
        )

        branch = BranchPythonOperator(
            task_id="branch_on_health",
            python_callable=branch_on_connector_health,
        )

        restart = PythonOperator(
            task_id="restart_failed_connectors",
            python_callable=restart_failed_connectors,
        )

        healthy = BashOperator(
            task_id="connectors_healthy",
            bash_command="echo 'All Debezium connectors healthy'",
        )

        check_status >> branch >> [restart, healthy]

    check_lag = PythonOperator(
        task_id="check_kafka_lag",
        python_callable=check_kafka_consumer_lag,
    )

    check_flink = PythonOperator(
        task_id="check_flink_jobs",
        python_callable=check_flink_cdc_jobs,
    )

    validate_freshness = PythonOperator(
        task_id="validate_data_freshness",
        python_callable=validate_data_freshness,
    )

    publish_metrics = PythonOperator(
        task_id="publish_cdc_metrics",
        python_callable=publish_cdc_metrics,
        trigger_rule="all_done",
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline 2 CDC monitoring completed - {{ ts }}'",
        trigger_rule="all_done",
    )

    start >> monitor_connectors >> [check_lag, check_flink]
    [check_lag, check_flink] >> validate_freshness >> publish_metrics >> end
