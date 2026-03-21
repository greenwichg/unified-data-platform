"""
Airflow DAG: Druid Maintenance
Automated maintenance for Apache Druid OLAP cluster:
  - Segment compaction: merge small segments into optimal sizes
  - Segment cleanup: remove old segments beyond retention policy
  - Kill unused tasks: clean up zombie ingestion tasks
  - Datasource health monitoring: segment count, availability, load status

Druid handles 20B events/week and 8M queries/week for realtime analytics.
"""

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": on_failure_callback,
}

DRUID_COORDINATOR_URL = "{{ var.value.druid_coordinator_url }}"
DRUID_OVERLORD_URL = "{{ var.value.druid_overlord_url }}"
DRUID_ROUTER_URL = "{{ var.value.druid_router_url }}"

# Datasources with retention and compaction configuration
DATASOURCE_CONFIG = {
    "zomato_realtime_events": {
        "retention_days": 90,
        "compaction_target_size_bytes": 500_000_000,  # 500MB
        "compaction_max_rows_per_segment": 5_000_000,
        "compaction_skip_offset_from_latest": "P1D",
    },
    "zomato_order_events": {
        "retention_days": 180,
        "compaction_target_size_bytes": 500_000_000,
        "compaction_max_rows_per_segment": 5_000_000,
        "compaction_skip_offset_from_latest": "P1D",
    },
    "zomato_delivery_tracking": {
        "retention_days": 60,
        "compaction_target_size_bytes": 300_000_000,
        "compaction_max_rows_per_segment": 3_000_000,
        "compaction_skip_offset_from_latest": "PT6H",
    },
}


def submit_compaction_tasks(**context):
    """Submit manual compaction tasks for each datasource."""
    coordinator_url = Variable.get("druid_coordinator_url")
    compaction_results = {}

    for datasource, config in DATASOURCE_CONFIG.items():
        compaction_config = {
            "dataSource": datasource,
            "taskPriority": 25,
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsPerSegment": config["compaction_max_rows_per_segment"],
                "maxRowsInMemory": 1_000_000,
                "maxBytesInMemory": 536_870_912,
                "maxTotalRows": config["compaction_max_rows_per_segment"] * 4,
                "splitHintSpec": {
                    "type": "maxSize",
                    "maxSplitSize": config["compaction_target_size_bytes"],
                },
                "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": config["compaction_max_rows_per_segment"],
                    "maxTotalRows": config["compaction_max_rows_per_segment"] * 10,
                },
            },
            "granularitySpec": {
                "segmentGranularity": "HOUR",
                "queryGranularity": "MINUTE",
            },
            "skipOffsetFromLatest": config["compaction_skip_offset_from_latest"],
        }

        try:
            # Update auto-compaction config
            resp = requests.post(
                f"{coordinator_url}/druid/coordinator/v1/config/compaction",
                json=compaction_config,
                timeout=30,
            )

            compaction_results[datasource] = {
                "status_code": resp.status_code,
                "success": resp.status_code in (200, 204),
            }

            if resp.status_code in (200, 204):
                print(f"Compaction config updated for {datasource}")
            else:
                print(
                    f"WARNING: Failed to update compaction for {datasource}: "
                    f"HTTP {resp.status_code} - {resp.text}"
                )

        except requests.exceptions.RequestException as e:
            compaction_results[datasource] = {"error": str(e)}
            print(f"ERROR: Compaction request failed for {datasource}: {e}")

    context["ti"].xcom_push(key="compaction_results", value=compaction_results)
    return compaction_results


def enforce_retention_rules(**context):
    """Drop segments older than retention policy for each datasource."""
    coordinator_url = Variable.get("druid_coordinator_url")
    retention_results = {}

    for datasource, config in DATASOURCE_CONFIG.items():
        retention_days = config["retention_days"]
        cutoff_date = (
            datetime.utcnow() - timedelta(days=retention_days)
        ).strftime("%Y-%m-%dT00:00:00.000Z")

        # Set kill task to remove segments before cutoff
        kill_task = {
            "type": "kill",
            "dataSource": datasource,
            "interval": f"2020-01-01T00:00:00.000Z/{cutoff_date}",
            "markAsUnused": True,
        }

        try:
            # First mark segments as unused
            resp = requests.post(
                f"{coordinator_url}/druid/coordinator/v1/datasources/"
                f"{datasource}/markUnused",
                json={
                    "interval": f"2020-01-01T00:00:00.000Z/{cutoff_date}",
                },
                timeout=30,
            )

            retention_results[datasource] = {
                "cutoff_date": cutoff_date,
                "retention_days": retention_days,
                "mark_unused_status": resp.status_code,
            }

            print(
                f"{datasource}: marked segments before {cutoff_date} as unused "
                f"(HTTP {resp.status_code})"
            )

        except requests.exceptions.RequestException as e:
            retention_results[datasource] = {"error": str(e)}
            print(f"ERROR: Retention enforcement failed for {datasource}: {e}")

    context["ti"].xcom_push(key="retention_results", value=retention_results)
    return retention_results


def kill_zombie_tasks(**context):
    """Identify and kill long-running or stuck ingestion tasks."""
    overlord_url = Variable.get("druid_overlord_url")
    max_task_duration_hours = 6
    killed_tasks = []

    try:
        # Get running tasks
        resp = requests.get(
            f"{overlord_url}/druid/indexer/v1/runningTasks", timeout=15
        )
        resp.raise_for_status()
        running_tasks = resp.json()

        now = datetime.utcnow()

        for task in running_tasks:
            task_id = task.get("id", "")
            created_time_str = task.get("createdTime")

            if not created_time_str:
                continue

            from dateutil.parser import parse as parse_date
            created_time = parse_date(created_time_str).replace(tzinfo=None)
            task_age_hours = (now - created_time).total_seconds() / 3600

            # Kill tasks running longer than threshold (excluding supervisors)
            task_type = task.get("type", "")
            if (
                task_age_hours > max_task_duration_hours
                and task_type not in ("supervisor",)
            ):
                try:
                    kill_resp = requests.post(
                        f"{overlord_url}/druid/indexer/v1/task/{task_id}/shutdown",
                        timeout=15,
                    )
                    killed_tasks.append({
                        "task_id": task_id,
                        "type": task_type,
                        "age_hours": round(task_age_hours, 1),
                        "kill_status": kill_resp.status_code,
                    })
                    print(
                        f"Killed zombie task {task_id} "
                        f"(type={task_type}, age={task_age_hours:.1f}h)"
                    )

                except requests.exceptions.RequestException as e:
                    print(f"Failed to kill task {task_id}: {e}")

    except requests.exceptions.RequestException as e:
        print(f"WARNING: Unable to check running tasks: {e}")

    context["ti"].xcom_push(key="killed_tasks", value=killed_tasks)
    print(f"Killed {len(killed_tasks)} zombie tasks")
    return killed_tasks


def check_datasource_health(**context):
    """Monitor overall datasource health: segment counts, availability, load."""
    coordinator_url = Variable.get("druid_coordinator_url")
    health_report = {}
    alerts = []

    for datasource in DATASOURCE_CONFIG:
        try:
            # Get datasource metadata
            resp = requests.get(
                f"{coordinator_url}/druid/coordinator/v1/datasources/{datasource}",
                timeout=15,
            )

            if resp.status_code != 200:
                alerts.append(f"{datasource}: not found (HTTP {resp.status_code})")
                health_report[datasource] = {"status": "NOT_FOUND"}
                continue

            ds_data = resp.json()
            segments_info = ds_data.get("segments", {})
            total_segments = segments_info.get("count", 0)
            total_size_bytes = segments_info.get("size", 0)

            # Get load status
            load_resp = requests.get(
                f"{coordinator_url}/druid/coordinator/v1/datasources/"
                f"{datasource}/loadstatus?simple",
                timeout=10,
            )
            load_data = load_resp.json() if load_resp.status_code == 200 else {}
            load_pct = load_data.get(datasource, 0)

            health_report[datasource] = {
                "total_segments": total_segments,
                "total_size_gb": round(total_size_bytes / (1024**3), 2),
                "load_percentage": load_pct,
                "status": "HEALTHY" if load_pct >= 100 else "LOADING",
            }

            if load_pct < 100:
                alerts.append(
                    f"{datasource}: only {load_pct}% segments loaded"
                )

            if total_segments == 0:
                alerts.append(f"{datasource}: zero segments")

            print(
                f"{datasource}: {total_segments} segments, "
                f"{total_size_bytes / (1024**3):.2f} GB, "
                f"{load_pct}% loaded"
            )

        except requests.exceptions.RequestException as e:
            alerts.append(f"{datasource}: health check failed ({e})")
            health_report[datasource] = {"status": f"ERROR: {e}"}

    context["ti"].xcom_push(key="health_report", value=health_report)
    context["ti"].xcom_push(key="health_alerts", value=alerts)

    if alerts:
        print(f"Druid health alerts: {alerts}")

    return health_report


def publish_druid_metrics(**context):
    """Publish Druid maintenance metrics to CloudWatch."""
    import boto3

    cloudwatch = boto3.client("cloudwatch", region_name="ap-south-1")

    health_report = context["ti"].xcom_pull(
        task_ids="health_monitoring.check_datasource_health",
        key="health_report",
    ) or {}
    health_alerts = context["ti"].xcom_pull(
        task_ids="health_monitoring.check_datasource_health",
        key="health_alerts",
    ) or []
    killed_tasks = context["ti"].xcom_pull(
        task_ids="cleanup.kill_zombie_tasks",
        key="killed_tasks",
    ) or []

    total_segments = sum(
        d.get("total_segments", 0) for d in health_report.values()
    )
    total_size_gb = sum(
        d.get("total_size_gb", 0) for d in health_report.values()
    )
    healthy_ds = sum(
        1 for d in health_report.values() if d.get("status") == "HEALTHY"
    )

    metrics = [
        {
            "MetricName": "DruidTotalSegments",
            "Value": total_segments,
            "Unit": "Count",
        },
        {
            "MetricName": "DruidTotalSizeGB",
            "Value": total_size_gb,
            "Unit": "Gigabytes",
        },
        {
            "MetricName": "DruidHealthyDatasources",
            "Value": healthy_ds,
            "Unit": "Count",
        },
        {
            "MetricName": "DruidAlertCount",
            "Value": len(health_alerts),
            "Unit": "Count",
        },
        {
            "MetricName": "DruidZombieTasksKilled",
            "Value": len(killed_tasks),
            "Unit": "Count",
        },
    ]

    cloudwatch.put_metric_data(
        Namespace="ZomatoDataPlatform/Druid",
        MetricData=[
            {**m, "Dimensions": [{"Name": "Environment", "Value": "prod"}]}
            for m in metrics
        ],
    )

    print(
        f"Published Druid metrics: {total_segments} segments, "
        f"{total_size_gb:.2f} GB, {healthy_ds}/{len(DATASOURCE_CONFIG)} healthy, "
        f"{len(killed_tasks)} zombie tasks killed"
    )


with DAG(
    dag_id="druid_maintenance",
    default_args=default_args,
    description="Druid maintenance: compaction, retention, cleanup, health monitoring",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["druid", "maintenance", "compaction", "cleanup"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Druid maintenance started - {{ ds }}'",
    )

    with TaskGroup("compaction") as compaction:
        submit_compaction = PythonOperator(
            task_id="submit_compaction_tasks",
            python_callable=submit_compaction_tasks,
        )

    with TaskGroup("retention") as retention:
        enforce_retention = PythonOperator(
            task_id="enforce_retention_rules",
            python_callable=enforce_retention_rules,
        )

    with TaskGroup("cleanup") as cleanup:
        kill_zombies = PythonOperator(
            task_id="kill_zombie_tasks",
            python_callable=kill_zombie_tasks,
        )

    with TaskGroup("health_monitoring") as health_monitoring:
        check_health = PythonOperator(
            task_id="check_datasource_health",
            python_callable=check_datasource_health,
        )

    publish_metrics = PythonOperator(
        task_id="publish_druid_metrics",
        python_callable=publish_druid_metrics,
        trigger_rule="all_done",
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Druid maintenance completed - {{ ds }}'",
        trigger_rule="all_done",
    )

    start >> [compaction, retention, cleanup]
    [compaction, retention, cleanup] >> health_monitoring >> publish_metrics >> end
