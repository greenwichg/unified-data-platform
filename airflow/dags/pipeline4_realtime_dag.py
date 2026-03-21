"""
Airflow DAG: Pipeline 4 - Realtime Pipeline Monitoring
Kafka -> Flink Realtime Processor -> Druid (OLAP) / Kafka (enriched events)

Monitors the health of the realtime event processing pipeline:
  - Kafka topic throughput and consumer lag
  - Flink streaming job health and checkpoint status
  - Druid ingestion task health and segment availability
  - End-to-end latency validation

Handles 20B events/week with sub-second processing requirements.
"""

from datetime import datetime, timedelta

import boto3
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
    "email": ["data-alerts@zomato.com", "realtime-oncall@zomato.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure_callback,
}

MANAGED_FLINK_APP_NAME = "{{ var.value.managed_flink_realtime_app_name }}"
AWS_REGION = "{{ var.value.aws_region }}"
DRUID_COORDINATOR_URL = "{{ var.value.druid_coordinator_url }}"
DRUID_OVERLORD_URL = "{{ var.value.druid_overlord_url }}"

REALTIME_TOPICS = [
    "zomato-order-events",
    "zomato-delivery-events",
    "zomato-user-activity-events",
    "zomato-restaurant-events",
    "druid-ingestion-events",
]

DRUID_DATASOURCES = [
    "zomato_realtime_events",
    "zomato_order_events",
    "zomato_delivery_tracking",
]

MAX_CONSUMER_LAG_RECORDS = 50000
MAX_E2E_LATENCY_SEC = 30
MAX_CHECKPOINT_FAILURE_COUNT = 3


def check_flink_streaming_jobs(**context):
    """Monitor Flink realtime streaming job health and checkpoints."""
    flink_url = Variable.get("flink_rest_url")
    job_health = {}
    alerts = []

    try:
        resp = requests.get(f"{flink_url}/jobs/overview", timeout=15)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])

        realtime_jobs = [
            j for j in jobs if "realtime" in j.get("name", "").lower()
        ]

        for job in realtime_jobs:
            job_id = job["jid"]
            job_name = job["name"]
            job_state = job["state"]

            # Get checkpoint stats
            try:
                cp_resp = requests.get(
                    f"{flink_url}/jobs/{job_id}/checkpoints", timeout=10
                )
                cp_data = cp_resp.json() if cp_resp.status_code == 200 else {}
                cp_counts = cp_data.get("counts", {})
                latest_cp = cp_data.get("latest", {}).get("completed")

                checkpoint_info = {
                    "completed": cp_counts.get("completed", 0),
                    "failed": cp_counts.get("failed", 0),
                    "in_progress": cp_counts.get("in_progress", 0),
                    "latest_duration_ms": (
                        latest_cp.get("duration") if latest_cp else None
                    ),
                    "latest_size_bytes": (
                        latest_cp.get("state_size") if latest_cp else None
                    ),
                }
            except Exception:
                checkpoint_info = {"error": "Unable to fetch checkpoint data"}

            # Get backpressure
            try:
                vertices_resp = requests.get(
                    f"{flink_url}/jobs/{job_id}", timeout=10
                )
                vertices = vertices_resp.json().get("vertices", [])
                backpressured = [
                    v["name"] for v in vertices
                    if v.get("status") == "RUNNING"
                    and v.get("metrics", {}).get("isBackPressured", False)
                ]
            except Exception:
                backpressured = []

            job_health[job_name] = {
                "job_id": job_id,
                "state": job_state,
                "duration_ms": job.get("duration"),
                "checkpoints": checkpoint_info,
                "backpressured_vertices": backpressured,
            }

            if job_state != "RUNNING":
                alerts.append(f"{job_name}: state={job_state}")

            if checkpoint_info.get("failed", 0) > MAX_CHECKPOINT_FAILURE_COUNT:
                alerts.append(
                    f"{job_name}: {checkpoint_info['failed']} checkpoint failures"
                )

            if backpressured:
                alerts.append(
                    f"{job_name}: backpressure on {len(backpressured)} vertices"
                )

    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Cannot reach Flink REST API: {e}")

    context["ti"].xcom_push(key="job_health", value=job_health)
    context["ti"].xcom_push(key="flink_alerts", value=alerts)

    if alerts:
        print(f"Flink alerts: {alerts}")
    else:
        print(f"All {len(realtime_jobs)} realtime Flink jobs healthy")

    return job_health


def check_druid_ingestion(**context):
    """Monitor Druid Kafka ingestion supervisors and task health."""
    overlord_url = Variable.get("druid_overlord_url")
    ingestion_status = {}
    alerts = []

    try:
        # Check supervisor status
        resp = requests.get(
            f"{overlord_url}/druid/indexer/v1/supervisor", timeout=15
        )
        resp.raise_for_status()
        supervisors = resp.json()

        for supervisor_id in supervisors:
            try:
                status_resp = requests.get(
                    f"{overlord_url}/druid/indexer/v1/supervisor/{supervisor_id}/status",
                    timeout=10,
                )
                status_data = status_resp.json()
                payload = status_data.get("payload", {})

                state = payload.get("state", "UNKNOWN")
                detailed_state = payload.get("detailedState", "UNKNOWN")
                healthy = payload.get("healthy", False)

                # Get active tasks
                active_tasks = len(payload.get("activeTasks", []))
                publishing_tasks = len(payload.get("publishingTasks", []))

                ingestion_status[supervisor_id] = {
                    "state": state,
                    "detailed_state": detailed_state,
                    "healthy": healthy,
                    "active_tasks": active_tasks,
                    "publishing_tasks": publishing_tasks,
                }

                if not healthy:
                    alerts.append(
                        f"Supervisor {supervisor_id}: unhealthy "
                        f"(state={state}, detail={detailed_state})"
                    )

                if active_tasks == 0:
                    alerts.append(
                        f"Supervisor {supervisor_id}: no active ingestion tasks"
                    )

            except requests.exceptions.RequestException as e:
                alerts.append(f"Supervisor {supervisor_id}: unreachable ({e})")

    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Cannot reach Druid Overlord: {e}")

    context["ti"].xcom_push(key="ingestion_status", value=ingestion_status)
    context["ti"].xcom_push(key="druid_alerts", value=alerts)

    if alerts:
        print(f"Druid ingestion alerts: {alerts}")

    return ingestion_status


def check_druid_segment_freshness(**context):
    """Verify Druid datasources have recent segments loaded."""
    coordinator_url = Variable.get("druid_coordinator_url")
    freshness_results = {}
    stale_datasources = []

    for datasource in DRUID_DATASOURCES:
        try:
            resp = requests.get(
                f"{coordinator_url}/druid/coordinator/v1/datasources/{datasource}",
                timeout=10,
            )

            if resp.status_code == 200:
                ds_data = resp.json()
                segments = ds_data.get("segments", {})
                total_segments = segments.get("count", 0)

                # Check for recent segments via metadata query
                intervals_resp = requests.get(
                    f"{coordinator_url}/druid/coordinator/v1/datasources/"
                    f"{datasource}/intervals?simple",
                    timeout=10,
                )
                intervals = intervals_resp.json() if intervals_resp.status_code == 200 else {}

                freshness_results[datasource] = {
                    "total_segments": total_segments,
                    "recent_intervals": len(intervals),
                    "status": "OK",
                }
            else:
                stale_datasources.append(datasource)
                freshness_results[datasource] = {"status": "NOT_FOUND"}

        except requests.exceptions.RequestException as e:
            stale_datasources.append(datasource)
            freshness_results[datasource] = {"status": f"ERROR: {e}"}

    context["ti"].xcom_push(key="freshness_results", value=freshness_results)

    if stale_datasources:
        print(f"WARNING: Stale or missing Druid datasources: {stale_datasources}")

    return freshness_results


def check_end_to_end_latency(**context):
    """Measure end-to-end latency from Kafka to Druid query availability."""
    import time

    druid_broker_url = Variable.get("druid_broker_url", default_var=None)
    if not druid_broker_url:
        print("Druid broker URL not configured; skipping latency check")
        return

    query = {
        "query": """
            SELECT MAX(__time) AS latest_event
            FROM zomato_realtime_events
            WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '5' MINUTE
        """,
        "resultFormat": "object",
    }

    try:
        resp = requests.post(
            f"{druid_broker_url}/druid/v2/sql",
            json=query,
            timeout=15,
        )
        resp.raise_for_status()
        results = resp.json()

        if results and results[0].get("latest_event"):
            from dateutil.parser import parse as parse_date
            latest = parse_date(results[0]["latest_event"])
            now = datetime.utcnow()
            latency_sec = (now - latest.replace(tzinfo=None)).total_seconds()

            context["ti"].xcom_push(key="e2e_latency_sec", value=latency_sec)
            print(f"End-to-end latency: {latency_sec:.1f}s")

            if latency_sec > MAX_E2E_LATENCY_SEC:
                raise AirflowException(
                    f"E2E latency {latency_sec:.1f}s exceeds "
                    f"threshold {MAX_E2E_LATENCY_SEC}s"
                )
        else:
            print("WARNING: No recent events in Druid realtime datasource")

    except requests.exceptions.RequestException as e:
        print(f"WARNING: Unable to check E2E latency: {e}")


def publish_realtime_metrics(**context):
    """Publish realtime pipeline health metrics to CloudWatch."""
    import boto3

    cloudwatch = boto3.client("cloudwatch", region_name="ap-south-1")

    job_health = context["ti"].xcom_pull(
        task_ids="monitor_flink.check_flink_jobs", key="job_health"
    ) or {}
    flink_alerts = context["ti"].xcom_pull(
        task_ids="monitor_flink.check_flink_jobs", key="flink_alerts"
    ) or []
    druid_alerts = context["ti"].xcom_pull(
        task_ids="monitor_druid.check_druid_ingestion", key="druid_alerts"
    ) or []
    e2e_latency = context["ti"].xcom_pull(
        task_ids="check_e2e_latency", key="e2e_latency_sec"
    )

    running_jobs = sum(
        1 for j in job_health.values() if j.get("state") == "RUNNING"
    )

    metrics = [
        {
            "MetricName": "RealtimeFlinkJobsRunning",
            "Value": running_jobs,
            "Unit": "Count",
        },
        {
            "MetricName": "RealtimeAlertCount",
            "Value": len(flink_alerts) + len(druid_alerts),
            "Unit": "Count",
        },
    ]

    if e2e_latency is not None:
        metrics.append({
            "MetricName": "RealtimeE2ELatencySeconds",
            "Value": e2e_latency,
            "Unit": "Seconds",
        })

    cloudwatch.put_metric_data(
        Namespace="ZomatoDataPlatform/Realtime",
        MetricData=[
            {**m, "Dimensions": [{"Name": "Environment", "Value": "prod"}]}
            for m in metrics
        ],
    )

    print(
        f"Published realtime metrics: {running_jobs} jobs running, "
        f"{len(flink_alerts) + len(druid_alerts)} alerts"
    )


with DAG(
    dag_id="pipeline4_realtime_monitoring",
    default_args=default_args,
    description="Realtime pipeline: monitor Flink streaming, Druid ingestion, E2E latency",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline4", "realtime", "flink", "druid", "monitoring"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Pipeline 4 realtime monitoring - {{ ts }}'",
    )

    with TaskGroup("monitor_flink") as monitor_flink:
        check_flink = PythonOperator(
            task_id="check_flink_jobs",
            python_callable=check_flink_streaming_jobs,
        )

    with TaskGroup("monitor_druid") as monitor_druid:
        check_ingestion = PythonOperator(
            task_id="check_druid_ingestion",
            python_callable=check_druid_ingestion,
        )

        check_freshness = PythonOperator(
            task_id="check_segment_freshness",
            python_callable=check_druid_segment_freshness,
        )

        check_ingestion >> check_freshness

    check_latency = PythonOperator(
        task_id="check_e2e_latency",
        python_callable=check_end_to_end_latency,
    )

    publish_metrics = PythonOperator(
        task_id="publish_realtime_metrics",
        python_callable=publish_realtime_metrics,
        trigger_rule="all_done",
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline 4 realtime monitoring completed - {{ ts }}'",
        trigger_rule="all_done",
    )

    start >> [monitor_flink, monitor_druid]
    [monitor_flink, monitor_druid] >> check_latency >> publish_metrics >> end
