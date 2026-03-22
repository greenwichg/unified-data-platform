"""
Slack alerting callbacks for Airflow DAGs.

Provides on_failure_callback and on_retry_callback that post structured
alert messages to a Slack channel via webhook. Includes DAG, task, error
details, and links to the Airflow UI for quick triage.
"""

import json
import logging
from typing import Any, Dict, Optional

import requests

from airflow.models import Variable

logger = logging.getLogger(__name__)

# Slack webhook URL stored as an Airflow Variable for security
SLACK_WEBHOOK_VAR = "slack_webhook_url"
SLACK_CHANNEL_VAR = "slack_alert_channel"
AIRFLOW_BASE_URL_VAR = "airflow_base_url"

DEFAULT_CHANNEL = "#data-platform-alerts"
DEFAULT_AIRFLOW_URL = "https://airflow.zomato.internal"


def _get_slack_webhook() -> Optional[str]:
    """Retrieve the Slack webhook URL from Airflow Variables."""
    try:
        return Variable.get(SLACK_WEBHOOK_VAR)
    except Exception:
        logger.error(
            "Slack webhook URL not found in Airflow Variable '%s'. "
            "Alerts will not be sent.",
            SLACK_WEBHOOK_VAR,
        )
        return None


def _get_airflow_url() -> str:
    """Get the Airflow base URL for constructing task links."""
    try:
        return Variable.get(AIRFLOW_BASE_URL_VAR)
    except Exception:
        return DEFAULT_AIRFLOW_URL


def _get_channel() -> str:
    """Get the target Slack channel."""
    try:
        return Variable.get(SLACK_CHANNEL_VAR)
    except Exception:
        return DEFAULT_CHANNEL


def _build_task_url(context: Dict[str, Any]) -> str:
    """Build a direct URL to the failed task instance in the Airflow UI."""
    base_url = _get_airflow_url()
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else "unknown"
    execution_date = context.get("execution_date", "")
    return (
        f"{base_url}/dags/{dag_id}/grid"
        f"?dag_run_id={execution_date}&task_id={task_id}"
    )


def _post_to_slack(payload: Dict[str, Any]) -> bool:
    """Send a payload to the Slack webhook."""
    webhook_url = _get_slack_webhook()
    if not webhook_url:
        return False

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

        if response.status_code != 200:
            logger.error(
                "Slack webhook returned %d: %s",
                response.status_code,
                response.text,
            )
            return False

        return True

    except requests.RequestException as e:
        logger.error("Failed to send Slack alert: %s", str(e))
        return False


def on_failure_callback(context: Dict[str, Any]) -> None:
    """
    Airflow on_failure_callback that posts a detailed alert to Slack.

    Usage in DAG default_args:
        default_args = {
            "on_failure_callback": on_failure_callback,
            ...
        }
    """
    ti = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception")

    dag_id = dag.dag_id if dag else "unknown"
    task_id = ti.task_id if ti else "unknown"
    execution_date = str(context.get("execution_date", "unknown"))
    try_number = ti.try_number if ti else 0
    max_tries = ti.max_tries if ti else 0
    error_message = str(exception)[:1500] if exception else "No exception details"
    task_url = _build_task_url(context)
    channel = _get_channel()

    payload = {
        "channel": channel,
        "username": "Airflow Alert Bot",
        "icon_emoji": ":rotating_light:",
        "attachments": [
            {
                "color": "#ff0000",
                "title": f"Task Failure: {dag_id}.{task_id}",
                "title_link": task_url,
                "fields": [
                    {
                        "title": "DAG",
                        "value": dag_id,
                        "short": True,
                    },
                    {
                        "title": "Task",
                        "value": task_id,
                        "short": True,
                    },
                    {
                        "title": "Execution Date",
                        "value": execution_date,
                        "short": True,
                    },
                    {
                        "title": "Try",
                        "value": f"{try_number}/{max_tries + 1}",
                        "short": True,
                    },
                    {
                        "title": "Error",
                        "value": f"```{error_message}```",
                        "short": False,
                    },
                ],
                "footer": "Zomato Data Platform | Airflow",
                "ts": int(context.get("execution_date", 0).timestamp())
                if hasattr(context.get("execution_date"), "timestamp")
                else 0,
            }
        ],
    }

    success = _post_to_slack(payload)
    if success:
        logger.info("Slack failure alert sent for %s.%s", dag_id, task_id)
    else:
        logger.warning("Failed to send Slack alert for %s.%s", dag_id, task_id)


def on_retry_callback(context: Dict[str, Any]) -> None:
    """
    Airflow on_retry_callback that posts a warning to Slack.

    Less urgent than failure - posted with warning color.
    """
    ti = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception")

    dag_id = dag.dag_id if dag else "unknown"
    task_id = ti.task_id if ti else "unknown"
    try_number = ti.try_number if ti else 0
    max_tries = ti.max_tries if ti else 0
    error_message = str(exception)[:500] if exception else "No exception details"
    task_url = _build_task_url(context)
    channel = _get_channel()

    payload = {
        "channel": channel,
        "username": "Airflow Alert Bot",
        "icon_emoji": ":warning:",
        "attachments": [
            {
                "color": "#ffcc00",
                "title": f"Task Retry: {dag_id}.{task_id}",
                "title_link": task_url,
                "text": (
                    f"Attempt {try_number}/{max_tries + 1} failed. Retrying...\n"
                    f"Error: `{error_message}`"
                ),
                "footer": "Zomato Data Platform | Airflow",
            }
        ],
    }

    _post_to_slack(payload)


def on_sla_miss_callback(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: list,
    blocking_tis: list,
) -> None:
    """
    SLA miss callback that posts to Slack when a DAG misses its SLA.
    """
    dag_id = dag.dag_id if dag else "unknown"
    channel = _get_channel()

    payload = {
        "channel": channel,
        "username": "Airflow Alert Bot",
        "icon_emoji": ":clock3:",
        "attachments": [
            {
                "color": "#ff9900",
                "title": f"SLA Miss: {dag_id}",
                "fields": [
                    {
                        "title": "Affected Tasks",
                        "value": task_list,
                        "short": False,
                    },
                    {
                        "title": "Blocking Tasks",
                        "value": blocking_task_list or "None",
                        "short": False,
                    },
                ],
                "footer": "Zomato Data Platform | Airflow SLA Monitor",
            }
        ],
    }

    _post_to_slack(payload)
