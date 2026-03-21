"""
Helper functions for EMR step submission and monitoring.

Provides utilities for:
  - Submitting Spark/Hive/Sqoop steps to an existing EMR cluster
  - Monitoring step completion with configurable polling
  - Retrieving step logs from S3
  - Building common EMR step configurations
"""

import logging
import time
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

DEFAULT_REGION = "us-east-1"
POLL_INTERVAL_SECONDS = 30
MAX_POLL_ATTEMPTS = 240  # 2 hours at 30s intervals

TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}


def get_emr_client(region: str = DEFAULT_REGION) -> boto3.client:
    """Get a boto3 EMR client."""
    return boto3.client("emr", region_name=region)


def submit_emr_step(
    cluster_id: str,
    step_name: str,
    jar_args: List[str],
    action_on_failure: str = "CONTINUE",
    jar: str = "command-runner.jar",
    region: str = DEFAULT_REGION,
) -> str:
    """
    Submit a single step to an EMR cluster.

    :param cluster_id: EMR cluster ID (e.g., j-XXXXXXXXXXXXX)
    :param step_name: Human-readable name for the step
    :param jar_args: Arguments for the HadoopJarStep
    :param action_on_failure: CONTINUE, CANCEL_AND_WAIT, or TERMINATE_CLUSTER
    :param jar: JAR to execute (default: command-runner.jar)
    :param region: AWS region
    :returns: Step ID
    """
    client = get_emr_client(region)

    step_config = {
        "Name": step_name,
        "ActionOnFailure": action_on_failure,
        "HadoopJarStep": {
            "Jar": jar,
            "Args": jar_args,
        },
    }

    logger.info("Submitting EMR step '%s' to cluster %s", step_name, cluster_id)

    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[step_config],
    )

    step_id = response["StepIds"][0]
    logger.info("Step submitted: %s (ID: %s)", step_name, step_id)
    return step_id


def submit_spark_step(
    cluster_id: str,
    step_name: str,
    script_s3_path: str,
    script_args: Optional[List[str]] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    deploy_mode: str = "cluster",
    region: str = DEFAULT_REGION,
) -> str:
    """
    Submit a PySpark job to EMR.

    :param cluster_id: EMR cluster ID
    :param step_name: Step name
    :param script_s3_path: S3 path to the PySpark script
    :param script_args: Arguments to pass to the script
    :param spark_conf: Spark configuration overrides
    :param deploy_mode: Spark deploy mode (cluster or client)
    :param region: AWS region
    :returns: Step ID
    """
    args = [
        "spark-submit",
        "--deploy-mode", deploy_mode,
        "--master", "yarn",
        "--conf", "spark.dynamicAllocation.enabled=true",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.sql.orc.enabled=true",
    ]

    if spark_conf:
        for key, value in spark_conf.items():
            args.extend(["--conf", f"{key}={value}"])

    args.append(script_s3_path)

    if script_args:
        args.extend(script_args)

    return submit_emr_step(
        cluster_id=cluster_id,
        step_name=step_name,
        jar_args=args,
        region=region,
    )


def wait_for_step(
    cluster_id: str,
    step_id: str,
    poll_interval: int = POLL_INTERVAL_SECONDS,
    max_attempts: int = MAX_POLL_ATTEMPTS,
    region: str = DEFAULT_REGION,
) -> Dict:
    """
    Poll an EMR step until it reaches a terminal state.

    :param cluster_id: EMR cluster ID
    :param step_id: Step ID to monitor
    :param poll_interval: Seconds between polls
    :param max_attempts: Maximum number of polling attempts
    :param region: AWS region
    :returns: Final step status dict
    :raises TimeoutError: If max_attempts exceeded
    :raises RuntimeError: If step fails
    """
    client = get_emr_client(region)

    for attempt in range(1, max_attempts + 1):
        response = client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id,
        )

        step = response["Step"]
        state = step["Status"]["State"]

        logger.info(
            "Step %s status: %s (poll %d/%d)",
            step_id, state, attempt, max_attempts,
        )

        if state in TERMINAL_STATES:
            if state == "COMPLETED":
                logger.info("Step %s completed successfully.", step_id)
                return step
            else:
                failure_reason = (
                    step["Status"]
                    .get("FailureDetails", {})
                    .get("Reason", "Unknown")
                )
                raise RuntimeError(
                    f"EMR step {step_id} ended with state {state}: {failure_reason}"
                )

        time.sleep(poll_interval)

    raise TimeoutError(
        f"EMR step {step_id} did not complete within "
        f"{max_attempts * poll_interval} seconds"
    )


def get_step_logs(
    cluster_id: str,
    step_id: str,
    log_type: str = "stderr",
    region: str = DEFAULT_REGION,
) -> Optional[str]:
    """
    Retrieve step logs from S3.

    :param cluster_id: EMR cluster ID
    :param step_id: Step ID
    :param log_type: Log type: 'stdout', 'stderr', or 'controller'
    :param region: AWS region
    :returns: Log content as string, or None if not found
    """
    emr_client = get_emr_client(region)
    s3_client = boto3.client("s3", region_name=region)

    try:
        cluster = emr_client.describe_cluster(ClusterId=cluster_id)
        log_uri = cluster["Cluster"].get("LogUri", "")

        if not log_uri:
            logger.warning("No log URI configured for cluster %s", cluster_id)
            return None

        log_uri = log_uri.rstrip("/")
        bucket = log_uri.replace("s3://", "").split("/")[0]
        prefix = "/".join(log_uri.replace("s3://", "").split("/")[1:])
        key = f"{prefix}/{cluster_id}/steps/{step_id}/{log_type}.gz"

        response = s3_client.get_object(Bucket=bucket, Key=key)
        import gzip
        return gzip.decompress(response["Body"].read()).decode("utf-8")

    except ClientError as e:
        logger.warning("Could not retrieve logs: %s", str(e))
        return None


def get_cluster_status(
    cluster_id: str,
    region: str = DEFAULT_REGION,
) -> Dict:
    """
    Get the current status of an EMR cluster.

    :param cluster_id: EMR cluster ID
    :param region: AWS region
    :returns: Cluster status dict with state and instance counts
    """
    client = get_emr_client(region)

    cluster = client.describe_cluster(ClusterId=cluster_id)["Cluster"]
    instance_groups = client.list_instance_groups(ClusterId=cluster_id)

    running_instances = sum(
        ig["RunningInstanceCount"]
        for ig in instance_groups["InstanceGroups"]
    )

    return {
        "cluster_id": cluster_id,
        "state": cluster["Status"]["State"],
        "name": cluster["Name"],
        "running_instances": running_instances,
        "normalized_instance_hours": cluster.get("NormalizedInstanceHours", 0),
    }
