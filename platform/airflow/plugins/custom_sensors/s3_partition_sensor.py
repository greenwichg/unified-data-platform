"""
Custom Airflow Sensor: S3 Partition Sensor

Waits for a specific S3 partition (prefix) to appear, with optional
validation of minimum file count and total size. Used to gate downstream
DAG tasks on data availability in the S3 data lake.

Example usage:
    wait_for_orders = S3PartitionSensor(
        task_id="wait_for_orders_partition",
        bucket="zomato-data-platform-prod-raw-data-lake",
        prefix="pipeline1-batch-etl/sqoop-output/orders/{{ ds }}/",
        min_file_count=1,
        min_total_bytes=1024,
        aws_conn_id="aws_default",
    )
"""

import logging
from typing import Optional, Sequence

import boto3
from botocore.exceptions import ClientError

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

logger = logging.getLogger(__name__)


class S3PartitionSensor(BaseSensorOperator):
    """
    Sensor that waits for an S3 prefix (partition) to contain data.

    :param bucket: S3 bucket name.
    :param prefix: S3 prefix (partition path) to check.
    :param min_file_count: Minimum number of files required (default 1).
    :param min_total_bytes: Minimum total size in bytes across all files (default 0).
    :param allowed_extensions: If set, only count files with these extensions.
    :param aws_conn_id: Airflow AWS connection ID.
    :param region_name: AWS region (default us-east-1).
    """

    template_fields: Sequence[str] = ("bucket", "prefix")
    ui_color = "#f0e68c"

    def __init__(
        self,
        bucket: str,
        prefix: str,
        min_file_count: int = 1,
        min_total_bytes: int = 0,
        allowed_extensions: Optional[list] = None,
        aws_conn_id: str = "aws_default",
        region_name: str = "us-east-1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/" if not prefix.endswith("/") else prefix
        self.min_file_count = min_file_count
        self.min_total_bytes = min_total_bytes
        self.allowed_extensions = allowed_extensions
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def _get_s3_client(self):
        """Create an S3 client. Uses Airflow connection if available."""
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            hook = S3Hook(aws_conn_id=self.aws_conn_id)
            return hook.get_conn()
        except Exception:
            return boto3.client("s3", region_name=self.region_name)

    def poke(self, context: Context) -> bool:
        """
        Check if the S3 partition exists and meets the minimum thresholds.
        """
        s3_client = self._get_s3_client()

        logger.info(
            "Checking s3://%s/%s (min_files=%d, min_bytes=%d)",
            self.bucket,
            self.prefix,
            self.min_file_count,
            self.min_total_bytes,
        )

        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)

            file_count = 0
            total_bytes = 0

            for page in pages:
                contents = page.get("Contents", [])
                for obj in contents:
                    key = obj["Key"]

                    # Skip directory markers
                    if key.endswith("/"):
                        continue

                    # Filter by extension if specified
                    if self.allowed_extensions:
                        if not any(key.endswith(ext) for ext in self.allowed_extensions):
                            continue

                    file_count += 1
                    total_bytes += obj["Size"]

            logger.info(
                "Found %d files, %d bytes at s3://%s/%s",
                file_count,
                total_bytes,
                self.bucket,
                self.prefix,
            )

            meets_count = file_count >= self.min_file_count
            meets_size = total_bytes >= self.min_total_bytes

            if meets_count and meets_size:
                logger.info("Partition criteria met.")
                return True

            if not meets_count:
                logger.info(
                    "Waiting: %d/%d files found.",
                    file_count,
                    self.min_file_count,
                )
            if not meets_size:
                logger.info(
                    "Waiting: %d/%d bytes found.",
                    total_bytes,
                    self.min_total_bytes,
                )

            return False

        except ClientError as e:
            logger.warning("S3 error while checking partition: %s", str(e))
            return False
