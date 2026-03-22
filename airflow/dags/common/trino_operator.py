"""
Custom Airflow Operator for executing SQL against Amazon Athena.

Migrated from self-hosted Trino to Amazon Athena (serverless).
Wraps the AthenaOperator from apache-airflow-providers-amazon to provide:
  - Parameterized database/workgroup/output location
  - Automatic retry with exponential backoff on transient failures
  - Query result logging and optional XCom push
  - Support for multi-statement SQL (semicolon-separated)

The TrinoOperator name is preserved as a backward-compatible alias.
"""

import time
import logging
from typing import Any, Optional, Sequence

import boto3
from botocore.exceptions import ClientError

from airflow.models import BaseOperator
from airflow.utils.context import Context

logger = logging.getLogger(__name__)

# Athena error codes that are safe to retry
RETRYABLE_ERROR_STATES = {
    "INTERNAL_ERROR",
    "SERVICE_ERROR",
    "THROTTLING",
}


class AthenaQueryOperator(BaseOperator):
    """
    Execute SQL statements against Amazon Athena.

    :param sql: SQL statement(s) to execute. Multiple statements separated by semicolons.
    :param database: Glue Data Catalog database to use (e.g., 'zomato').
    :param workgroup: Athena workgroup (e.g., 'etl', 'adhoc', 'reporting').
    :param output_location: S3 path for Athena query results.
    :param aws_conn_id: Airflow AWS connection ID (default 'aws_default').
    :param region_name: AWS region (default 'ap-south-1').
    :param query_timeout: Maximum query execution time in seconds (default 3600).
    :param max_retries: Number of retries on transient failures (default 3).
    :param retry_backoff_seconds: Initial backoff between retries in seconds (default 30).
    :param push_results: Whether to push query results to XCom (default False).
    """

    template_fields: Sequence[str] = ("sql", "database", "output_location", "workgroup")
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#e8d0ef"

    def __init__(
        self,
        sql: str,
        database: str = "zomato",
        workgroup: str = "etl",
        output_location: str = "",
        aws_conn_id: str = "aws_default",
        region_name: str = "ap-south-1",
        query_timeout: int = 3600,
        max_retries: int = 3,
        retry_backoff_seconds: int = 30,
        push_results: bool = False,
        # Legacy parameters accepted but ignored for backward compatibility
        trino_host: str = "",
        trino_port: int = 0,
        catalog: str = "",
        schema: str = "",
        user: str = "",
        source: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.database = database or schema or "zomato"
        self.workgroup = workgroup
        self.output_location = output_location
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.query_timeout = query_timeout
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.push_results = push_results

    def _get_athena_client(self):
        """Create a new Athena client."""
        return boto3.client("athena", region_name=self.region_name)

    def _execute_with_retry(self, sql_statement: str) -> Optional[list]:
        """Execute a single SQL statement with retry logic."""
        last_exception = None
        client = self._get_athena_client()

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    "Executing on Athena workgroup '%s' (attempt %d/%d):\n%s",
                    self.workgroup,
                    attempt,
                    self.max_retries,
                    sql_statement.strip()[:500],
                )

                exec_params = {
                    "QueryString": sql_statement,
                    "WorkGroup": self.workgroup,
                    "QueryExecutionContext": {"Database": self.database},
                }
                if self.output_location:
                    exec_params["ResultConfiguration"] = {
                        "OutputLocation": self.output_location,
                    }

                response = client.start_query_execution(**exec_params)
                execution_id = response["QueryExecutionId"]

                # Poll for completion
                elapsed = 0
                poll_interval = 5
                while elapsed < self.query_timeout:
                    status_resp = client.get_query_execution(
                        QueryExecutionId=execution_id
                    )
                    state = status_resp["QueryExecution"]["Status"]["State"]

                    if state == "SUCCEEDED":
                        break
                    elif state in ("FAILED", "CANCELLED"):
                        reason = status_resp["QueryExecution"]["Status"].get(
                            "StateChangeReason", "unknown"
                        )
                        raise RuntimeError(
                            f"Athena query {state}: {reason} "
                            f"(execution_id={execution_id})"
                        )

                    time.sleep(poll_interval)
                    elapsed += poll_interval
                else:
                    # Timeout: cancel the query
                    client.stop_query_execution(QueryExecutionId=execution_id)
                    raise TimeoutError(
                        f"Athena query timed out after {self.query_timeout}s "
                        f"(execution_id={execution_id})"
                    )

                # Fetch results if needed
                results = []
                if self.push_results:
                    result_resp = client.get_query_results(
                        QueryExecutionId=execution_id
                    )
                    rows = result_resp["ResultSet"]["Rows"]
                    # Skip the header row
                    for row in rows[1:]:
                        results.append(
                            [col.get("VarCharValue") for col in row["Data"]]
                        )

                row_count = len(results)
                logger.info(
                    "Query completed successfully (execution_id=%s). Rows returned: %d",
                    execution_id,
                    row_count,
                )
                return results

            except (ClientError, RuntimeError) as e:
                last_exception = e
                error_code = ""
                if isinstance(e, ClientError):
                    error_code = e.response.get("Error", {}).get("Code", "")

                if (
                    error_code in RETRYABLE_ERROR_STATES
                    and attempt < self.max_retries
                ):
                    backoff = self.retry_backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "Retryable Athena error '%s' on attempt %d/%d. "
                        "Retrying in %ds: %s",
                        error_code,
                        attempt,
                        self.max_retries,
                        backoff,
                        str(e),
                    )
                    time.sleep(backoff)
                else:
                    raise
            except Exception as e:
                logger.error("Non-retryable error executing Athena query: %s", str(e))
                raise

        raise last_exception

    def execute(self, context: Context) -> Optional[list]:
        """Execute SQL against Athena, handling multi-statement SQL."""
        statements = [
            stmt.strip()
            for stmt in self.sql.split(";")
            if stmt.strip()
        ]

        logger.info(
            "Executing %d SQL statement(s) against Athena workgroup '%s', database '%s'",
            len(statements),
            self.workgroup,
            self.database,
        )

        all_results = []
        for i, statement in enumerate(statements, 1):
            logger.info("--- Statement %d/%d ---", i, len(statements))
            results = self._execute_with_retry(statement)
            if results:
                all_results.extend(results)

        if self.push_results and all_results:
            context["ti"].xcom_push(key="query_results", value=all_results[:1000])

        return all_results if self.push_results else None


# Backward-compatible alias
TrinoOperator = AthenaQueryOperator
