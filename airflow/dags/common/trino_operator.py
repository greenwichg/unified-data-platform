"""
Custom Airflow Operator for executing SQL against a Trino cluster.

Wraps the trino-python-client to provide:
  - Parameterized Trino host/port/catalog/schema
  - Automatic retry with exponential backoff on transient failures
  - Query result logging and optional XCom push
  - Support for multi-statement SQL (semicolon-separated)
"""

import time
import logging
from typing import Any, Optional, Sequence

import trino
from trino.exceptions import TrinoExternalError, TrinoInternalError

from airflow.models import BaseOperator
from airflow.utils.context import Context

logger = logging.getLogger(__name__)

# Trino error codes that are safe to retry
RETRYABLE_ERROR_CODES = {
    "QUERY_QUEUE_FULL",
    "CLUSTER_OUT_OF_MEMORY",
    "EXCEEDED_TIME_LIMIT",
    "SERVER_STARTING_UP",
    "GENERIC_INTERNAL_ERROR",
}


class TrinoOperator(BaseOperator):
    """
    Execute SQL statements against a specified Trino cluster.

    :param sql: SQL statement(s) to execute. Multiple statements separated by semicolons.
    :param trino_host: Trino coordinator hostname.
    :param trino_port: Trino coordinator port (default 8080).
    :param catalog: Trino catalog to use (e.g., 'iceberg', 'mysql').
    :param schema: Trino schema to use (e.g., 'zomato').
    :param user: Trino user for the session (default 'airflow').
    :param source: Source identifier for Trino query tracking.
    :param query_timeout: Maximum query execution time in seconds (default 3600).
    :param max_retries: Number of retries on transient failures (default 3).
    :param retry_backoff_seconds: Initial backoff between retries in seconds (default 30).
    :param push_results: Whether to push query results to XCom (default False).
    """

    template_fields: Sequence[str] = ("sql", "trino_host", "catalog", "schema")
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#e8d0ef"

    def __init__(
        self,
        sql: str,
        trino_host: str,
        trino_port: int = 8080,
        catalog: str = "iceberg",
        schema: str = "zomato",
        user: str = "airflow",
        source: str = "airflow-trino-operator",
        query_timeout: int = 3600,
        max_retries: int = 3,
        retry_backoff_seconds: int = 30,
        push_results: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.trino_host = trino_host
        self.trino_port = trino_port
        self.catalog = catalog
        self.schema = schema
        self.user = user
        self.source = source
        self.query_timeout = query_timeout
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.push_results = push_results

    def _get_connection(self) -> trino.dbapi.Connection:
        """Create a new Trino connection."""
        return trino.dbapi.connect(
            host=self.trino_host,
            port=self.trino_port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            source=self.source,
            http_scheme="https" if self.trino_port == 443 else "http",
            request_timeout=self.query_timeout,
        )

    def _execute_with_retry(self, sql_statement: str) -> Optional[list]:
        """Execute a single SQL statement with retry logic."""
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                logger.info(
                    "Executing on %s:%s (attempt %d/%d):\n%s",
                    self.trino_host,
                    self.trino_port,
                    attempt,
                    self.max_retries,
                    sql_statement.strip()[:500],
                )

                cursor.execute(sql_statement)
                results = cursor.fetchall()

                row_count = len(results) if results else 0
                logger.info(
                    "Query completed successfully. Rows returned: %d", row_count
                )

                cursor.close()
                conn.close()
                return results

            except (TrinoExternalError, TrinoInternalError) as e:
                last_exception = e
                error_name = getattr(e, "error_name", "UNKNOWN")
                if error_name in RETRYABLE_ERROR_CODES and attempt < self.max_retries:
                    backoff = self.retry_backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "Retryable Trino error '%s' on attempt %d/%d. "
                        "Retrying in %ds: %s",
                        error_name,
                        attempt,
                        self.max_retries,
                        backoff,
                        str(e),
                    )
                    time.sleep(backoff)
                else:
                    raise
            except Exception as e:
                logger.error("Non-retryable error executing Trino query: %s", str(e))
                raise

        raise last_exception

    def execute(self, context: Context) -> Optional[list]:
        """Execute SQL against Trino, handling multi-statement SQL."""
        statements = [
            stmt.strip()
            for stmt in self.sql.split(";")
            if stmt.strip()
        ]

        logger.info(
            "Executing %d SQL statement(s) against %s:%s/%s.%s",
            len(statements),
            self.trino_host,
            self.trino_port,
            self.catalog,
            self.schema,
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
