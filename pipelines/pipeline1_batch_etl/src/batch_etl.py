"""
Pipeline 1 - Batch ETL: Aurora MySQL → Sqoop → S3 (ORC)

Orchestrates Apache Sqoop imports from Aurora MySQL to S3 in ORC format.
This is the traditional batch ETL path for historical data loading.

Tables imported: orders, users, restaurants, menu_items, payments, promotions
Schedule: Runs every 6 hours via Airflow
"""

import json
import logging
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline1_batch_etl")


@dataclass
class SqoopConfig:
    """Configuration for a Sqoop import job."""

    table: str
    split_by: str
    num_mappers: int = 8
    incremental_column: str = "updated_at"
    check_column: str = "updated_at"
    merge_key: str = "id"


# Tables to import from Aurora MySQL
SQOOP_TABLES = [
    SqoopConfig(table="orders", split_by="order_id", num_mappers=16),
    SqoopConfig(table="users", split_by="user_id", num_mappers=8),
    SqoopConfig(table="restaurants", split_by="restaurant_id", num_mappers=4),
    SqoopConfig(table="menu_items", split_by="item_id", num_mappers=8),
    SqoopConfig(table="payments", split_by="payment_id", num_mappers=12),
    SqoopConfig(table="promotions", split_by="promo_id", num_mappers=4),
]


def build_sqoop_command(
    config: SqoopConfig,
    jdbc_url: str,
    s3_target: str,
    last_value: str | None = None,
) -> list[str]:
    """Build a Sqoop import command for the given table configuration."""
    partition = datetime.utcnow().strftime("%Y/%m/%d/%H")
    target_dir = f"{s3_target}/pipeline1-batch-etl/sqoop-output/{config.table}/{partition}"

    cmd = [
        "sqoop", "import",
        "--connect", jdbc_url,
        "--table", config.table,
        "--target-dir", target_dir,
        "--as-orcfile",
        "--compress",
        "--compression-codec", "org.apache.hadoop.io.compress.SnappyCodec",
        "--num-mappers", str(config.num_mappers),
        "--split-by", config.split_by,
        "--hive-import",
        "--hive-overwrite",
        "--hive-database", "zomato_raw",
        "--hive-table", config.table,
        "--null-string", "\\\\N",
        "--null-non-string", "\\\\N",
        "--map-column-java", "created_at=String,updated_at=String",
    ]

    if last_value:
        cmd.extend([
            "--incremental", "lastmodified",
            "--check-column", config.check_column,
            "--last-value", last_value,
            "--merge-key", config.merge_key,
        ])

    return cmd


def run_sqoop_import(
    config: SqoopConfig,
    jdbc_url: str,
    s3_target: str,
    last_value: str | None = None,
) -> bool:
    """Execute a Sqoop import for a single table."""
    cmd = build_sqoop_command(config, jdbc_url, s3_target, last_value)
    logger.info("Starting Sqoop import for table: %s", config.table)
    logger.info("Command: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,
            check=False,
        )

        if result.returncode == 0:
            logger.info("Sqoop import completed successfully for table: %s", config.table)
            return True

        logger.error(
            "Sqoop import failed for table %s: %s", config.table, result.stderr
        )
        return False

    except subprocess.TimeoutExpired:
        logger.error("Sqoop import timed out for table: %s", config.table)
        return False
    except FileNotFoundError:
        logger.error("Sqoop binary not found. Ensure Sqoop is installed.")
        return False


def get_last_imported_value(table: str, state_file: str) -> str | None:
    """Get the last imported value for incremental imports."""
    state_path = Path(state_file)
    if not state_path.exists():
        return None

    with open(state_path) as f:
        state = json.load(f)

    return state.get(table, {}).get("last_value")


def save_import_state(table: str, last_value: str, state_file: str) -> None:
    """Save the last imported value for a table."""
    state_path = Path(state_file)
    state = {}
    if state_path.exists():
        with open(state_path) as f:
            state = json.load(f)

    state[table] = {
        "last_value": last_value,
        "imported_at": datetime.utcnow().isoformat(),
    }

    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w") as f:
        json.dump(state, f, indent=2)


def convert_to_orc_with_hive(table: str, s3_raw_path: str, s3_orc_path: str) -> bool:
    """Convert raw Sqoop output to optimized ORC with Hive."""
    partition = datetime.utcnow().strftime("%Y/%m/%d/%H")

    hive_query = f"""
    INSERT OVERWRITE TABLE zomato_processed.{table}
    PARTITION (dt='{datetime.utcnow().strftime("%Y-%m-%d")}')
    SELECT * FROM zomato_raw.{table}
    WHERE dt='{datetime.utcnow().strftime("%Y-%m-%d")}';
    """

    cmd = ["hive", "-e", hive_query]
    logger.info("Converting %s to optimized ORC format", table)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800, check=False)
        if result.returncode == 0:
            logger.info("ORC conversion completed for table: %s", table)
            return True
        logger.error("ORC conversion failed for %s: %s", table, result.stderr)
        return False
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.error("ORC conversion error for %s: %s", table, e)
        return False


def run_batch_etl(
    jdbc_url: str,
    s3_bucket: str,
    state_file: str = "/tmp/sqoop_state.json",
    tables: list[SqoopConfig] | None = None,
) -> dict:
    """Run the full batch ETL pipeline for all configured tables."""
    s3_target = f"s3://{s3_bucket}"
    results = {"success": [], "failed": [], "start_time": datetime.utcnow().isoformat()}

    for config in tables or SQOOP_TABLES:
        last_value = get_last_imported_value(config.table, state_file)
        success = run_sqoop_import(config, jdbc_url, s3_target, last_value)

        if success:
            new_last_value = datetime.utcnow().isoformat()
            save_import_state(config.table, new_last_value, state_file)
            results["success"].append(config.table)

            # Convert to optimized ORC
            convert_to_orc_with_hive(
                config.table,
                f"{s3_target}/pipeline1-batch-etl/sqoop-output/{config.table}",
                f"{s3_target}/pipeline1-batch-etl/orc/{config.table}",
            )
        else:
            results["failed"].append(config.table)

    results["end_time"] = datetime.utcnow().isoformat()
    logger.info("Batch ETL results: %s", json.dumps(results, indent=2))
    return results


if __name__ == "__main__":
    import os

    jdbc = os.environ.get(
        "AURORA_JDBC_URL",
        "jdbc:mysql://aurora-cluster.cluster-xxxxx.us-east-1.rds.amazonaws.com:3306/zomato",
    )
    bucket = os.environ.get("S3_BUCKET", "zomato-data-platform-dev-raw-data-lake")

    results = run_batch_etl(jdbc, bucket)

    if results["failed"]:
        logger.error("Some tables failed: %s", results["failed"])
        sys.exit(1)
    logger.info("All tables imported successfully")
