"""
Database Schema Migration Utility for the Zomato Data Platform.

Manages schema migrations across:
  - Aurora MySQL (source database)
  - AWS Glue Data Catalog (table definitions, replaces Hive Metastore)
  - Iceberg tables (via Athena; Trino retained for local dev)

MIGRATION NOTE: Hive Metastore has been replaced by AWS Glue Data Catalog
in production. Trino has been replaced by Amazon Athena (serverless).
The 'trino' target is kept for local development; in production, use
the 'athena' target which executes DDL via Athena/Glue.

Features:
  - Sequential migration numbering
  - Rollback support
  - Dry-run mode
  - Migration state tracking in a dedicated table
  - Pre/post migration validation

Usage:
    python schema_migration.py migrate --target aurora --env dev
    python schema_migration.py migrate --target athena --env prod
    python schema_migration.py rollback --target aurora --version 003
    python schema_migration.py status --target aurora
"""

import argparse
import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("schema_migration")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class Migration:
    """Represents a single schema migration."""

    version: str
    description: str
    up_sql: str
    down_sql: str
    checksum: str
    target: str

    @property
    def filename(self) -> str:
        return f"V{self.version}__{self.description.replace(' ', '_')}.sql"


@dataclass
class MigrationRecord:
    """Record of an applied migration."""

    version: str
    description: str
    checksum: str
    applied_at: str
    execution_time_ms: int
    success: bool


# ---------------------------------------------------------------------------
# Migration loader
# ---------------------------------------------------------------------------
MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "scripts" / "migrations"


def discover_migrations(target: str, migrations_dir: Path | None = None) -> list[Migration]:
    """Discover migration files for the given target.

    Migration files follow the naming convention:
        V{version}__{description}.sql

    Each file contains an UP section and an optional DOWN section separated by
    a ``-- DOWN`` marker.
    """
    base_dir = migrations_dir or MIGRATIONS_DIR / target
    if not base_dir.exists():
        logger.warning("Migrations directory not found: %s", base_dir)
        return []

    migrations = []
    for sql_file in sorted(base_dir.glob("V*__*.sql")):
        content = sql_file.read_text()

        # Split on -- DOWN marker
        if "-- DOWN" in content:
            parts = content.split("-- DOWN", 1)
            up_sql = parts[0].strip()
            down_sql = parts[1].strip()
        else:
            up_sql = content.strip()
            down_sql = ""

        # Extract version and description from filename
        stem = sql_file.stem  # e.g., V001__create_orders_table
        version = stem.split("__")[0].lstrip("V")
        description = stem.split("__")[1].replace("_", " ") if "__" in stem else stem

        checksum = hashlib.md5(content.encode()).hexdigest()

        migrations.append(
            Migration(
                version=version,
                description=description,
                up_sql=up_sql,
                down_sql=down_sql,
                checksum=checksum,
                target=target,
            )
        )

    return sorted(migrations, key=lambda m: m.version)


# ---------------------------------------------------------------------------
# Migration state management
# ---------------------------------------------------------------------------
class MigrationStateStore:
    """Tracks which migrations have been applied.

    Uses a JSON file for portability. In production, this would be a table
    in the target database itself.
    """

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self._state: dict = self._load()

    def _load(self) -> dict:
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return {"applied": [], "target": "", "last_updated": ""}

    def _save(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state["last_updated"] = datetime.utcnow().isoformat()
        with open(self.state_file, "w") as f:
            json.dump(self._state, f, indent=2)

    def get_applied_versions(self) -> set[str]:
        return {r["version"] for r in self._state.get("applied", [])}

    def get_latest_version(self) -> str | None:
        applied = self._state.get("applied", [])
        if not applied:
            return None
        return max(r["version"] for r in applied)

    def record_migration(self, record: MigrationRecord) -> None:
        self._state.setdefault("applied", []).append({
            "version": record.version,
            "description": record.description,
            "checksum": record.checksum,
            "applied_at": record.applied_at,
            "execution_time_ms": record.execution_time_ms,
            "success": record.success,
        })
        self._save()

    def remove_migration(self, version: str) -> None:
        self._state["applied"] = [
            r for r in self._state.get("applied", []) if r["version"] != version
        ]
        self._save()

    def get_all_records(self) -> list[dict]:
        return self._state.get("applied", [])


# ---------------------------------------------------------------------------
# Migration executor
# ---------------------------------------------------------------------------
class MigrationExecutor:
    """Executes migrations against a target database."""

    def __init__(self, target: str, env: str, dry_run: bool = False):
        self.target = target
        self.env = env
        self.dry_run = dry_run
        self.state_dir = Path(os.environ.get(
            "MIGRATION_STATE_DIR",
            f"/tmp/zomato_migrations/{env}",
        ))
        self.state_store = MigrationStateStore(
            str(self.state_dir / f"{target}_state.json")
        )

    def _get_connection(self):
        """Get a database connection for the target."""
        if self.target == "aurora":
            import pymysql

            return pymysql.connect(
                host=os.environ.get("AURORA_HOST", "localhost"),
                port=int(os.environ.get("AURORA_PORT", "3306")),
                user=os.environ.get("DB_USER", "zomato_app"),
                password=os.environ.get("DB_PASSWORD", ""),
                database=os.environ.get("DB_NAME", "zomato"),
                autocommit=False,
            )
        elif self.target == "athena":
            import pyathena

            return pyathena.connect(
                region_name=os.environ.get("AWS_REGION", "us-east-1"),
                work_group=os.environ.get("ATHENA_WORKGROUP", "zomato-etl"),
                s3_staging_dir=os.environ.get(
                    "ATHENA_OUTPUT_LOCATION",
                    "s3://zomato-data-platform-athena-results/",
                ),
                catalog_name="AwsDataCatalog",
                schema_name="zomato",
            )
        elif self.target == "trino":
            # NOTE: Trino target is for local development only.
            # In production, use the 'athena' target instead.
            import trino

            return trino.dbapi.connect(
                host=os.environ.get("TRINO_HOST", "localhost"),
                port=int(os.environ.get("TRINO_PORT", "8080")),
                user=os.environ.get("TRINO_USER", "migration"),
                catalog="iceberg",
                schema="zomato",
            )
        else:
            raise ValueError(f"Unknown target: {self.target}")

    def _execute_sql(self, sql: str) -> None:
        """Execute SQL against the target database."""
        if self.dry_run:
            logger.info("[DRY RUN] Would execute:\n%s", sql[:500])
            return

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            # Split on semicolons for multi-statement migrations
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    logger.debug("Executing: %s", statement[:200])
                    cursor.execute(statement)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def migrate(self, migrations_dir: Path | None = None) -> list[MigrationRecord]:
        """Apply all pending migrations."""
        migrations = discover_migrations(self.target, migrations_dir)
        applied = self.state_store.get_applied_versions()

        pending = [m for m in migrations if m.version not in applied]

        if not pending:
            logger.info("No pending migrations for %s", self.target)
            return []

        logger.info(
            "Found %d pending migration(s) for %s: %s",
            len(pending),
            self.target,
            [m.version for m in pending],
        )

        records = []
        for migration in pending:
            logger.info(
                "Applying migration V%s: %s",
                migration.version,
                migration.description,
            )

            start = datetime.utcnow()
            try:
                self._execute_sql(migration.up_sql)
                elapsed = int((datetime.utcnow() - start).total_seconds() * 1000)

                record = MigrationRecord(
                    version=migration.version,
                    description=migration.description,
                    checksum=migration.checksum,
                    applied_at=datetime.utcnow().isoformat(),
                    execution_time_ms=elapsed,
                    success=True,
                )
                self.state_store.record_migration(record)
                records.append(record)
                logger.info(
                    "Migration V%s applied successfully (%dms)",
                    migration.version,
                    elapsed,
                )

            except Exception as e:
                elapsed = int((datetime.utcnow() - start).total_seconds() * 1000)
                record = MigrationRecord(
                    version=migration.version,
                    description=migration.description,
                    checksum=migration.checksum,
                    applied_at=datetime.utcnow().isoformat(),
                    execution_time_ms=elapsed,
                    success=False,
                )
                records.append(record)
                logger.error("Migration V%s FAILED: %s", migration.version, e)
                raise

        return records

    def rollback(self, target_version: str, migrations_dir: Path | None = None) -> bool:
        """Rollback to the specified version (exclusive)."""
        migrations = discover_migrations(self.target, migrations_dir)
        applied = self.state_store.get_applied_versions()

        to_rollback = sorted(
            [m for m in migrations if m.version in applied and m.version > target_version],
            key=lambda m: m.version,
            reverse=True,
        )

        if not to_rollback:
            logger.info("Nothing to rollback")
            return True

        logger.info(
            "Rolling back %d migration(s): %s",
            len(to_rollback),
            [m.version for m in to_rollback],
        )

        for migration in to_rollback:
            if not migration.down_sql:
                logger.error(
                    "Migration V%s has no rollback SQL. Aborting.",
                    migration.version,
                )
                return False

            logger.info(
                "Rolling back V%s: %s",
                migration.version,
                migration.description,
            )

            try:
                self._execute_sql(migration.down_sql)
                self.state_store.remove_migration(migration.version)
                logger.info("Rollback of V%s successful", migration.version)
            except Exception as e:
                logger.error("Rollback of V%s FAILED: %s", migration.version, e)
                return False

        return True

    def status(self) -> None:
        """Print the current migration status."""
        records = self.state_store.get_all_records()
        latest = self.state_store.get_latest_version()

        print(f"\nMigration Status: {self.target} ({self.env})")
        print(f"Latest version: {latest or 'none'}")
        print(f"Applied migrations: {len(records)}")
        print("-" * 70)

        if records:
            print(f"{'Version':<10} {'Description':<30} {'Applied At':<25} {'Time':<8} {'Status'}")
            print("-" * 70)
            for r in sorted(records, key=lambda x: x["version"]):
                status = "OK" if r["success"] else "FAILED"
                print(
                    f"{r['version']:<10} "
                    f"{r['description'][:28]:<30} "
                    f"{r['applied_at'][:23]:<25} "
                    f"{r['execution_time_ms']}ms{'':<4} "
                    f"{status}"
                )
        print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Zomato Data Platform Schema Migration Utility"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # migrate
    migrate_parser = subparsers.add_parser("migrate", help="Apply pending migrations")
    migrate_parser.add_argument(
        "--target",
        required=True,
        choices=["aurora", "athena", "trino"],
        help="Migration target",
    )
    migrate_parser.add_argument("--env", default="dev", help="Environment")
    migrate_parser.add_argument("--dry-run", action="store_true", help="Dry run mode")

    # rollback
    rollback_parser = subparsers.add_parser("rollback", help="Rollback migrations")
    rollback_parser.add_argument("--target", required=True, choices=["aurora", "trino"])
    rollback_parser.add_argument(
        "--version", required=True, help="Target version to rollback to"
    )
    rollback_parser.add_argument("--env", default="dev")
    rollback_parser.add_argument("--dry-run", action="store_true")

    # status
    status_parser = subparsers.add_parser("status", help="Show migration status")
    status_parser.add_argument("--target", required=True, choices=["aurora", "trino"])
    status_parser.add_argument("--env", default="dev")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    executor = MigrationExecutor(
        target=args.target,
        env=args.env,
        dry_run=getattr(args, "dry_run", False),
    )

    if args.command == "migrate":
        records = executor.migrate()
        failed = [r for r in records if not r.success]
        if failed:
            logger.error("Migration failed. %d error(s).", len(failed))
            sys.exit(1)
        logger.info("All migrations applied successfully.")

    elif args.command == "rollback":
        success = executor.rollback(args.version)
        if not success:
            sys.exit(1)

    elif args.command == "status":
        executor.status()


if __name__ == "__main__":
    main()
