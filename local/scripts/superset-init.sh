#!/bin/bash
###############################################################################
# Superset Initialization Script (local dev only)
#
# Runs once as superset-init container before superset-webserver starts.
# 1. Initialises the Superset metadata database
# 2. Creates the admin user (admin / admin)
# 3. Loads default roles and permissions
# 4. Pre-configures database connections to Trino, Druid, and MySQL
###############################################################################

set -euo pipefail

echo "==> Initialising Superset database..."
superset db upgrade

echo "==> Creating admin user..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@zomato.com \
  --password admin 2>/dev/null || echo "Admin user already exists, skipping."

echo "==> Loading default roles and permissions..."
superset init

echo "==> Configuring database connections..."
python3 - <<'PYTHON'
import sys

try:
    from superset import create_app
    from superset.extensions import db
    from superset.models.core import Database

    app = create_app()
    with app.app_context():
        connections = [
            (
                "Trino (Local)",
                # Local Trino — mirrors Amazon Athena in production
                "trino://trino:8080/iceberg",
            ),
            (
                "Druid (Local)",
                # Local Druid router — mirrors Apache Druid on EC2 R8g in production
                "druid://druid-router:8888/druid/v2/sql/",
            ),
            (
                "MySQL (Local)",
                # Local MySQL — mirrors Amazon Aurora MySQL in production
                "mysql+pymysql://root:rootpass@mysql:3306/zomato",
            ),
        ]

        added = 0
        for name, uri in connections:
            existing = db.session.query(Database).filter_by(database_name=name).first()
            if not existing:
                database = Database(
                    database_name=name,
                    sqlalchemy_uri=uri,
                    expose_in_sqllab=True,
                    allow_run_async=True,
                    allow_ctas=False,
                    allow_cvas=False,
                    allow_dml=False,
                )
                db.session.add(database)
                added += 1
                print(f"  ✓ Added: {name} → {uri}")
            else:
                print(f"  · Already exists: {name}")

        if added:
            db.session.commit()
            print(f"\n{added} connection(s) added.")
        else:
            print("\nAll connections already configured.")

except Exception as e:
    print(f"WARNING: Could not configure database connections: {e}", file=sys.stderr)
    print("You can add them manually via Superset UI → Settings → Database Connections", file=sys.stderr)
    # Don't fail the init — Superset still works without pre-configured connections
PYTHON

echo ""
echo "==> Superset init complete."
echo "    Admin UI:  http://localhost:8088  (admin / admin)"
echo "    Trino:     pre-configured (mirrors Amazon Athena)"
echo "    Druid:     pre-configured (mirrors Druid on EC2)"
echo "    MySQL:     pre-configured (mirrors Aurora MySQL)"
