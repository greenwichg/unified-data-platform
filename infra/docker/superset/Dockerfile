# =============================================================================
# Zomato Data Platform - Apache Superset (Local Development)
# Superset with Athena, Trino (local dev), and Druid drivers for data lake visualization
#
# PRODUCTION NOTE: In production, Amazon Athena (serverless) replaces Trino.
# Superset connects to Athena via the PyAthena SQLAlchemy driver.
# =============================================================================
FROM apache/superset:3.1.0

LABEL maintainer="data-engineering@zomato.com"
LABEL description="Apache Superset with Athena, Trino (local dev), and Druid SQLAlchemy drivers"

USER root

# Install database drivers for Athena (production), Trino (local dev), and Druid
# NOTE: In production, use PyAthena for Athena connectivity.
# Trino drivers are retained for local development only.
RUN pip install --no-cache-dir \
    pyathena[sqlalchemy]>=3.0.0 \
    trino[sqlalchemy]==0.327.0 \
    pydruid==0.6.6 \
    sqlalchemy-trino==0.6.1 \
    redis==5.0.1

# Superset configuration
ENV SUPERSET_SECRET_KEY="zomato-superset-secret-key-change-in-prod" \
    SUPERSET_CONFIG_PATH="/app/superset_config.py" \
    FLASK_APP="superset"

# Write Superset config for local development
RUN cat > /app/superset_config.py <<'PYEOF'
import os

# Basic configuration
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "zomato-superset-secret-key-change-in-prod")
SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_DB_URI", "sqlite:////app/superset.db")

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ALERT_REPORTS": True,
}

# Cache configuration (use Redis in production)
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# Database connection defaults
# Production uses Athena; Trino is for local dev only
PREFERRED_DATABASES = ["athena", "trino", "druid"]

# Row limit for queries
ROW_LIMIT = 50000
SQL_MAX_ROW = 100000

# Timeout for SQL queries (seconds)
SQLLAB_TIMEOUT = 300

# Default timezone
BABEL_DEFAULT_LOCALE = "en"
BABEL_DEFAULT_TIMEZONE = "Asia/Kolkata"
PYEOF

USER superset

EXPOSE 8088

HEALTHCHECK --interval=15s --timeout=10s --retries=10 --start-period=60s \
    CMD curl -f http://localhost:8088/health || exit 1
