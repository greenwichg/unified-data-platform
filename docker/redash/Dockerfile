# =============================================================================
# Zomato Data Platform - Redash (Local Development)
# Redash for ad-hoc SQL queries against Trino and Druid
# =============================================================================
FROM redash/redash:10.1.0.b50633

LABEL maintainer="data-engineering@zomato.com"
LABEL description="Redash with Trino and Druid query runners"

USER root

# Install Trino and Druid query runner dependencies
RUN pip install --no-cache-dir \
    trino[sqlalchemy]==0.327.0 \
    pydruid==0.6.6 \
    pyhive[trino]==0.7.0

# Environment configuration
ENV REDASH_LOG_LEVEL=INFO \
    REDASH_REDIS_URL=redis://redis:6379/0 \
    REDASH_DATABASE_URL="postgresql://redash:redash@postgres:5432/redash" \
    REDASH_COOKIE_SECRET="zomato-redash-cookie-secret-change-in-prod" \
    REDASH_SECRET_KEY="zomato-redash-secret-key-change-in-prod" \
    REDASH_WEB_WORKERS=2 \
    REDASH_MAIL_SERVER="localhost" \
    REDASH_MAIL_PORT=25 \
    REDASH_MAIL_DEFAULT_SENDER="redash@zomato.com" \
    REDASH_ADDITIONAL_QUERY_RUNNERS="redash.query_runner.trino,redash.query_runner.druid" \
    PYTHONUNBUFFERED=0

USER redash

EXPOSE 5000

HEALTHCHECK --interval=15s --timeout=10s --retries=10 --start-period=30s \
    CMD curl -f http://localhost:5000/ping || exit 1
