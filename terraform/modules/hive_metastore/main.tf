###############################################################################
# DEPRECATED - Hive Metastore Module
# Replaced by AWS Glue Data Catalog (managed in terraform/modules/trino/main.tf)
#
# The self-hosted Hive Metastore (RDS MySQL + ECS Fargate Thrift Server) has
# been replaced by the AWS Glue Data Catalog, which is configured as part of
# the Athena module (terraform/modules/trino/).
#
# Glue Data Catalog databases:
#   - zomato_raw      : Raw ingestion layer
#   - zomato_curated  : Cleaned, deduped, enriched data
#   - zomato_gold     : Pre-aggregated metrics and KPIs
#
# This file is intentionally emptied. Remove this module from your
# environment configurations (dev/staging/prod main.tf).
###############################################################################
