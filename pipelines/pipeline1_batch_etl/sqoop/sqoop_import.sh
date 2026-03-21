#!/usr/bin/env bash
#
# Pipeline 1 - Parameterized Sqoop Import
# Imports tables from Aurora MySQL to S3 in ORC format.
# Reads table configs from sqoop_tables.conf (YAML).
# Supports incremental (lastmodified) mode with state tracking.
#
# Usage:
#   ./sqoop_import.sh [--table <table_name>] [--full-load]
#
# Environment variables:
#   AURORA_JDBC_URL    - JDBC connection string for Aurora MySQL
#   DB_USER            - Database username
#   DB_PASSWORD_FILE   - Path to file containing database password
#   S3_BUCKET          - Target S3 bucket for ORC output
#   STATE_DIR          - Directory for incremental import state (default: /var/lib/sqoop/state)
#   SQOOP_CONF         - Path to sqoop_tables.conf (default: same directory as this script)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_PREFIX="[sqoop-import]"

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
AURORA_JDBC_URL="${AURORA_JDBC_URL:-jdbc:mysql://aurora-cluster.cluster-xxxxx.us-east-1.rds.amazonaws.com:3306/zomato}"
DB_USER="${DB_USER:-sqoop_user}"
DB_PASSWORD_FILE="${DB_PASSWORD_FILE:-/run/secrets/aurora_password}"
S3_BUCKET="${S3_BUCKET:-zomato-data-platform-prod-raw-data-lake}"
STATE_DIR="${STATE_DIR:-/var/lib/sqoop/state}"
SQOOP_CONF="${SQOOP_CONF:-${SCRIPT_DIR}/sqoop_tables.conf}"
HIVE_DATABASE="zomato_raw"
WAREHOUSE_DIR="s3://${S3_BUCKET}/pipeline1-batch-etl/sqoop-output"

# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------
TARGET_TABLE=""
FULL_LOAD=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --table)
            TARGET_TABLE="$2"
            shift 2
            ;;
        --full-load)
            FULL_LOAD=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--table <table_name>] [--full-load]"
            exit 0
            ;;
        *)
            echo "${LOG_PREFIX} Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log_info()  { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') ${LOG_PREFIX} INFO  $*"; }
log_error() { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') ${LOG_PREFIX} ERROR $*" >&2; }
log_warn()  { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') ${LOG_PREFIX} WARN  $*" >&2; }

read_password() {
    if [[ -f "${DB_PASSWORD_FILE}" ]]; then
        cat "${DB_PASSWORD_FILE}"
    elif [[ -n "${DB_PASSWORD:-}" ]]; then
        echo "${DB_PASSWORD}"
    else
        log_error "No database password found. Set DB_PASSWORD_FILE or DB_PASSWORD."
        exit 1
    fi
}

# Parse a single field from the YAML config for a given table.
# This is a lightweight parser sufficient for the flat sqoop_tables.conf structure.
parse_yaml_field() {
    local file="$1"
    local table="$2"
    local field="$3"
    local default="${4:-}"

    # Extract the block for the requested table, then pull the field value.
    local value
    value=$(awk -v tbl="${table}" -v fld="${field}" '
        /^[[:space:]]*- table:/ {
            # Check if this is the table block we want
            if ($0 ~ "table:[[:space:]]*" tbl "$") { found=1 } else { found=0 }
            next
        }
        found && $0 ~ fld ":" {
            sub(/^[^:]+:[[:space:]]*/, "")
            gsub(/[[:space:]]*$/, "")
            gsub(/"/, "")
            print
            found=0
        }
    ' "${file}")

    if [[ -n "${value}" ]]; then
        echo "${value}"
    else
        echo "${default}"
    fi
}

# List all table names from the config
list_tables() {
    local file="$1"
    grep -E '^\s*- table:' "${file}" | sed 's/.*table:[[:space:]]*//' | tr -d '"' | tr -d "'"
}

# Get or create the state file for incremental imports
get_last_value() {
    local table="$1"
    local state_file="${STATE_DIR}/${table}.last_value"
    if [[ -f "${state_file}" ]]; then
        cat "${state_file}"
    fi
}

save_last_value() {
    local table="$1"
    local value="$2"
    mkdir -p "${STATE_DIR}"
    echo "${value}" > "${STATE_DIR}/${table}.last_value"
    log_info "Saved last value for ${table}: ${value}"
}

# ---------------------------------------------------------------------------
# Sqoop import for a single table
# ---------------------------------------------------------------------------
run_sqoop_import() {
    local table="$1"
    local split_by
    local num_mappers
    local incremental_column
    local password

    split_by=$(parse_yaml_field "${SQOOP_CONF}" "${table}" "split_by" "id")
    num_mappers=$(parse_yaml_field "${SQOOP_CONF}" "${table}" "num_mappers" "8")
    incremental_column=$(parse_yaml_field "${SQOOP_CONF}" "${table}" "incremental_column" "updated_at")
    password=$(read_password)

    local partition
    partition="$(date -u '+%Y/%m/%d/%H')"
    local target_dir="${WAREHOUSE_DIR}/${table}/${partition}"

    log_info "Importing table=${table} split_by=${split_by} mappers=${num_mappers} target=${target_dir}"

    # Build the base Sqoop command
    local -a cmd=(
        sqoop import
        --connect "${AURORA_JDBC_URL}"
        --username "${DB_USER}"
        --password-file "file://${DB_PASSWORD_FILE}"
        --table "${table}"
        --target-dir "${target_dir}"
        --as-orcfile
        --compress
        --compression-codec org.apache.hadoop.io.compress.SnappyCodec
        --num-mappers "${num_mappers}"
        --split-by "${split_by}"
        --hive-import
        --hive-database "${HIVE_DATABASE}"
        --hive-table "${table}"
        --null-string '\\N'
        --null-non-string '\\N'
        --map-column-java "created_at=String,updated_at=String"
        --fetch-size 10000
        --direct
    )

    # Incremental mode unless --full-load is specified
    if [[ "${FULL_LOAD}" == "false" ]]; then
        local last_value
        last_value=$(get_last_value "${table}")

        if [[ -n "${last_value}" ]]; then
            log_info "Incremental import for ${table} since ${last_value}"
            cmd+=(
                --incremental lastmodified
                --check-column "${incremental_column}"
                --last-value "${last_value}"
                --merge-key "${split_by}"
            )
        else
            log_info "No previous state for ${table}; performing initial full load"
        fi
    else
        log_info "Full load requested for ${table}"
        cmd+=(--hive-overwrite)
    fi

    # Execute
    local start_ts
    start_ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

    if "${cmd[@]}"; then
        local end_ts
        end_ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
        log_info "Successfully imported ${table} (started=${start_ts} ended=${end_ts})"
        save_last_value "${table}" "${start_ts}"
        return 0
    else
        local rc=$?
        log_error "Sqoop import failed for ${table} (exit code ${rc})"
        return ${rc}
    fi
}

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
if [[ ! -f "${SQOOP_CONF}" ]]; then
    log_error "Config file not found: ${SQOOP_CONF}"
    exit 1
fi

if ! command -v sqoop &>/dev/null; then
    log_error "sqoop binary not found in PATH"
    exit 1
fi

# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
FAILED_TABLES=()
SUCCESS_TABLES=()

if [[ -n "${TARGET_TABLE}" ]]; then
    TABLES=("${TARGET_TABLE}")
else
    mapfile -t TABLES < <(list_tables "${SQOOP_CONF}")
fi

log_info "Starting Sqoop imports for ${#TABLES[@]} table(s): ${TABLES[*]}"

for table in "${TABLES[@]}"; do
    if run_sqoop_import "${table}"; then
        SUCCESS_TABLES+=("${table}")
    else
        FAILED_TABLES+=("${table}")
    fi
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_info "===== Import Summary ====="
log_info "Successful: ${SUCCESS_TABLES[*]:-none}"
if [[ ${#FAILED_TABLES[@]} -gt 0 ]]; then
    log_error "Failed: ${FAILED_TABLES[*]}"
    exit 1
fi

log_info "All imports completed successfully"
exit 0
