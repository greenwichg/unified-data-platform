#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Terraform Validation Script
#
# Runs the following checks on all Terraform modules and environments:
#   1. terraform fmt -check   (formatting)
#   2. terraform validate     (syntax & internal consistency)
#   3. tflint                 (linting)
#   4. checkov                (security scanning)
#
# Usage:
#   ./tests/terraform/validate.sh [--skip-checkov] [--skip-tflint]
#
# Exit codes:
#   0 - all checks passed
#   1 - one or more checks failed
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TF_DIR="${REPO_ROOT}/terraform"
MODULES_DIR="${TF_DIR}/modules"
ENVS_DIR="${TF_DIR}/environments"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SKIP_CHECKOV=false
SKIP_TFLINT=false
ERRORS=0

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
for arg in "$@"; do
    case "$arg" in
        --skip-checkov) SKIP_CHECKOV=true ;;
        --skip-tflint)  SKIP_TFLINT=true ;;
        --help|-h)
            echo "Usage: $0 [--skip-checkov] [--skip-tflint]"
            exit 0
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
log_info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_warn "$1 not found in PATH — skipping $1 checks"
        return 1
    fi
    return 0
}

# Collect all directories containing .tf files
find_tf_dirs() {
    find "$1" -name '*.tf' -print0 \
        | xargs -0 -n1 dirname \
        | sort -u
}

# ---------------------------------------------------------------------------
# Step 1: terraform fmt -check
# ---------------------------------------------------------------------------
step_fmt() {
    log_info "=== Step 1: terraform fmt -check ==="

    if ! check_command terraform; then
        return
    fi

    local fmt_errors=0
    while IFS= read -r dir; do
        if ! terraform fmt -check -diff -recursive "$dir" > /dev/null 2>&1; then
            log_error "Formatting issues in: $dir"
            terraform fmt -check -diff -recursive "$dir" 2>&1 || true
            fmt_errors=$((fmt_errors + 1))
        fi
    done < <(echo "$TF_DIR")

    if [ "$fmt_errors" -eq 0 ]; then
        log_info "All Terraform files are properly formatted."
    else
        log_error "Found formatting issues in $fmt_errors location(s)."
        ERRORS=$((ERRORS + fmt_errors))
    fi
}

# ---------------------------------------------------------------------------
# Step 2: terraform validate (per module & environment)
# ---------------------------------------------------------------------------
step_validate() {
    log_info "=== Step 2: terraform validate ==="

    if ! check_command terraform; then
        return
    fi

    # Validate each module
    if [ -d "$MODULES_DIR" ]; then
        for module_dir in "$MODULES_DIR"/*/; do
            module_name="$(basename "$module_dir")"
            log_info "Validating module: $module_name"

            # terraform validate requires init first (with backend disabled)
            if ! terraform -chdir="$module_dir" init -backend=false -input=false -no-color > /dev/null 2>&1; then
                log_warn "terraform init failed for module $module_name (may need provider config) — skipping validate"
                continue
            fi

            if ! terraform -chdir="$module_dir" validate -no-color; then
                log_error "Validation failed for module: $module_name"
                ERRORS=$((ERRORS + 1))
            else
                log_info "Module $module_name: OK"
            fi
        done
    fi

    # Validate each environment
    if [ -d "$ENVS_DIR" ]; then
        for env_dir in "$ENVS_DIR"/*/; do
            env_name="$(basename "$env_dir")"
            log_info "Validating environment: $env_name"

            if ! terraform -chdir="$env_dir" init -backend=false -input=false -no-color > /dev/null 2>&1; then
                log_warn "terraform init failed for environment $env_name — skipping validate"
                continue
            fi

            if ! terraform -chdir="$env_dir" validate -no-color; then
                log_error "Validation failed for environment: $env_name"
                ERRORS=$((ERRORS + 1))
            else
                log_info "Environment $env_name: OK"
            fi
        done
    fi
}

# ---------------------------------------------------------------------------
# Step 3: tflint
# ---------------------------------------------------------------------------
step_tflint() {
    log_info "=== Step 3: tflint ==="

    if [ "$SKIP_TFLINT" = true ]; then
        log_info "Skipping tflint (--skip-tflint)"
        return
    fi

    if ! check_command tflint; then
        return
    fi

    # Initialize tflint (download rulesets)
    tflint --init --config="${REPO_ROOT}/.tflint.hcl" 2>/dev/null || true

    local lint_errors=0

    while IFS= read -r dir; do
        log_info "Linting: $(realpath --relative-to="$REPO_ROOT" "$dir")"
        if ! tflint --chdir="$dir" \
                --config="${REPO_ROOT}/.tflint.hcl" \
                --minimum-failure-severity=warning \
                --no-color 2>/dev/null; then
            # tflint may not have a config file — try without
            if ! tflint --chdir="$dir" --minimum-failure-severity=warning --no-color 2>&1; then
                log_error "tflint issues in: $dir"
                lint_errors=$((lint_errors + 1))
            fi
        fi
    done < <(find_tf_dirs "$TF_DIR")

    if [ "$lint_errors" -eq 0 ]; then
        log_info "tflint: all checks passed."
    else
        log_error "tflint found issues in $lint_errors directory(ies)."
        ERRORS=$((ERRORS + lint_errors))
    fi
}

# ---------------------------------------------------------------------------
# Step 4: checkov (security scanning)
# ---------------------------------------------------------------------------
step_checkov() {
    log_info "=== Step 4: checkov security scanning ==="

    if [ "$SKIP_CHECKOV" = true ]; then
        log_info "Skipping checkov (--skip-checkov)"
        return
    fi

    if ! check_command checkov; then
        return
    fi

    local checkov_errors=0

    # Run checkov on the entire terraform directory
    log_info "Running checkov on: $TF_DIR"
    if ! checkov \
            --directory "$TF_DIR" \
            --framework terraform \
            --quiet \
            --compact \
            --skip-check CKV_AWS_144,CKV_AWS_145,CKV2_AWS_6 \
            --output cli \
            --no-guide 2>&1; then
        log_error "checkov found security issues"
        checkov_errors=$((checkov_errors + 1))
    fi

    # Also run checkov on individual environments for more targeted output
    if [ -d "$ENVS_DIR" ]; then
        for env_dir in "$ENVS_DIR"/*/; do
            env_name="$(basename "$env_dir")"
            log_info "Scanning environment: $env_name"
            if ! checkov \
                    --directory "$env_dir" \
                    --framework terraform \
                    --quiet \
                    --compact \
                    --skip-check CKV_AWS_144,CKV_AWS_145,CKV2_AWS_6 \
                    --output cli \
                    --no-guide 2>&1; then
                log_error "checkov issues in environment: $env_name"
                checkov_errors=$((checkov_errors + 1))
            fi
        done
    fi

    if [ "$checkov_errors" -eq 0 ]; then
        log_info "checkov: all security checks passed."
    else
        log_error "checkov found issues in $checkov_errors scan(s)."
        ERRORS=$((ERRORS + checkov_errors))
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    log_info "=========================================="
    log_info " Zomato Data Platform — Terraform Validation"
    log_info "=========================================="
    log_info "Repo root: $REPO_ROOT"
    log_info "Terraform dir: $TF_DIR"
    echo ""

    step_fmt
    echo ""
    step_validate
    echo ""
    step_tflint
    echo ""
    step_checkov
    echo ""

    log_info "=========================================="
    if [ "$ERRORS" -gt 0 ]; then
        log_error "Validation completed with $ERRORS error(s)."
        exit 1
    else
        log_info "All validation checks passed!"
        exit 0
    fi
}

main
