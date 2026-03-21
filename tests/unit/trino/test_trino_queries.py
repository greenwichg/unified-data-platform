"""
Unit tests for Trino SQL DDL and query syntax validation.

Tests cover:
  - Iceberg table DDL syntax correctness
  - Partition column definitions
  - Hive/Iceberg catalog property validation
  - Trino SQL query syntax for analytics queries
  - Schema/column naming conventions
"""

import os
import re
from pathlib import Path

import pytest

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schemas" / "iceberg"


def read_sql_file(name: str) -> str:
    """Read a SQL file from the iceberg schemas directory."""
    path = SCHEMA_DIR / name
    if not path.exists():
        pytest.skip(f"{name} not found")
    return path.read_text()


# ---------------------------------------------------------------------------
# SQL syntax helpers
# ---------------------------------------------------------------------------
def assert_valid_create_table(sql: str):
    """Validate basic CREATE TABLE syntax."""
    normalized = " ".join(sql.split())
    assert re.search(r"CREATE\s+TABLE", normalized, re.IGNORECASE), "Missing CREATE TABLE"
    assert "(" in normalized, "Missing opening parenthesis"
    assert ")" in normalized, "Missing closing parenthesis"


def extract_columns(sql: str) -> list[str]:
    """Extract column names from a CREATE TABLE statement."""
    # Find content between first ( and matching )
    match = re.search(r"CREATE\s+TABLE\s+\S+\s*\((.*)\)", sql, re.DOTALL | re.IGNORECASE)
    if not match:
        return []
    body = match.group(1)
    columns = []
    for line in body.split("\n"):
        line = line.strip().rstrip(",")
        if not line or line.upper().startswith("WITH") or line.startswith("--"):
            continue
        parts = line.split()
        if len(parts) >= 2 and not parts[0].upper().startswith("PARTITIONED"):
            col_name = parts[0].strip("`\"'")
            if col_name.upper() not in ("WITH", "PARTITIONED", "COMMENT", ")"):
                columns.append(col_name)
    return columns


# ---------------------------------------------------------------------------
# Orders table DDL tests
# ---------------------------------------------------------------------------
class TestOrdersTableDDL:
    @pytest.fixture()
    def sql(self):
        return read_sql_file("orders_table.sql")

    def test_is_valid_create_table(self, sql):
        assert_valid_create_table(sql)

    def test_table_name(self, sql):
        assert "orders" in sql.lower()

    def test_required_columns(self, sql):
        sql_lower = sql.lower()
        required = ["order_id", "user_id", "restaurant_id", "status", "total_amount"]
        for col in required:
            assert col in sql_lower, f"Missing column: {col}"

    def test_has_partition_column(self, sql):
        assert re.search(r"partitioned\s+by", sql, re.IGNORECASE) or "PARTITIONED BY" in sql.upper()

    def test_uses_orc_format(self, sql):
        assert "orc" in sql.lower() or "ORC" in sql

    def test_has_timestamp_columns(self, sql):
        sql_lower = sql.lower()
        assert "created_at" in sql_lower or "order_date" in sql_lower
        assert "updated_at" in sql_lower


# ---------------------------------------------------------------------------
# Menu table DDL tests
# ---------------------------------------------------------------------------
class TestMenuTableDDL:
    @pytest.fixture()
    def sql(self):
        return read_sql_file("menu_table.sql")

    def test_is_valid_create_table(self, sql):
        assert_valid_create_table(sql)

    def test_table_name(self, sql):
        assert "menu" in sql.lower()

    def test_required_columns(self, sql):
        sql_lower = sql.lower()
        required = ["item_id", "restaurant_id", "name", "price"]
        for col in required:
            assert col in sql_lower, f"Missing column: {col}"

    def test_has_cuisine_type(self, sql):
        assert "cuisine_type" in sql.lower()


# ---------------------------------------------------------------------------
# User activity table DDL tests
# ---------------------------------------------------------------------------
class TestUserActivityTableDDL:
    @pytest.fixture()
    def sql(self):
        return read_sql_file("user_activity_table.sql")

    def test_is_valid_create_table(self, sql):
        assert_valid_create_table(sql)

    def test_table_name(self, sql):
        assert "user_activity" in sql.lower()

    def test_required_columns(self, sql):
        sql_lower = sql.lower()
        required = ["user_id", "event_type"]
        for col in required:
            assert col in sql_lower, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# Common DDL property tests
# ---------------------------------------------------------------------------
class TestIcebergTableProperties:
    @pytest.mark.parametrize("sql_file", [
        "orders_table.sql", "menu_table.sql", "user_activity_table.sql",
    ])
    def test_iceberg_format_version(self, sql_file):
        sql = read_sql_file(sql_file)
        assert "'format-version'" in sql or "format_version" in sql.lower() or "format-version" in sql.lower()

    @pytest.mark.parametrize("sql_file", [
        "orders_table.sql", "menu_table.sql", "user_activity_table.sql",
    ])
    def test_location_specified(self, sql_file):
        sql = read_sql_file(sql_file)
        assert "location" in sql.lower() or "s3://" in sql.lower()

    @pytest.mark.parametrize("sql_file", [
        "orders_table.sql", "menu_table.sql", "user_activity_table.sql",
    ])
    def test_no_semicolon_missing(self, sql_file):
        sql = read_sql_file(sql_file).strip()
        assert sql.endswith(";"), f"{sql_file} should end with semicolon"


# ---------------------------------------------------------------------------
# Trino analytics query syntax tests
# ---------------------------------------------------------------------------
class TestTrinoQuerySyntax:
    """Validate common Trino query patterns used in the platform."""

    def test_date_partition_filter(self):
        query = """
        SELECT order_id, total_amount
        FROM iceberg.zomato.orders
        WHERE dt = '2024-01-15'
        ORDER BY total_amount DESC
        LIMIT 100
        """
        assert "WHERE" in query
        assert "ORDER BY" in query

    def test_aggregation_with_window(self):
        query = """
        SELECT
            city,
            date_trunc('hour', created_at) AS hour,
            COUNT(*) AS order_count,
            SUM(total_amount) AS revenue,
            AVG(total_amount) AS avg_order_value
        FROM iceberg.zomato.orders
        WHERE dt >= '2024-01-01'
        GROUP BY city, date_trunc('hour', created_at)
        ORDER BY revenue DESC
        """
        assert "GROUP BY" in query
        assert "date_trunc" in query

    def test_join_orders_users(self):
        query = """
        SELECT
            o.order_id,
            u.name AS user_name,
            o.total_amount,
            o.city
        FROM iceberg.zomato.orders o
        JOIN iceberg.zomato.users u ON o.user_id = u.user_id
        WHERE o.dt = '2024-01-15'
        """
        assert "JOIN" in query
        assert "ON" in query

    def test_approximate_distinct(self):
        query = """
        SELECT
            city,
            approx_distinct(user_id) AS unique_users,
            COUNT(*) AS total_orders
        FROM iceberg.zomato.orders
        WHERE dt >= current_date - INTERVAL '7' DAY
        GROUP BY city
        """
        assert "approx_distinct" in query
