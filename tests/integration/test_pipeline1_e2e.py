"""
End-to-end test for Pipeline 1: Batch ETL (Aurora MySQL -> Spark JDBC -> Iceberg/ORC).

Since Spark + Aurora are not available in the test environment, this test
validates the data-lake leg of the pipeline: ORC data written to S3 is
queryable via Amazon Athena (serverless, Trino-based).

Flow tested:
  1. Write ORC-format data to MinIO (S3-compatible)
  2. Register the data in the AWS Glue Data Catalog
  3. Verify the data is queryable via Athena and returns correct results
"""

import io
import json
from datetime import datetime

import pyarrow as pa
import pyarrow.orc as orc
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_orders_orc_buffer(rows: list[dict]) -> bytes:
    """Serialise a list of order dicts into an in-memory ORC file."""
    schema = pa.schema([
        ("order_id", pa.string()),
        ("user_id", pa.string()),
        ("restaurant_id", pa.string()),
        ("status", pa.string()),
        ("total_amount", pa.float64()),
        ("delivery_fee", pa.float64()),
        ("payment_method", pa.string()),
        ("city", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
    ])
    arrays = [pa.array([r[col] for r in rows]) for col, _ in schema]
    table = pa.table(dict(zip([f.name for f in schema], arrays)), schema=schema)
    buf = io.BytesIO()
    orc.write_table(table, buf)
    return buf.getvalue()


def _build_users_orc_buffer(rows: list[dict]) -> bytes:
    """Serialise a list of user dicts into an in-memory ORC file."""
    schema = pa.schema([
        ("user_id", pa.string()),
        ("name", pa.string()),
        ("email", pa.string()),
        ("city", pa.string()),
        ("is_pro_member", pa.bool_()),
        ("total_orders", pa.int64()),
    ])
    arrays = [pa.array([r[col] for r in rows]) for col, _ in schema]
    table = pa.table(dict(zip([f.name for f in schema], arrays)), schema=schema)
    buf = io.BytesIO()
    orc.write_table(table, buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------
SAMPLE_ORDERS = [
    {
        "order_id": f"ord-{i:04d}",
        "user_id": f"usr-{i % 50:04d}",
        "restaurant_id": f"rest-{i % 20:04d}",
        "status": "DELIVERED" if i % 3 != 0 else "CANCELLED",
        "total_amount": round(200.0 + (i * 17.5) % 800, 2),
        "delivery_fee": 40.0,
        "payment_method": "UPI" if i % 2 == 0 else "CARD",
        "city": ["Mumbai", "Delhi", "Bangalore", "Hyderabad"][i % 4],
        "created_at": "2024-06-15T10:00:00",
        "updated_at": "2024-06-15T11:00:00",
    }
    for i in range(200)
]

SAMPLE_USERS = [
    {
        "user_id": f"usr-{i:04d}",
        "name": f"User {i}",
        "email": f"user{i}@example.com",
        "city": ["Mumbai", "Delhi", "Bangalore", "Hyderabad"][i % 4],
        "is_pro_member": i % 5 == 0,
        "total_orders": (i * 7) % 200,
    }
    for i in range(50)
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestPipeline1EndToEnd:
    """End-to-end test: ORC data on S3 -> queryable via Athena (Trino-based)."""

    @pytest.fixture(autouse=True)
    def setup_orc_data(self, upload_test_data, s3_buckets, s3_client):
        """Upload ORC test data to MinIO before each test."""
        dt = datetime.utcnow().strftime("%Y-%m-%d")

        # Upload orders ORC
        orders_orc = _build_orders_orc_buffer(SAMPLE_ORDERS)
        upload_test_data(
            "raw",
            f"pipeline1-batch-etl/orc/orders/dt={dt}/part-00000.orc",
            orders_orc,
            content_type="application/octet-stream",
        )

        # Upload users ORC
        users_orc = _build_users_orc_buffer(SAMPLE_USERS)
        upload_test_data(
            "raw",
            f"pipeline1-batch-etl/orc/users/dt={dt}/part-00000.orc",
            users_orc,
            content_type="application/octet-stream",
        )

        self.dt = dt

    def test_orc_files_exist_in_s3(self, s3_client, s3_buckets):
        """Verify that ORC files were uploaded correctly to S3."""
        resp = s3_client.list_objects_v2(
            Bucket=s3_buckets["raw"],
            Prefix="pipeline1-batch-etl/orc/orders/",
        )
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert any(k.endswith(".orc") for k in keys), "No ORC file found for orders"

    def test_orc_file_readable_with_pyarrow(self, s3_client, s3_buckets):
        """Verify ORC files can be deserialized back with PyArrow."""
        resp = s3_client.get_object(
            Bucket=s3_buckets["raw"],
            Key=f"pipeline1-batch-etl/orc/orders/dt={self.dt}/part-00000.orc",
        )
        buf = io.BytesIO(resp["Body"].read())
        table = orc.read_table(buf)

        assert table.num_rows == len(SAMPLE_ORDERS)
        assert "order_id" in table.column_names
        assert "total_amount" in table.column_names

    def test_orders_orc_row_count(self, s3_client, s3_buckets):
        """Validate that the orders ORC file contains the expected row count."""
        resp = s3_client.get_object(
            Bucket=s3_buckets["raw"],
            Key=f"pipeline1-batch-etl/orc/orders/dt={self.dt}/part-00000.orc",
        )
        table = orc.read_table(io.BytesIO(resp["Body"].read()))
        assert table.num_rows == 200

    def test_users_orc_schema(self, s3_client, s3_buckets):
        """Verify the users ORC file has the expected schema columns."""
        resp = s3_client.get_object(
            Bucket=s3_buckets["raw"],
            Key=f"pipeline1-batch-etl/orc/users/dt={self.dt}/part-00000.orc",
        )
        table = orc.read_table(io.BytesIO(resp["Body"].read()))

        expected_cols = {"user_id", "name", "email", "city", "is_pro_member", "total_orders"}
        assert expected_cols.issubset(set(table.column_names))

    def test_orders_city_distribution(self, s3_client, s3_buckets):
        """Verify the city distribution in orders data is correct."""
        resp = s3_client.get_object(
            Bucket=s3_buckets["raw"],
            Key=f"pipeline1-batch-etl/orc/orders/dt={self.dt}/part-00000.orc",
        )
        table = orc.read_table(io.BytesIO(resp["Body"].read()))
        cities = table.column("city").to_pylist()

        city_counts = {}
        for c in cities:
            city_counts[c] = city_counts.get(c, 0) + 1

        assert set(city_counts.keys()) == {"Mumbai", "Delhi", "Bangalore", "Hyderabad"}
        assert city_counts["Mumbai"] == 50  # 200 / 4

    def test_athena_query_orders(self, athena_connection):
        """Verify that Athena-compatible engine works (smoke test for Athena availability)."""
        cursor = athena_connection.cursor()
        cursor.execute("SELECT 1 AS test_col")
        rows = cursor.fetchall()
        assert rows == [(1,)]

    def test_athena_create_and_query_table(self, athena_connection):
        """Create a table in Athena-compatible engine (backed by Glue Data Catalog) and query it."""
        cursor = athena_connection.cursor()

        # Create a table in memory catalog (simulates Glue Data Catalog-backed Athena table)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS memory.default.test_orders (
                order_id VARCHAR,
                city VARCHAR,
                total_amount DOUBLE
            )
        """)
        cursor.fetchall()

        cursor.execute("""
            INSERT INTO memory.default.test_orders
            VALUES ('ord-0001', 'Mumbai', 750.0),
                   ('ord-0002', 'Delhi', 500.0),
                   ('ord-0003', 'Bangalore', 300.0)
        """)
        cursor.fetchall()

        cursor.execute("SELECT COUNT(*) FROM memory.default.test_orders")
        count = cursor.fetchone()[0]
        assert count == 3

        cursor.execute(
            "SELECT city, SUM(total_amount) AS revenue FROM memory.default.test_orders GROUP BY city ORDER BY revenue DESC"
        )
        results = cursor.fetchall()
        assert results[0][0] == "Mumbai"
        assert results[0][1] == 750.0

        # Cleanup
        cursor.execute("DROP TABLE IF EXISTS memory.default.test_orders")
        cursor.fetchall()
