"""
Airflow DAG: Athena ETL Queries

Runs scheduled Athena queries for data transformations,
aggregations, and derived table generation. Joins data across MySQL, Druid,
and the S3 data lake.

Migrated from self-hosted Trino to Amazon Athena (serverless).
250K+ queries/week, 2PB scanned via Athena.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "data-engineering",
    "depends_on_past": True,
    "email": ["data-alerts@zomato.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}

ATHENA_OUTPUT_S3 = "{{ var.value.athena_query_results_s3 }}"

# Athena queries for daily aggregations
ATHENA_QUERIES = {
    "daily_order_summary": """
        CREATE TABLE IF NOT EXISTS analytics.daily_order_summary
        WITH (format = 'ORC', partitioned_by = ARRAY['dt'])
        AS SELECT 1 WHERE false;

        INSERT INTO analytics.daily_order_summary
        SELECT
            restaurant_id,
            city,
            COUNT(*) AS total_orders,
            SUM(total_amount) AS total_revenue,
            AVG(total_amount) AS avg_order_value,
            COUNT(DISTINCT user_id) AS unique_customers,
            SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
            SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
            CAST('{{ ds }}' AS VARCHAR) AS dt
        FROM iceberg.zomato.orders
        WHERE dt = '{{ ds }}'
        GROUP BY restaurant_id, city;
    """,
    "daily_user_activity": """
        INSERT INTO analytics.daily_user_activity
        SELECT
            u.city,
            COUNT(DISTINCT u.user_id) AS active_users,
            COUNT(DISTINCT CASE WHEN u.is_pro_member THEN u.user_id END) AS pro_users,
            COUNT(o.order_id) AS total_orders,
            AVG(o.total_amount) AS avg_spend,
            CAST('{{ ds }}' AS VARCHAR) AS dt
        FROM iceberg.zomato.users u
        LEFT JOIN iceberg.zomato.orders o
            ON u.user_id = o.user_id AND o.dt = '{{ ds }}'
        WHERE u.dt = '{{ ds }}'
        GROUP BY u.city;
    """,
    "popular_items_by_city": """
        INSERT INTO analytics.popular_items_by_city
        SELECT
            m.city,
            m.cuisine_type,
            m.name AS item_name,
            COUNT(*) AS order_count,
            SUM(CAST(m.price AS DOUBLE)) AS total_revenue,
            AVG(m.rating) AS avg_rating,
            CAST('{{ ds }}' AS VARCHAR) AS dt
        FROM iceberg.zomato.menu m
        WHERE m.dt = '{{ ds }}'
        GROUP BY m.city, m.cuisine_type, m.name
        ORDER BY order_count DESC;
    """,
    "promo_effectiveness": """
        INSERT INTO analytics.promo_effectiveness
        SELECT
            p.promo_code,
            p.discount_type,
            COUNT(*) AS usage_count,
            SUM(o.total_amount) AS total_order_value,
            AVG(o.total_amount) AS avg_order_value,
            SUM(CAST(p.discount_value AS DOUBLE)) AS total_discount_given,
            CAST('{{ ds }}' AS VARCHAR) AS dt
        FROM iceberg.zomato.orders o
        JOIN iceberg.zomato.promo p
            ON o.dt = p.dt
        WHERE o.dt = '{{ ds }}'
          AND p.is_active = true
        GROUP BY p.promo_code, p.discount_type;
    """,
}

with DAG(
    dag_id="athena_etl_queries",
    default_args=default_args,
    description="Daily Athena ETL aggregations on the data lake",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["athena", "etl", "analytics"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting Athena ETL queries - {{ ds }}'",
    )

    with TaskGroup("athena_queries") as queries_group:
        for query_name, query_sql in ATHENA_QUERIES.items():
            AthenaOperator(
                task_id=query_name,
                query=query_sql,
                database="zomato",
                output_location=ATHENA_OUTPUT_S3,
                workgroup="etl",
                aws_conn_id="aws_default",
            )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Athena ETL queries completed - {{ ds }}'",
    )

    start >> queries_group >> end
