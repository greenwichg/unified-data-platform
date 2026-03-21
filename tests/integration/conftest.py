"""
Pytest fixtures for integration tests using testcontainers.

Provides containerized Kafka, MinIO (S3-compatible), and Trino instances
for end-to-end testing of the Zomato data platform pipelines.
"""

import json
import os
import time
from typing import Generator

import boto3
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ---------------------------------------------------------------------------
# Testcontainer imports – lazy so the module can still be imported when
# testcontainers is not installed (e.g., during collection-only runs).
# ---------------------------------------------------------------------------
try:
    from testcontainers.kafka import KafkaContainer
    from testcontainers.minio import MinioContainer
    from testcontainers.core.container import DockerContainer
except ImportError:
    KafkaContainer = None
    MinioContainer = None
    DockerContainer = None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KAFKA_TOPICS = ["orders", "users", "menu", "promo", "topics", "druid-ingestion-events"]
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET_RAW = "zomato-data-platform-test-raw-data-lake"
MINIO_BUCKET_PROCESSED = "zomato-data-platform-test-processed"
TRINO_IMAGE = "trinodb/trino:435"
TRINO_PORT = 8080


# ---------------------------------------------------------------------------
# Kafka fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def kafka_container() -> Generator:
    """Start a Kafka container (Confluent) for the entire test session."""
    if KafkaContainer is None:
        pytest.skip("testcontainers not installed")

    container = (
        KafkaContainer("confluentinc/cp-kafka:7.6.0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("KAFKA_NUM_PARTITIONS", "3")
        .with_env("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container) -> str:
    """Return the Kafka bootstrap server address."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_admin(kafka_bootstrap) -> AdminClient:
    """Return a Kafka AdminClient connected to the test broker."""
    return AdminClient({"bootstrap.servers": kafka_bootstrap})


@pytest.fixture(scope="session")
def kafka_topics(kafka_admin, kafka_bootstrap) -> list[str]:
    """Create the standard Zomato Kafka topics and return their names."""
    new_topics = [
        NewTopic(topic, num_partitions=3, replication_factor=1)
        for topic in KAFKA_TOPICS
    ]
    futures = kafka_admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result(timeout=30)
        except Exception:
            # Topic may already exist from a previous run
            pass

    # Wait for topic metadata to propagate
    _wait_for_topics(kafka_bootstrap, KAFKA_TOPICS, timeout=30)
    return KAFKA_TOPICS


@pytest.fixture(scope="function")
def kafka_producer(kafka_bootstrap) -> Generator:
    """Provide a Kafka producer for a single test function."""
    producer = Producer({
        "bootstrap.servers": kafka_bootstrap,
        "acks": "all",
        "retries": 3,
        "linger.ms": 10,
        "enable.idempotence": True,
    })
    yield producer
    producer.flush(timeout=10)


@pytest.fixture(scope="function")
def kafka_consumer_factory(kafka_bootstrap):
    """Factory fixture: returns a function to create consumers for specific topics."""
    consumers: list[Consumer] = []

    def _create(topics: list[str], group_id: str = "test-consumer") -> Consumer:
        consumer = Consumer({
            "bootstrap.servers": kafka_bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
        })
        consumer.subscribe(topics)
        consumers.append(consumer)
        return consumer

    yield _create

    for consumer in consumers:
        consumer.close()


# ---------------------------------------------------------------------------
# MinIO (S3-compatible) fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def minio_container() -> Generator:
    """Start a MinIO container for the entire test session."""
    if MinioContainer is None:
        pytest.skip("testcontainers not installed")

    container = MinioContainer(
        "minio/minio:RELEASE.2024-01-18T22-51-28Z",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def s3_client(minio_container) -> boto3.client:
    """Return a boto3 S3 client pointed at MinIO."""
    endpoint_url = f"http://{minio_container.get_container_host_ip()}:{minio_container.get_exposed_port(9000)}"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    return client


@pytest.fixture(scope="session")
def s3_endpoint_url(minio_container) -> str:
    """Return the MinIO endpoint URL."""
    return f"http://{minio_container.get_container_host_ip()}:{minio_container.get_exposed_port(9000)}"


@pytest.fixture(scope="session")
def s3_buckets(s3_client) -> dict[str, str]:
    """Create test S3 buckets and return a mapping of logical name -> bucket name."""
    buckets = {
        "raw": MINIO_BUCKET_RAW,
        "processed": MINIO_BUCKET_PROCESSED,
    }
    for bucket_name in buckets.values():
        try:
            s3_client.create_bucket(Bucket=bucket_name)
        except s3_client.exceptions.BucketAlreadyOwnedByYou:
            pass
    return buckets


@pytest.fixture(scope="function")
def upload_test_data(s3_client, s3_buckets):
    """Factory fixture: upload test data files to MinIO and return the keys."""
    uploaded_keys: list[tuple[str, str]] = []

    def _upload(bucket_key: str, key: str, data: bytes | str, content_type: str = "application/octet-stream"):
        bucket = s3_buckets[bucket_key]
        body = data.encode("utf-8") if isinstance(data, str) else data
        s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
        uploaded_keys.append((bucket, key))
        return f"s3://{bucket}/{key}"

    yield _upload

    # Cleanup uploaded objects after each test
    for bucket, key in uploaded_keys:
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Trino fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def trino_container(minio_container, s3_endpoint_url) -> Generator:
    """Start a Trino container configured with Hive connector pointing to MinIO."""
    if DockerContainer is None:
        pytest.skip("testcontainers not installed")

    hive_properties = (
        "connector.name=hive\n"
        "hive.metastore=file\n"
        f"hive.s3.endpoint={s3_endpoint_url}\n"
        f"hive.s3.aws-access-key={MINIO_ACCESS_KEY}\n"
        f"hive.s3.aws-secret-key={MINIO_SECRET_KEY}\n"
        "hive.s3.path-style-access=true\n"
        "hive.s3.ssl.enabled=false\n"
        "hive.non-managed-table-writes-enabled=true\n"
        "hive.allow-drop-table=true\n"
    )

    container = (
        DockerContainer(TRINO_IMAGE)
        .with_exposed_ports(TRINO_PORT)
        .with_env("TRINO_ENVIRONMENT", "testing")
    )
    container.start()

    # Wait for Trino to be ready
    _wait_for_trino(container, timeout=120)

    yield container
    container.stop()


@pytest.fixture(scope="session")
def trino_host(trino_container) -> str:
    """Return host:port for the Trino coordinator."""
    host = trino_container.get_container_host_ip()
    port = trino_container.get_exposed_port(TRINO_PORT)
    return f"{host}:{port}"


@pytest.fixture(scope="session")
def trino_connection(trino_host):
    """Return a Trino DB-API connection."""
    try:
        import trino as trino_lib
    except ImportError:
        pytest.skip("trino Python package not installed")

    host, port = trino_host.split(":")
    conn = trino_lib.dbapi.connect(
        host=host,
        port=int(port),
        user="test",
        catalog="memory",
        schema="default",
    )
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Environment variable overrides
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def set_test_env_vars(kafka_bootstrap, s3_endpoint_url, s3_buckets):
    """Inject test-specific environment variables for pipeline code."""
    env_overrides = {
        "KAFKA_BOOTSTRAP": kafka_bootstrap,
        "S3_BUCKET": s3_buckets["raw"],
        "S3_ENDPOINT_URL": s3_endpoint_url,
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_DEFAULT_REGION": "us-east-1",
    }
    original = {k: os.environ.get(k) for k in env_overrides}
    os.environ.update(env_overrides)
    yield
    for k, v in original.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _wait_for_topics(bootstrap: str, topics: list[str], timeout: int = 30) -> None:
    """Poll the broker until all expected topics are present."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.time() + timeout
    while time.time() < deadline:
        metadata = admin.list_topics(timeout=5)
        existing = set(metadata.topics.keys())
        if all(t in existing for t in topics):
            return
        time.sleep(1)
    raise TimeoutError(f"Topics {topics} did not appear within {timeout}s")


def _wait_for_trino(container, timeout: int = 120) -> None:
    """Wait until the Trino server responds to a health check."""
    import urllib.request

    host = container.get_container_host_ip()
    port = container.get_exposed_port(TRINO_PORT)
    url = f"http://{host}:{port}/v1/info"
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                if data.get("starting") is False:
                    return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"Trino did not become ready within {timeout}s")
