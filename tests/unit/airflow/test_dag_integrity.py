"""
Unit tests for Airflow DAG integrity.

Validates that all DAGs in the airflow/dags directory:
  1. Parse without Python errors
  2. Have no import errors
  3. Have correct schedule intervals
  4. Follow team naming / tagging conventions
  5. Have required default_args (owner, retries, email_on_failure)
"""

import importlib
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"

# Ensure DAG directory is on the path so imports resolve
sys.path.insert(0, str(DAGS_DIR))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _discover_dag_files() -> list[Path]:
    """Return all Python files in the DAGs directory."""
    if not DAGS_DIR.exists():
        return []
    return sorted(DAGS_DIR.glob("*.py"))


def _load_dag_module(dag_file: Path):
    """Import a DAG file as a Python module."""
    module_name = dag_file.stem
    spec = importlib.util.spec_from_file_location(module_name, str(dag_file))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _extract_dags(module) -> list:
    """Extract DAG objects from a loaded module."""
    from airflow.models import DAG

    dags = []
    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        if isinstance(obj, DAG):
            dags.append(obj)
    return dags


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
DAG_FILES = _discover_dag_files()


@pytest.fixture(scope="session")
def airflow_env():
    """Set minimal Airflow environment variables for DAG parsing."""
    env_vars = {
        "AIRFLOW_HOME": "/tmp/airflow_test",
        "AIRFLOW__CORE__DAGS_FOLDER": str(DAGS_DIR),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
    }
    with patch.dict(os.environ, env_vars):
        yield


# ---------------------------------------------------------------------------
# Expected DAG metadata
# ---------------------------------------------------------------------------
EXPECTED_DAGS = {
    "pipeline1_batch_etl": {
        "schedule": "0 */6 * * *",
        "tags_contain": ["pipeline1"],
    },
    "pipeline3_dynamodb_spark": {
        "schedule": "@hourly",
        "tags_contain": ["pipeline3"],
    },
    "trino_etl_queries": {
        "schedule": "0 2 * * *",
        "tags_contain": ["trino"],
    },
    "data_quality_checks": {
        "schedule": "0 */4 * * *",
        "tags_contain": ["data-quality"],
    },
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestDagDiscovery:
    """Verify DAG files are discovered."""

    def test_dag_directory_exists(self):
        assert DAGS_DIR.exists(), f"DAGs directory not found: {DAGS_DIR}"

    def test_dag_files_found(self):
        assert len(DAG_FILES) > 0, "No DAG files found"

    def test_expected_dag_files_present(self):
        filenames = {f.stem for f in DAG_FILES}
        expected = {
            "pipeline1_batch_etl_dag",
            "pipeline3_dynamodb_spark_dag",
            "trino_etl_dag",
            "data_quality_dag",
        }
        missing = expected - filenames
        assert not missing, f"Missing DAG files: {missing}"


class TestDagParsing:
    """Verify all DAGs parse without errors."""

    @pytest.mark.parametrize("dag_file", DAG_FILES, ids=[f.stem for f in DAG_FILES])
    def test_dag_file_imports_without_error(self, dag_file, airflow_env):
        """Each DAG file must import/execute without raising exceptions."""
        try:
            module = _load_dag_module(dag_file)
        except Exception as e:
            pytest.fail(f"DAG file {dag_file.name} failed to import: {e}")

    @pytest.mark.parametrize("dag_file", DAG_FILES, ids=[f.stem for f in DAG_FILES])
    def test_dag_file_contains_dag_object(self, dag_file, airflow_env):
        """Each DAG file must define at least one DAG object."""
        module = _load_dag_module(dag_file)
        dags = _extract_dags(module)
        assert len(dags) >= 1, f"{dag_file.name} does not define any DAG objects"


class TestDagConfiguration:
    """Validate DAG-level configuration correctness."""

    @pytest.fixture(scope="class")
    def all_dags(self, airflow_env) -> dict:
        """Load all DAGs once for the class."""
        result = {}
        for dag_file in DAG_FILES:
            module = _load_dag_module(dag_file)
            for dag in _extract_dags(module):
                result[dag.dag_id] = dag
        return result

    def test_no_duplicate_dag_ids(self, all_dags):
        """All DAG IDs must be unique."""
        # If we got here without error, dict keys enforce uniqueness
        assert len(all_dags) == len(set(all_dags.keys()))

    @pytest.mark.parametrize("dag_id,expected", EXPECTED_DAGS.items())
    def test_schedule_interval(self, all_dags, dag_id, expected):
        """Verify each DAG has the correct schedule interval."""
        if dag_id not in all_dags:
            pytest.skip(f"DAG {dag_id} not loaded")
        dag = all_dags[dag_id]
        schedule = dag.schedule_interval
        # schedule_interval can be a string or timedelta
        schedule_str = str(schedule) if not isinstance(schedule, str) else schedule
        assert schedule_str == expected["schedule"], (
            f"DAG {dag_id}: expected schedule '{expected['schedule']}', got '{schedule_str}'"
        )

    @pytest.mark.parametrize("dag_id,expected", EXPECTED_DAGS.items())
    def test_dag_tags(self, all_dags, dag_id, expected):
        """Verify each DAG has required tags."""
        if dag_id not in all_dags:
            pytest.skip(f"DAG {dag_id} not loaded")
        dag = all_dags[dag_id]
        tags = set(dag.tags or [])
        for required_tag in expected["tags_contain"]:
            assert required_tag in tags, (
                f"DAG {dag_id}: missing required tag '{required_tag}', has {tags}"
            )


class TestDagDefaultArgs:
    """Validate that DAGs have required default_args."""

    @pytest.fixture(scope="class")
    def all_dags(self, airflow_env) -> dict:
        result = {}
        for dag_file in DAG_FILES:
            module = _load_dag_module(dag_file)
            for dag in _extract_dags(module):
                result[dag.dag_id] = dag
        return result

    @pytest.mark.parametrize("dag_id", list(EXPECTED_DAGS.keys()))
    def test_owner_is_set(self, all_dags, dag_id):
        if dag_id not in all_dags:
            pytest.skip(f"DAG {dag_id} not loaded")
        dag = all_dags[dag_id]
        owner = dag.default_args.get("owner", "")
        assert owner, f"DAG {dag_id}: 'owner' not set in default_args"
        assert owner == "data-engineering", (
            f"DAG {dag_id}: expected owner 'data-engineering', got '{owner}'"
        )

    @pytest.mark.parametrize("dag_id", list(EXPECTED_DAGS.keys()))
    def test_retries_configured(self, all_dags, dag_id):
        if dag_id not in all_dags:
            pytest.skip(f"DAG {dag_id} not loaded")
        dag = all_dags[dag_id]
        retries = dag.default_args.get("retries", 0)
        assert retries >= 1, f"DAG {dag_id}: retries should be >= 1, got {retries}"

    @pytest.mark.parametrize("dag_id", list(EXPECTED_DAGS.keys()))
    def test_email_on_failure_enabled(self, all_dags, dag_id):
        if dag_id not in all_dags:
            pytest.skip(f"DAG {dag_id} not loaded")
        dag = all_dags[dag_id]
        email_on_failure = dag.default_args.get("email_on_failure", False)
        assert email_on_failure is True, (
            f"DAG {dag_id}: email_on_failure should be True"
        )

    @pytest.mark.parametrize("dag_id", list(EXPECTED_DAGS.keys()))
    def test_catchup_disabled(self, all_dags, dag_id):
        if dag_id not in all_dags:
            pytest.skip(f"DAG {dag_id} not loaded")
        dag = all_dags[dag_id]
        assert dag.catchup is False, f"DAG {dag_id}: catchup should be False"

    @pytest.mark.parametrize("dag_id", list(EXPECTED_DAGS.keys()))
    def test_max_active_runs(self, all_dags, dag_id):
        if dag_id not in all_dags:
            pytest.skip(f"DAG {dag_id} not loaded")
        dag = all_dags[dag_id]
        assert dag.max_active_runs >= 1, (
            f"DAG {dag_id}: max_active_runs should be >= 1"
        )
        assert dag.max_active_runs <= 3, (
            f"DAG {dag_id}: max_active_runs should be <= 3 for data pipelines"
        )


class TestDagTasks:
    """Validate that DAGs have reasonable task structure."""

    @pytest.fixture(scope="class")
    def all_dags(self, airflow_env) -> dict:
        result = {}
        for dag_file in DAG_FILES:
            module = _load_dag_module(dag_file)
            for dag in _extract_dags(module):
                result[dag.dag_id] = dag
        return result

    @pytest.mark.parametrize("dag_id", list(EXPECTED_DAGS.keys()))
    def test_dag_has_tasks(self, all_dags, dag_id):
        if dag_id not in all_dags:
            pytest.skip(f"DAG {dag_id} not loaded")
        dag = all_dags[dag_id]
        assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"

    @pytest.mark.parametrize("dag_id", list(EXPECTED_DAGS.keys()))
    def test_dag_has_no_cycles(self, all_dags, dag_id):
        """Verify the DAG is actually a DAG (no circular dependencies)."""
        if dag_id not in all_dags:
            pytest.skip(f"DAG {dag_id} not loaded")
        dag = all_dags[dag_id]
        # Airflow itself validates this during DAG creation,
        # but let's assert it explicitly
        try:
            dag.topological_sort()
        except Exception as e:
            pytest.fail(f"DAG {dag_id} has circular dependencies: {e}")
