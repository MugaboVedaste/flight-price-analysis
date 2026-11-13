
import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Apache Airflow is not installed")

DAG_FILE = (
    Path(__file__).resolve().parents[1] / "dags" / "flight_price_pipeline.py"
)

EXPECTED_TASKS = {
    "ingest_flight_data",
    "validate_flight_data",
    "transform_flight_data",
    "load_postgres_data",
    "run_flight_analytics",
}


def _load_dag_module(monkeypatch, gmail_address):
    if gmail_address is None:
        monkeypatch.delenv("GMAIL_ADDRESS", raising=False)
    else:
        monkeypatch.setenv("GMAIL_ADDRESS", gmail_address)

    spec = importlib.util.spec_from_file_location("dag_under_test", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("gmail_address", [None, "alerts@example.com"])
def test_dag_imports_with_all_tasks(monkeypatch, gmail_address):
    module = _load_dag_module(monkeypatch, gmail_address)

    dag = module.flight_price_pipeline()
    assert {task.task_id for task in dag.tasks} == EXPECTED_TASKS


def test_analytics_runs_after_loading(monkeypatch):
    module = _load_dag_module(monkeypatch, None)

    dag = module.flight_price_pipeline()
    analytics = dag.get_task("run_flight_analytics")

    assert "load_postgres_data" in analytics.upstream_task_ids
