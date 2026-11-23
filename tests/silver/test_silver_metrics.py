from unittest.mock import MagicMock

import pytest
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver

from refinery.silver.metrics import new_runs, require_table, summary_metrics
from refinery.silver.quality import check, not_null

CHECKS = [not_null("t", "a", "error"), not_null("t", "b", "warn")]


@pytest.fixture
def metrics_engine(spark):
    return DQEngine(MagicMock(), spark=spark, observer=DQMetricsObserver(name="test"))


@pytest.fixture
def metrics(spark, engine, metrics_engine):
    """Rows: clean, one with an error, one with a warning."""
    df = spark.createDataFrame([(1, 1), (None, 1), (1, None)], "a INT, b INT")
    checked = check(df, CHECKS, engine)
    return summary_metrics(checked, CHECKS, metrics_engine, "t").collect()


def test_row_counts(metrics):
    values = {row.metric_name: row.metric_value for row in metrics}

    assert (values["input_row_count"], values["error_row_count"]) == ("3", "1")
    assert (values["warning_row_count"], values["valid_row_count"]) == ("1", "1")


def test_each_check_is_reported(metrics):
    values = {row.metric_name: row.metric_value for row in metrics}

    assert "t.a.not_null" in values["check_metrics"]
    assert "t.b.not_null" in values["check_metrics"]


def test_rows_name_the_table_and_share_one_run(metrics):
    assert {row.input_location for row in metrics} == {"t"}
    assert {row.quarantine_location for row in metrics} == {"t_quarantine"}
    assert len({row.run_id for row in metrics}) == 1


def test_new_runs_skips_runs_already_in_history(spark):
    latest = spark.createDataFrame([("r1", "x"), ("r1", "y"), ("r2", "x")], "run_id STRING, metric_name STRING")
    history = spark.createDataFrame([("r1", "x")], "run_id STRING, metric_name STRING")

    assert sorted(new_runs(latest, history).collect()) == [("r2", "x")]


def test_new_runs_keeps_everything_when_history_is_empty(spark):
    latest = spark.createDataFrame([("r1", "x")], "run_id STRING, metric_name STRING")
    history = spark.createDataFrame([], "run_id STRING, metric_name STRING")

    assert new_runs(latest, history).count() == 1


def test_require_table_fails_when_the_table_is_missing():
    session = MagicMock()
    session.catalog.tableExists.return_value = False

    with pytest.raises(RuntimeError, match="created by Terraform"):
        require_table(session, "catalog.schema.history")


def test_require_table_passes_when_the_table_exists():
    session = MagicMock()
    session.catalog.tableExists.return_value = True

    require_table(session, "catalog.schema.history")
