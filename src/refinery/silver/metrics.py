"""DQX summary metrics of the silver tables: row counts per table and per check, for the quality dashboard."""

from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession


def build_metrics_engine(spark: SparkSession) -> DQEngine:
    """Engine that only computes summary metrics; the checks engine has no observer, so apply_checks is unchanged."""
    return DQEngine(WorkspaceClient(), spark=spark, observer=DQMetricsObserver(name="refinery-silver"))


def summary_metrics(checked: DataFrame, checks: list[dict], engine: DQEngine, table: str) -> DataFrame:
    """One row per metric for a checked table: input, error, warning and valid rows, and each check.

    `checked` still has the DQX result columns, and `checks` are the checks applied to it. The run_id is the same
    for every table in one pipeline update, so one update is one run.
    """
    return engine.compute_summary_metrics(
        checked,
        checks=checks,
        input_config=InputConfig(location=table),
        quarantine_config=OutputConfig(location=f"{table}_quarantine"),
    )


def new_runs(latest: DataFrame, history: DataFrame) -> DataFrame:
    """Metrics of runs that are not in the history yet, so a rerun of the task adds nothing."""
    return latest.join(history.select("run_id").distinct(), "run_id", "left_anti")


def require_table(spark: SparkSession, table_name: str) -> None:
    """Fail clearly when the history table is missing: Terraform creates it, this job only appends."""
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"{table_name} does not exist; it is created by Terraform "
            "(in DEV, create it once by hand with the columns of dq_summary_metrics_latest)"
        )
