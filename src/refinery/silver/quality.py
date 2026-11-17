"""Silver data quality helpers built on DQX.

DQX is applied once per table (`check`). Silver keeps the rows without errors (`valid`),
quarantine keeps every row with an error or a warning (`quarantine`).
"""

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ERRORS = "_errors"
WARNINGS = "_warnings"


def build_engine(spark: SparkSession) -> DQEngine:
    """Create the DQX engine used inside pipelines (workspace auth comes from the runtime)."""
    return DQEngine(WorkspaceClient(), spark=spark)


def check(df: DataFrame, checks: list[dict], engine: DQEngine) -> DataFrame:
    """Apply the checks once and append the `_errors` and `_warnings` result columns."""
    return engine.apply_checks_by_metadata(df, checks)


def valid(checked: DataFrame) -> DataFrame:
    """Rows without errors, as they go to silver.

    Rows with warnings only stay in silver, so the result columns are dropped.
    """
    return checked.filter(F.col(ERRORS).isNull()).drop(ERRORS, WARNINGS)


def quarantine(checked: DataFrame) -> DataFrame:
    """Rows with at least one error or warning, keeping the result columns.

    Count rejected rows with `_errors IS NOT NULL`: warning-only rows are here too,
    but they are also in silver.
    """
    return checked.filter(F.col(ERRORS).isNotNull() | F.col(WARNINGS).isNotNull())
