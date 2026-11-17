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


def build_check(table: str, label: str, rule: str, level: str, function: str, **arguments) -> dict:
    """One DQX check as metadata, named <table>.<label>.<rule>."""
    return {
        "name": f"{table}.{label}.{rule}",
        "criticality": level,
        "check": {"function": function, "arguments": arguments},
    }


def not_null(table, column, level):
    return build_check(table, column, "not_null", level, "is_not_null", column=column)


def one_of(table, column, allowed, level):
    # DQX reads a bare string in the list as a column name, so each value is quoted as a SQL literal.
    literals = [f"'{value}'" for value in allowed]
    return build_check(table, column, "allowed_values", level, "is_in_list", column=column, allowed=literals)


def unique(table, columns, level):
    return build_check(table, "+".join(columns), "unique", level, "is_unique", columns=columns)


def exists_in(table, column, ref_name, ref_column, level, rule="exists"):
    return build_check(
        table, column, rule, level, "foreign_key", columns=[column], ref_columns=[ref_column], ref_df_name=ref_name
    )


def expression(table, column, rule, level, sql, message):
    return build_check(table, column, rule, level, "sql_expression", expression=sql, msg=message, columns=[column])


def check(
    df: DataFrame, checks: list[dict], engine: DQEngine, ref_dfs: dict[str, DataFrame] | None = None
) -> DataFrame:
    """Apply the checks once and append the `_errors` and `_warnings` result columns.

    `ref_dfs` holds the tables that foreign key checks look up, by the name used in the check.
    """
    return engine.apply_checks_by_metadata(df, checks, ref_dfs=ref_dfs)


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
