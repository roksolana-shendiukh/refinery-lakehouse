from unittest.mock import MagicMock

import pytest
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import functions as F

from refinery.silver.quality import check, quarantine, valid

CHECKS = [
    {
        "name": "demo.code.not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "code"}},
    },
    {
        "name": "demo.label.not_null",
        "criticality": "warn",
        "check": {"function": "is_not_null", "arguments": {"column": "label"}},
    },
]


@pytest.fixture(scope="module")
def checked(spark):
    # Rows: clean, warning only, error only, error and warning.
    df = spark.createDataFrame([("A", "a"), ("B", None), (None, "c"), (None, None)], "code STRING, label STRING")
    # The engine only needs a workspace client for features we do not use here, so a mock is enough.
    engine = DQEngine(MagicMock(), spark=spark)
    return check(df, CHECKS, engine)


def test_valid_keeps_warning_only_rows(checked):
    rows = valid(checked).collect()
    assert {r.code for r in rows} == {"A", "B"}


def test_valid_has_the_input_columns_only(checked):
    assert valid(checked).columns == ["code", "label"]


def test_quarantine_has_every_row_with_a_finding(checked):
    assert quarantine(checked).count() == 3


def test_quarantine_is_input_columns_plus_result_columns(checked):
    assert quarantine(checked).columns == ["code", "label", "_errors", "_warnings"]


def test_warning_only_row_is_in_silver_and_in_quarantine(checked):
    in_silver = valid(checked).filter(F.col("code") == "B").count()
    in_quarantine = quarantine(checked).filter(F.col("code") == "B").count()
    assert (in_silver, in_quarantine) == (1, 1)


def test_rejected_rows_are_counted_by_errors(checked):
    assert quarantine(checked).filter(F.col("_errors").isNotNull()).count() == 2


def test_explicit_check_names_are_used(checked):
    names = quarantine(checked).select(F.explode("_errors").alias("e")).select("e.name").distinct().collect()
    assert [r.name for r in names] == ["demo.code.not_null"]
