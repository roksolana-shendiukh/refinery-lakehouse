"""Helpers shared by the silver tests."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("loaded_at", F.current_timestamp()).withColumn("source_file", F.lit("file"))


def finding_names(checked: DataFrame) -> list[str]:
    """Names of the failed checks, errors and warnings, one entry per failed row."""
    errors = checked.select(F.explode("_errors").alias("f"))
    warnings = checked.select(F.explode("_warnings").alias("f"))
    return sorted(row.name for row in errors.union(warnings).select("f.name").collect())
