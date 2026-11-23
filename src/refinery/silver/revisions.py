"""Revisions of EIA values: the same key loaded again with a different value."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def revisions(df: DataFrame, table: str, keys: list[str], value: str) -> DataFrame:
    """Per load: how many rows were already in an earlier load, and how many of them changed their value.

    EIA is read again for the last weeks on every load, so a key arrives with several `loaded_at`.
    The first load of a key is not counted.
    """
    by_key = Window.partitionBy(*keys).orderBy("loaded_at")
    compared = df.select(
        "loaded_at",
        (F.row_number().over(by_key) > 1).alias("seen_before"),
        (~F.col(value).eqNullSafe(F.lag(value).over(by_key))).alias("changed"),
    )
    return (
        compared.where("seen_before")
        .groupBy("loaded_at")
        .agg(F.count("*").alias("compared_rows"), F.sum(F.col("changed").cast("int")).alias("revised_rows"))
        .select(F.lit(table).alias("table_name"), "loaded_at", "compared_rows", "revised_rows")
    )
