"""Silver EIA tables: typed weekly supply and prices from bronze, as the STM says."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def to_supply(df: DataFrame) -> DataFrame:
    """Silver eia_supply columns from eia_supply_raw."""
    value = F.trim(F.col("value"))
    rate = value.try_cast("decimal(10,1)")
    status = F.when(rate.isNotNull(), "ok").when(F.upper(value) == "W", "withheld").otherwise("missing")
    return df.select(
        F.to_date(F.try_to_timestamp(F.col("period"), F.lit("yyyy-MM-dd"))).alias("report_week"),
        F.upper(F.trim(F.col("duoarea"))).alias("area_bk"),
        F.col("product").alias("product_bk"),
        rate.alias("rate_kbd"),
        status.alias("value_status"),
        F.col("ingested_at").alias("loaded_at"),
        "source_file",
    )
