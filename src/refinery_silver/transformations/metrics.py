"""Metrics of the last silver run: one row per metric and table, for the data-quality dashboard."""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from refinery.bronze.reference import REFERENCE_COLUMNS
from refinery.silver.eia import PRICE_CHECKS, SUPPLY_CHECKS
from refinery.silver.metrics import build_metrics_engine, summary_metrics
from refinery.silver.reference import pipeline_checks

# Schema as <catalog>.<schema>, set in the pipeline configuration
QUALITY = spark.conf.get("refinery.quality_schema")

ENGINE = build_metrics_engine(spark)

CHECKS = {name: pipeline_checks(name) for name in REFERENCE_COLUMNS}
CHECKS["eia_supply"] = SUPPLY_CHECKS
CHECKS["eia_price"] = PRICE_CHECKS


@dp.materialized_view(
    name=f"{QUALITY}.dq_summary_metrics_latest",
    comment="DQX summary metrics of the last silver run, one row per metric and table",
)
def dq_summary_metrics_latest():
    metrics = [
        summary_metrics(spark.read.table(f"{table}_checked"), checks, ENGINE, table) for table, checks in CHECKS.items()
    ]
    return reduce(DataFrame.unionByName, metrics)
