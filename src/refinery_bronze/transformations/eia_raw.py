"""Bronze streaming tables for the landed EIA files, read by Auto Loader."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from refinery.bronze.eia import DATASETS
from refinery.bronze.landing_columns import EIA_FIELDS, to_bronze

LANDING_PATH = spark.conf.get("refinery.landing_path")

RAW_SCHEMA = ", ".join(f"`{field}` STRING" for field in EIA_FIELDS)

BRONZE_TABLE = {
    "cluster_by": ["load_date"],
    "table_properties": {"pipelines.reset.allowed": "false"},
}


def read_landed(folder):
    """New JSON Lines files under one landing folder, with the file path as source_file."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.partitionColumns", "")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(RAW_SCHEMA)
        .load(f"{LANDING_PATH}/eia/{folder}/")
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="eia_supply_raw",
    comment="EIA weekly product supplied (cons/wpsup): one row per API record, values as text",
    **BRONZE_TABLE,
)
def eia_supply_raw():
    return to_bronze(read_landed(DATASETS["supply"].folder))


@dp.table(
    name="eia_price_raw",
    comment="EIA retail (pri/gnd) and spot (pri/spt) prices: one row per API record, values as text",
    **BRONZE_TABLE,
)
def eia_price_raw():
    return to_bronze(read_landed("price"))
