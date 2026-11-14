"""Bronze streaming tables for the landed reference CSVs, read by Auto Loader."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from refinery.bronze.landing_columns import to_reference_bronze
from refinery.bronze.reference import REFERENCE_COLUMNS, REFERENCE_FOLDER

LANDING_PATH = spark.conf.get("refinery.landing_path")


def read_landed_csv(name):
    """New versions of one reference CSV in the volume, every column as text, with source_file."""
    schema = ", ".join(f"`{column}` STRING" for column in REFERENCE_COLUMNS[name])
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.partitionColumns", "")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(schema)
        .load(f"{LANDING_PATH}/{REFERENCE_FOLDER}/{name}/")
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


def define_reference_table(name):
    @dp.table(
        name=f"{name}_raw",
        comment=f"Reference CSV {name}.csv from the repo: every landed version, values as text",
        cluster_by=["load_date"],
        table_properties={"pipelines.reset.allowed": "false"},
    )
    def reference_raw():
        return to_reference_bronze(read_landed_csv(name), name)


for name in REFERENCE_COLUMNS:
    define_reference_table(name)
