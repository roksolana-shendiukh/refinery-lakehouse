"""Bronze streaming table for the landed EIA pages of the RAG assistant, read by Auto Loader."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from refinery.bronze.landing_columns import with_landing_metadata
from refinery.rag.bronze import LANDING_FOLDER, LANDING_SCHEMA

LANDING_PATH = spark.conf.get("refinery.landing_path")

DATA_COLUMNS = [column.split()[0] for column in LANDING_SCHEMA.split(", ")]


@dp.table(
    name="eia_articles_raw",
    comment="EIA pages for the RAG assistant (Today in Energy articles, STEO pages): one row per landed page, as landed",
    cluster_by=["load_date"],
    table_properties={"pipelines.reset.allowed": "false"},
)
def eia_articles_raw():
    landed = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.partitionColumns", "")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(LANDING_SCHEMA)
        .load(f"{LANDING_PATH}/{LANDING_FOLDER}/")
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
    return with_landing_metadata(landed, DATA_COLUMNS)
