from pyspark.sql import DataFrame, SparkSession


def silver(spark: SparkSession, table: str) -> DataFrame:
    """A silver table by its short name, e.g. silver(spark, "ref_area")."""
    schema = spark.conf.get("refinery.silver_schema")
    return spark.read.table(f"{schema}.{table}")