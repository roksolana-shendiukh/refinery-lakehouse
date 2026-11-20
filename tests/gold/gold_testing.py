from pyspark.sql.types import StructType


def ddl_schema(spark, ddl: str) -> StructType:
    return spark.createDataFrame([], ddl).schema


def same_columns(df, spark, ddl: str) -> bool:
    expected = ddl_schema(spark, ddl)
    return [(f.name, f.dataType) for f in df.schema] == [(f.name, f.dataType) for f in expected]
