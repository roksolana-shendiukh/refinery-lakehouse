def test_local_spark_runs_a_query(spark):
    rows = spark.sql("SELECT 1 + 1 AS two").collect()

    assert rows[0].two == 2
