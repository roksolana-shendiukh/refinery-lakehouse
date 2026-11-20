from pyspark.sql import functions as F

from refinery.gold.keys import NOT_APPLICABLE, NOT_YET, UNKNOWN, hashed_sk, lookup_sk, with_special_members

DIM_SCHEMA = "thing_sk BIGINT, thing_bk STRING, size INT, is_big BOOLEAN"


def dim(spark):
    return spark.createDataFrame([(10, "A", 5, True)], DIM_SCHEMA)


def test_special_members_adds_three_rows_and_keeps_the_real_ones(spark):
    rows = with_special_members(dim(spark), "thing_sk").collect()

    assert sorted(r.thing_sk for r in rows) == [NOT_YET, NOT_APPLICABLE, UNKNOWN, 10]


def test_special_members_text_gets_the_label_and_other_columns_null(spark):
    rows = {r.thing_sk: r for r in with_special_members(dim(spark), "thing_sk").collect()}

    assert rows[UNKNOWN].thing_bk == "Unknown"
    assert rows[NOT_APPLICABLE].thing_bk == "Not applicable"
    assert rows[NOT_YET].thing_bk == "Not yet"
    assert rows[UNKNOWN].size is None
    assert rows[UNKNOWN].is_big is None


def test_special_members_keeps_the_column_types(spark):
    df = dim(spark)

    assert with_special_members(df, "thing_sk").schema == df.schema


def fact(spark):
    return spark.createDataFrame([(1, "R10"), (2, "R99"), (3, None)], "row_id INT, area_bk STRING")


def reference(spark):
    return spark.createDataFrame([("R10",), ("R20",)], "area_bk STRING")


def keys_by_row(df):
    return {r.row_id: r.area_sk for r in df.collect()}


def test_lookup_sk_gives_a_known_key_the_dimension_hash(spark):
    keys = keys_by_row(lookup_sk(fact(spark), reference(spark), "area_bk", "area_bk", "area_sk"))
    expected = spark.range(1).select(hashed_sk(F.lit("R10"))).first()[0]

    assert keys[1] == expected


def test_lookup_sk_gives_unknown_to_a_missing_or_null_key(spark):
    keys = keys_by_row(lookup_sk(fact(spark), reference(spark), "area_bk", "area_bk", "area_sk"))

    assert keys[2] == UNKNOWN
    assert keys[3] == UNKNOWN


def test_lookup_sk_keeps_every_fact_row_and_adds_only_the_key(spark):
    result = lookup_sk(fact(spark), reference(spark), "area_bk", "area_bk", "area_sk")

    assert result.count() == 3
    assert result.columns == ["row_id", "area_bk", "area_sk"]


def test_lookup_sk_does_not_duplicate_rows_when_the_reference_has_a_duplicate(spark):
    duplicated = reference(spark).union(reference(spark))

    assert lookup_sk(fact(spark), duplicated, "area_bk", "area_bk", "area_sk").count() == 3


def test_lookup_sk_works_when_the_columns_are_named_differently(spark):
    renamed = reference(spark).withColumnRenamed("area_bk", "product_bk")
    keys = keys_by_row(lookup_sk(fact(spark), renamed, "area_bk", "product_bk", "area_sk"))

    assert keys[1] != UNKNOWN
    assert keys[2] == UNKNOWN
