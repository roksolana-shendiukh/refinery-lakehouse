from refinery.gold.keys import NOT_APPLICABLE, NOT_YET, UNKNOWN, with_special_members

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
