from datetime import datetime

from refinery.silver.revisions import revisions

LOAD_1 = datetime(2026, 11, 5, 6)
LOAD_2 = datetime(2026, 11, 12, 6)
LOAD_3 = datetime(2026, 11, 19, 6)
SCHEMA = "week STRING, product STRING, loaded_at TIMESTAMP, rate INT"


def counts(spark, rows):
    """Per load: (rows compared with an earlier load, rows whose value changed)."""
    df = spark.createDataFrame(rows, SCHEMA)
    result = revisions(df, "t", ["week", "product"], "rate").collect()
    return {row.loaded_at: (row.compared_rows, row.revised_rows) for row in result}


def test_the_first_load_of_a_key_is_not_counted(spark):
    assert counts(spark, [("w1", "a", LOAD_1, 1)]) == {}


def test_changed_and_unchanged_values_are_counted_per_load(spark):
    rows = [
        ("w1", "a", LOAD_1, 1),
        ("w1", "a", LOAD_2, 2),
        ("w1", "a", LOAD_3, 2),
        ("w1", "b", LOAD_1, 5),
        ("w1", "b", LOAD_2, 5),
    ]

    assert counts(spark, rows) == {LOAD_2: (2, 1), LOAD_3: (1, 0)}


def test_a_missing_value_that_gets_a_value_is_a_revision(spark):
    rows = [("w1", "a", LOAD_1, None), ("w1", "a", LOAD_2, 3), ("w1", "a", LOAD_3, None)]

    assert counts(spark, rows) == {LOAD_2: (1, 1), LOAD_3: (1, 1)}


def test_result_names_the_table(spark):
    df = spark.createDataFrame([("w1", "a", LOAD_1, 1), ("w1", "a", LOAD_2, 2)], SCHEMA)

    assert revisions(df, "t", ["week", "product"], "rate").first().table_name == "t"
