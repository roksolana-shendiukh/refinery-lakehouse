"""Writing the RAG tables from job tasks: the tables exist (Terraform creates them), the task MERGEs into them.

A MERGE makes the table hold exactly the given records: changed rows are updated, new ones inserted and rows
that are no longer produced deleted. Unchanged rows are not touched, so the change data feed (and the vector
search index that reads it) sees only real changes.
"""

import logging

logger = logging.getLogger(__name__)

UPDATES_VIEW = "rag_updates"
UNCOMPARED_COLUMNS = {"processed_at"}  # a row that only got a new processed_at is not a change


def require_tables(spark, *table_names):
    for table_name in table_names:
        if not spark.catalog.tableExists(table_name):
            raise RuntimeError(f"{table_name} does not exist; it is created by Terraform (infra/rag_tables.tf)")


def merge_sql(columns, key):
    """MERGE of the updates view into the target by key: changed rows updated, new inserted, rows gone deleted."""
    changed = " OR ".join(f"NOT (t.{column} <=> s.{column})" for column in columns if column not in UNCOMPARED_COLUMNS)
    return f"""
        MERGE INTO IDENTIFIER(:target_table) AS t
        USING {UPDATES_VIEW} AS s
        ON t.{key} = s.{key}
        WHEN MATCHED AND ({changed}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """


def merge_records(spark, table_name, records, key):
    """Make the table hold exactly these records (in the table's own column order and types)."""
    schema = spark.table(table_name).schema
    rows = [tuple(record.get(field.name) for field in schema.fields) for record in records]
    spark.createDataFrame(rows, schema).createOrReplaceTempView(UPDATES_VIEW)

    result = spark.sql(merge_sql(schema.fieldNames(), key), args={"target_table": table_name}).collect()
    logger.info("%s: %s", table_name, result[0].asDict() if result else {})
