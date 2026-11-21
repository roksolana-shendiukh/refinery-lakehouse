from refinery.rag.tables import merge_sql


def test_merge_matches_by_key_and_updates_only_rows_whose_content_changed():
    sql = merge_sql(["chunk_id", "chunk_text", "processed_at"], key="chunk_id")

    assert "ON t.chunk_id = s.chunk_id" in sql
    assert "WHEN MATCHED AND (NOT (t.chunk_id <=> s.chunk_id) OR NOT (t.chunk_text <=> s.chunk_text))" in sql
    assert "processed_at <=>" not in sql


def test_merge_inserts_new_rows_and_deletes_rows_no_longer_produced():
    sql = merge_sql(["doc_id", "body_text"], key="doc_id")

    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in sql
