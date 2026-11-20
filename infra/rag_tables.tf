# Tables of the RAG assistant written by the refinery-rag-index job (not by a pipeline), so Terraform owns them.
# The job only MERGEs into them. If a column change makes Terraform recreate a table, the next job run refills it.

resource "databricks_sql_table" "eia_articles" {
  catalog_name       = var.catalog
  schema_name        = var.silver_schema
  name               = "eia_articles"
  table_type         = "MANAGED"
  data_source_format = "DELTA"
  warehouse_id       = var.warehouse_id
  comment            = "EIA articles and STEO sections for the RAG assistant: parsed, on topic, without reposts. Written by the clean_articles task of refinery-rag-index (MERGE by doc_id)."

  column {
    name     = "doc_id"
    type     = "string"
    nullable = false
    comment  = "article_<id> or outlook_<page>"
  }

  column {
    name     = "doc_type"
    type     = "string"
    nullable = false
    comment  = "article (Today in Energy) or outlook (STEO section)"
  }

  column {
    name     = "doc_key"
    type     = "string"
    nullable = false
    comment  = "Article id, or the STEO page name"
  }

  column {
    name     = "source_url"
    type     = "string"
    nullable = false
    comment  = "Page on eia.gov"
  }

  column {
    name     = "title"
    type     = "string"
    nullable = false
    comment  = "Article or STEO section title"
  }

  column {
    name     = "published_date"
    type     = "date"
    nullable = false
    comment  = "Article date, or the STEO release date"
  }

  column {
    name     = "next_release_date"
    type     = "date"
    nullable = true
    comment  = "Next STEO release; null for articles"
  }

  column {
    name     = "tags"
    type     = "array<string>"
    nullable = true
    comment  = "Tags on the article page; empty for STEO"
  }

  column {
    name     = "contributors"
    type     = "string"
    nullable = true
    comment  = "Principal contributors of the article"
  }

  column {
    name     = "body_text"
    type     = "string"
    nullable = false
    comment  = "Text with [Chart: ...] lines, normalized"
  }

  column {
    name     = "word_count"
    type     = "int"
    nullable = false
    comment  = "Words in body_text"
  }

  column {
    name     = "content_hash"
    type     = "string"
    nullable = false
    comment  = "SHA-256 of the landed HTML"
  }

  column {
    name     = "load_id"
    type     = "string"
    nullable = false
    comment  = "Load that landed this version of the page"
  }

  column {
    name     = "processed_at"
    type     = "timestamp"
    nullable = false
    comment  = "When the clean_articles task last changed this row (UTC)"
  }
}

resource "databricks_sql_table" "eia_articles_quarantine" {
  catalog_name       = var.catalog
  schema_name        = var.quality_schema
  name               = "eia_articles_quarantine"
  table_type         = "MANAGED"
  data_source_format = "DELTA"
  warehouse_id       = var.warehouse_id
  comment            = "EIA pages left out of the RAG knowledge base, one row per page with the reason. Written by the clean_articles task of refinery-rag-index (MERGE by doc_id)."

  column {
    name     = "doc_id"
    type     = "string"
    nullable = false
    comment  = "article_<id> or outlook_<page>"
  }

  column {
    name     = "doc_type"
    type     = "string"
    nullable = false
    comment  = "article (Today in Energy) or outlook (STEO section)"
  }

  column {
    name     = "doc_key"
    type     = "string"
    nullable = false
    comment  = "Article id, or the STEO page name"
  }

  column {
    name     = "source_url"
    type     = "string"
    nullable = false
    comment  = "Page on eia.gov"
  }

  column {
    name     = "title"
    type     = "string"
    nullable = true
    comment  = "Parsed title, or the title on the tag page if parsing failed"
  }

  column {
    name     = "published_date"
    type     = "date"
    nullable = true
    comment  = "Article date, or the STEO release date; null if parsing failed"
  }

  column {
    name     = "reject_reason"
    type     = "string"
    nullable = false
    comment  = "parse_error, no_title, too_short, published_too_early, repost or off_topic"
  }

  column {
    name     = "reject_detail"
    type     = "string"
    nullable = true
    comment  = "Parser error message for parse_error"
  }

  column {
    name     = "word_count"
    type     = "int"
    nullable = true
    comment  = "Words in the parsed text"
  }

  column {
    name     = "topic_mentions"
    type     = "int"
    nullable = true
    comment  = "Petroleum terms found in the parsed text"
  }

  column {
    name     = "content_hash"
    type     = "string"
    nullable = false
    comment  = "SHA-256 of the landed HTML"
  }

  column {
    name     = "load_id"
    type     = "string"
    nullable = false
    comment  = "Load that landed this version of the page"
  }

  column {
    name     = "processed_at"
    type     = "timestamp"
    nullable = false
    comment  = "When the clean_articles task last changed this row (UTC)"
  }
}
