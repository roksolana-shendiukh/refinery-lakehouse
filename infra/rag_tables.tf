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


resource "databricks_sql_table" "eia_article_chunks" {
  catalog_name       = var.catalog
  schema_name        = var.rag_schema
  name               = "eia_article_chunks"
  table_type         = "MANAGED"
  data_source_format = "DELTA"
  warehouse_id       = var.warehouse_id
  comment            = "Overlapping chunks of the silver EIA articles and STEO sections, the source of the vector search index. Written by the chunk_articles task of refinery-rag-index (MERGE by chunk_id)."

  # A Delta Sync vector search index reads the changes of its source table from the change data feed.
  properties = {
    "delta.enableChangeDataFeed" = "true"
  }

  column {
    name     = "chunk_id"
    type     = "string"
    nullable = false
    comment  = "<doc_id>#<chunk_index, 4 digits>; primary key of the vector search index"
  }

  column {
    name     = "doc_id"
    type     = "string"
    nullable = false
    comment  = "Page the chunk is from (silver eia_articles.doc_id)"
  }

  column {
    name     = "doc_type"
    type     = "string"
    nullable = false
    comment  = "article (Today in Energy) or outlook (STEO section)"
  }

  column {
    name     = "title"
    type     = "string"
    nullable = false
    comment  = "Title of the page"
  }

  column {
    name     = "published_date"
    type     = "date"
    nullable = false
    comment  = "Article date, or the STEO release date"
  }

  column {
    name     = "source_url"
    type     = "string"
    nullable = false
    comment  = "Page on eia.gov"
  }

  column {
    name     = "chunk_index"
    type     = "int"
    nullable = false
    comment  = "Position of the chunk in the page, from 0"
  }

  column {
    name     = "chunk_text"
    type     = "string"
    nullable = false
    comment  = "\"Title (date)\" line, then whole paragraphs; embedded by the vector search index"
  }

  column {
    name     = "processed_at"
    type     = "timestamp"
    nullable = false
    comment  = "When the chunk_articles task last changed this row (UTC)"
  }
}


resource "databricks_sql_table" "assistant_requests" {
  catalog_name       = var.catalog
  schema_name        = var.rag_schema
  name               = "assistant_requests"
  table_type         = "MANAGED"
  data_source_format = "DELTA"
  warehouse_id       = var.warehouse_id
  comment            = "Request log of the RAG assistant: one row per question, with the searches, sources, tokens and latency. Written by the assistant app."

  column {
    name     = "request_id"
    type     = "string"
    nullable = false
    comment  = "UUID of the request"
  }

  column {
    name     = "asked_at"
    type     = "timestamp"
    nullable = false
    comment  = "When the question was asked (UTC)"
  }

  column {
    name     = "user_email"
    type     = "string"
    nullable = true
    comment  = "Who asked, from the Databricks Apps user header; null outside the app"
  }

  column {
    name     = "question"
    type     = "string"
    nullable = false
    comment  = "The question as asked"
  }

  column {
    name     = "answer"
    type     = "string"
    nullable = false
    comment  = "The answer shown to the user (the refusal or error text if there was no answer)"
  }

  column {
    name     = "model"
    type     = "string"
    nullable = false
    comment  = "Serving endpoint of the chat model"
  }

  column {
    name     = "tool_calls"
    type     = "string"
    nullable = false
    comment  = "JSON array of the searches: tool name, arguments as sent by the model, ids of the chunks found"
  }

  column {
    name     = "retrieved_chunk_ids"
    type     = "array<string>"
    nullable = false
    comment  = "Every chunk the searches returned (rag.eia_article_chunks.chunk_id)"
  }

  column {
    name     = "cited_urls"
    type     = "array<string>"
    nullable = false
    comment  = "Pages the answer links to"
  }

  column {
    name     = "iterations"
    type     = "int"
    nullable = false
    comment  = "Model calls for this question"
  }

  column {
    name     = "prompt_tokens"
    type     = "int"
    nullable = false
    comment  = "Input tokens over all model calls"
  }

  column {
    name     = "completion_tokens"
    type     = "int"
    nullable = false
    comment  = "Output tokens over all model calls"
  }

  column {
    name     = "latency_ms"
    type     = "int"
    nullable = false
    comment  = "Time to answer, in milliseconds"
  }

  column {
    name     = "stopped_at_limit"
    type     = "boolean"
    nullable = false
    comment  = "The model still wanted to search after the iteration limit"
  }

  column {
    name     = "error"
    type     = "string"
    nullable = true
    comment  = "Error of the model or the search; null when the question was answered"
  }
}
