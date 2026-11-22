removed {
  from = databricks_grant.live_map_live_schema

  lifecycle {
    destroy = false
  }
}

# refinery-prod-ci runs the refinery-rag-index job, which MERGEs into the RAG tables (MERGE reads them too).
resource "databricks_grant" "prod_ci_eia_articles" {
  count      = var.prod_ci_principal != "" ? 1 : 0
  table      = databricks_sql_table.eia_articles.id
  principal  = var.prod_ci_principal
  privileges = ["SELECT", "MODIFY"]
}

resource "databricks_grant" "prod_ci_eia_articles_quarantine" {
  count      = var.prod_ci_principal != "" ? 1 : 0
  table      = databricks_sql_table.eia_articles_quarantine.id
  principal  = var.prod_ci_principal
  privileges = ["SELECT", "MODIFY"]
}

resource "databricks_grant" "prod_ci_eia_article_chunks" {
  count      = var.prod_ci_principal != "" ? 1 : 0
  table      = databricks_sql_table.eia_article_chunks.id
  principal  = var.prod_ci_principal
  privileges = ["SELECT", "MODIFY"]
}