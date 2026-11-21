# Table grants, made by Terraform, which runs as refinery-prod-infra and owns the tables it creates.
# Schema grants are made by the bundle (databricks.yml, prod target): only the schema owner can grant on a schema.
# AIS producer (refinery-ais-producer): least privilege for Zerobus writes; token requests fail
# with any table privilege except SELECT and MODIFY.
# USE CATALOG on the course catalog is not here: none of our principals owns the catalog,
# so that grant is made once by the catalog owner.
# databricks_grant (not databricks_grants) only manages this principal's rights,
# so access granted to anyone else is never removed.

locals {
  grant_producer = var.ais_producer_principal != ""
}

resource "databricks_grant" "producer_landing_table" {
  count      = local.grant_producer ? 1 : 0
  table      = databricks_sql_table.zerobus_ais_messages.id
  principal  = var.ais_producer_principal
  privileges = ["SELECT", "MODIFY"]
}

# refinery-prod-ci runs the live pipeline and the freshness alert, which read the landing table.
resource "databricks_grant" "prod_ci_landing_table" {
  count      = var.prod_ci_principal != "" ? 1 : 0
  table      = databricks_sql_table.zerobus_ais_messages.id
  principal  = var.prod_ci_principal
  privileges = ["SELECT"]
}

# Schema grants moved to the bundle. Terraform forgets them without revoking them,
# so the producer and the live map app keep their access during the move.
removed {
  from = databricks_grant.producer_bronze_schema

  lifecycle {
    destroy = false
  }
}

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
