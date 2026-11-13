# Rights granted by Terraform, which runs as refinery-prod-infra.
# refinery-prod-infra owns the landing table and has MANAGE on refinery_bronze and refinery_live
# (granted once by hand), so it can grant here; refinery-prod-ci (the bundle deployer) cannot grant.
# AIS producer (refinery-ais-producer): least privilege for Zerobus writes; token requests fail
# with any table privilege except SELECT and MODIFY.
# USE CATALOG on the course catalog is not here: neither principal owns the catalog,
# so that grant is made once by the catalog owner.
# databricks_grant (not databricks_grants) only manages this principal's rights,
# so access granted to anyone else is never removed.

locals {
  grant_producer = var.ais_producer_principal != ""
}

resource "databricks_grant" "producer_bronze_schema" {
  count      = local.grant_producer ? 1 : 0
  schema     = "${var.catalog}.${var.bronze_schema}"
  principal  = var.ais_producer_principal
  privileges = ["USE_SCHEMA"]
}

resource "databricks_grant" "producer_landing_table" {
  count      = local.grant_producer ? 1 : 0
  table      = databricks_sql_table.zerobus_ais_messages.id
  principal  = var.ais_producer_principal
  privileges = ["SELECT", "MODIFY"]
}

resource "databricks_grant" "live_map_live_schema" {
  count      = var.live_map_principal != "" ? 1 : 0
  schema     = "${var.catalog}.${var.live_schema}"
  principal  = var.live_map_principal
  privileges = ["USE_SCHEMA", "SELECT"]
}

# refinery-prod-ci runs the live pipeline and the freshness alert, which read the landing table.
resource "databricks_grant" "prod_ci_landing_table" {
  count      = var.prod_ci_principal != "" ? 1 : 0
  table      = databricks_sql_table.zerobus_ais_messages.id
  principal  = var.prod_ci_principal
  privileges = ["SELECT"]
}
