# Rights of the AIS producer (refinery-ais-producer) for Zerobus writes.
# Least privilege: Zerobus token requests fail with any table privilege except SELECT and MODIFY.
# USE CATALOG on the course catalog is not here: refinery-prod-ci does not own the catalog,
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
