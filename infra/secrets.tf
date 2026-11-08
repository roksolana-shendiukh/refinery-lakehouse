# Secret scope for the live map app. Terraform creates the scope and its access rules only:
# secret values are put with the Databricks CLI by the secret writer, so they never appear in
# Terraform state, plans or CI logs.
# The bundle gives the app's service principal READ on this scope (app resource "carto-key").
# The scope creator (refinery-prod-ci, which also deploys the bundle) keeps MANAGE.

resource "databricks_secret_scope" "live_map" {
  name = var.live_map_secret_scope
}

resource "databricks_secret_acl" "live_map_writer" {
  scope      = databricks_secret_scope.live_map.name
  principal  = var.secret_writer
  permission = "WRITE"
}
