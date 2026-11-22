# Governed tags for the gold ABAC policies (governance/gold_abac.sql).
# Account-level objects: created here; the weekly job assigns them to gold columns
# and creates the row filter and column mask policies that match them.

resource "databricks_tag_policy" "access_key" {
  tag_key     = "refinery_access_key"
  description = "Which key a gold column holds: area for the row filter, hub for the spot price mask"
  values      = [{ name = "area" }, { name = "hub" }]

  lifecycle {
    prevent_destroy = true # the gold policies match columns by this tag
  }
}

resource "databricks_tag_policy" "sensitivity" {
  tag_key     = "refinery_sensitivity"
  description = "Sensitive gold columns, masked for regional analysts"
  values      = [{ name = "price" }]

  lifecycle {
    prevent_destroy = true # the gold price mask matches columns by this tag
  }
}
