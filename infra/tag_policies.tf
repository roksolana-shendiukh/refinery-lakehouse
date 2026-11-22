# Governed tags for the gold ABAC policies (governance/gold_abac.sql).
# Account-level objects. Our service principals have no account-level CREATE on governed tags,
# so both tags were created once by hand and are managed here from then on (imported below).
# Permissions set at creation: ASSIGN for refinery-prod-ci (the weekly job assigns the tags),
# MANAGE for refinery-prod-infra (Terraform manages the tags).

import {
  to = databricks_tag_policy.access_key
  id = "refinery_access_key"
}

import {
  to = databricks_tag_policy.sensitivity
  id = "refinery_sensitivity"
}

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
