# Workspace-level provider. Credentials come from the environment, never from files:
# DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET (OAuth M2M, refinery-prod-ci).
provider "databricks" {}
