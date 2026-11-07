terraform {
  required_version = ">= 1.6"

  # State lives in HCP Terraform; plans and applies run locally (in GitHub Actions).
  cloud {
    organization = "refinery-lakehouse-tf"

    workspaces {
      name = "refinery-infra-prod"
    }
  }

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}
