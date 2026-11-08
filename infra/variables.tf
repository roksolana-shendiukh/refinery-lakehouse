variable "catalog" {
  description = "Unity Catalog catalog that holds the Refinery schemas"
  type        = string
}

variable "bronze_schema" {
  description = "Schema of the Zerobus landing table (created by the bundle, not by Terraform)"
  type        = string
}

variable "warehouse_id" {
  description = "SQL warehouse that runs the table DDL (otherwise the provider creates its own cluster)"
  type        = string
}

variable "ais_producer_principal" {
  description = "Application ID of refinery-ais-producer; empty = no grants yet"
  type        = string
  default     = ""
}

variable "live_schema" {
  description = "Schema of the live serving tables (created by the bundle, not by Terraform)"
  type        = string
}

variable "live_map_principal" {
  description = "Application ID of the live map app's service principal; empty = no grants yet"
  type        = string
  default     = ""
}

variable "live_map_secret_scope" {
  description = "Secret scope for the live map app's secrets (the CARTO basemap key)"
  type        = string
}

variable "secret_writer" {
  description = "User who puts secret values into the scope with the CLI (values never go through Terraform)"
  type        = string
}
