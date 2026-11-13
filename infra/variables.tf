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

variable "prod_ci_principal" {
  description = "Application ID of refinery-prod-ci: its live pipeline and alert read the landing table; empty = no grant"
  type        = string
  default     = ""
}
