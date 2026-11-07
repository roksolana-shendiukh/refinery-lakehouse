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
