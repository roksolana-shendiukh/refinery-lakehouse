variable "catalog" {
  description = "Unity Catalog catalog that holds the Refinery schemas"
  type        = string
}

variable "bronze_schema" {
  description = "Schema of the Zerobus landing table (created by the bundle, not by Terraform)"
  type        = string
}

variable "silver_schema" {
  description = "Schema of the silver RAG table eia_articles (created by the bundle, not by Terraform)"
  type        = string
}

variable "quality_schema" {
  description = "Schema of the RAG quarantine table eia_articles_quarantine (created by the bundle, not by Terraform)"
  type        = string
}

variable "rag_schema" {
  description = "Schema of the RAG chunks table eia_article_chunks (created by the bundle, not by Terraform)"
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

variable "prod_ci_principal" {
  description = "Application ID of refinery-prod-ci: its live pipeline and alert read the landing table, its RAG job writes the RAG tables; empty = no grants"
  type        = string
  default     = ""
}
