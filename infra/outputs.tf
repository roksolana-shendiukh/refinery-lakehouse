output "landing_table" {
  description = "Full name of the Zerobus landing table, used by the producer config"
  value       = databricks_sql_table.zerobus_ais_messages.id
}

output "live_map_secret_scope" {
  description = "Secret scope that holds the CARTO basemap key (key name: carto-api-key)"
  value       = databricks_secret_scope.live_map.name
}
