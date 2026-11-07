output "landing_table" {
  description = "Full name of the Zerobus landing table, used by the producer config"
  value       = databricks_sql_table.zerobus_ais_messages.id
}
