# Zerobus landing table for live AIS messages (schema-on-read)

resource "databricks_sql_table" "zerobus_ais_messages" {
  catalog_name       = var.catalog
  schema_name        = var.bronze_schema
  name               = "zerobus_ais_messages"
  table_type         = "MANAGED"
  data_source_format = "DELTA"
  warehouse_id       = var.warehouse_id
  cluster_keys       = ["message_type", "received_at"]
  comment            = "Zerobus landing table for live AIS messages. Written only by the AIS producer; never recreate or rename it."

  column {
    name     = "received_at"
    type     = "timestamp"
    nullable = false
    comment  = "When the producer received the message from the AIS websocket (UTC)"
  }

  column {
    name     = "message_type"
    type     = "string"
    nullable = false
    comment  = "AIS message type: PositionReport or ShipStaticData"
  }

  column {
    name     = "raw_message"
    type     = "variant"
    nullable = false
    comment  = "The whole AIS message, unchanged"
  }

  lifecycle {
    prevent_destroy = true
  }
}
