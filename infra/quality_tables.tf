# History of the DQX summary metrics, appended by the save_dq_metrics task of the weekly EIA job
# (not by a pipeline), so Terraform owns it. Same columns as the pipeline view
# quality.dq_summary_metrics_latest, which the task appends as it is.

resource "databricks_sql_table" "dq_summary_metrics" {
  catalog_name       = var.catalog
  schema_name        = var.quality_schema
  name               = "dq_summary_metrics"
  table_type         = "MANAGED"
  data_source_format = "DELTA"
  warehouse_id       = var.warehouse_id
  comment            = "DQX summary metrics of every silver run, one row per metric and table. Appended by the save_dq_metrics task of refinery-eia-weekly."

  column {
    name    = "run_id"
    type    = "string"
    comment = "One silver pipeline update; the same for every table of the run"
  }

  column {
    name    = "run_name"
    type    = "string"
    comment = "Name of the metrics observer"
  }

  column {
    name    = "input_location"
    type    = "string"
    comment = "Checked silver table"
  }

  column {
    name    = "output_location"
    type    = "string"
    comment = "Output table of the checks, if any"
  }

  column {
    name    = "quarantine_location"
    type    = "string"
    comment = "Quarantine table of the checked table"
  }

  column {
    name    = "checks_location"
    type    = "string"
    comment = "Where the checks are stored, if anywhere"
  }

  column {
    name    = "rule_set_fingerprint"
    type    = "string"
    comment = "Fingerprint of the applied checks"
  }

  column {
    name    = "metric_name"
    type    = "string"
    comment = "input_row_count, error_row_count, warning_row_count, valid_row_count or check_metrics"
  }

  column {
    name    = "metric_value"
    type    = "string"
    comment = "Value of the metric, as text (JSON for check_metrics)"
  }

  column {
    name    = "run_time"
    type    = "timestamp"
    comment = "When the metrics were computed"
  }

  column {
    name    = "error_column_name"
    type    = "string"
    comment = "DQX result column with errors"
  }

  column {
    name    = "warning_column_name"
    type    = "string"
    comment = "DQX result column with warnings"
  }

  column {
    name    = "user_metadata"
    type    = "map<string,string>"
    comment = "Extra metadata of the run"
  }

  lifecycle {
    prevent_destroy = true # the history cannot be rebuilt
  }
}
