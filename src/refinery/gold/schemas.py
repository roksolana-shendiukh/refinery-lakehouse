"""Gold table schemas as DDL strings, column for column as the STM (Silver -> Gold) says."""

DIM_STATUS_DDL = "status_sk BIGINT NOT NULL, price_type STRING, value_status STRING"

DIM_DATE_DDL = (
    "date_sk INT NOT NULL, date DATE, month_number TINYINT, month_name STRING, quarter TINYINT, year SMALLINT"
)
