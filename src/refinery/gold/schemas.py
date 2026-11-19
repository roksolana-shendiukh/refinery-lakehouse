"""Gold table schemas as DDL strings, column for column as the STM (Silver -> Gold) says."""

DIM_STATUS_DDL = "status_sk BIGINT NOT NULL, price_type STRING, value_status STRING"

DIM_DATE_DDL = (
    "date_sk INT NOT NULL, date DATE, month_number TINYINT, month_name STRING, quarter TINYINT, year SMALLINT"
)

DIM_HUB_DDL = "hub_sk BIGINT NOT NULL, hub_bk STRING, hub_name STRING, served_area_bk STRING"

DIM_PRODUCT_DDL = (
    "product_sk BIGINT NOT NULL, product_bk STRING, product_name STRING, product_family STRING, "
    "unit STRING, representative_grade_bk STRING"
)

DIM_FUEL_GRADE_DDL = "grade_sk BIGINT NOT NULL, grade_bk STRING, grade_name STRING, source_unit STRING"
