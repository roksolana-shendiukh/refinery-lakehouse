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

DIM_AREA_DDL = (
    "area_sk BIGINT NOT NULL, area_bk STRING, area_name STRING, area_level STRING, parent_area_sk BIGINT, "
    "country_name STRING, region_name STRING, subregion_name STRING, is_coastal BOOLEAN"
)

DIM_TIME_DDL = "time_sk INT NOT NULL, time STRING, hour TINYINT, minute TINYINT, day_part STRING"

DIM_PORT_DDL = (
    "port_sk BIGINT NOT NULL, port_bk STRING, port_name STRING, terminal_type STRING, area_bk STRING, boundary STRING"
)

FACT_SUPPLY_DDL = (
    "date_sk INT NOT NULL, area_sk BIGINT NOT NULL, product_sk BIGINT NOT NULL, status_sk BIGINT NOT NULL, "
    "rate_kbd DECIMAL(10,1)"
)


FACT_PRICE_DDL = (
    "date_sk INT NOT NULL, grade_sk BIGINT NOT NULL, area_sk BIGINT NOT NULL, hub_sk BIGINT NOT NULL, "
    "product_sk BIGINT NOT NULL, status_sk BIGINT NOT NULL, series_id STRING, price DECIMAL(9,3)"
)
