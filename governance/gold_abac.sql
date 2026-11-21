-- Gold access policies (ABAC): regional analysts see only their PADD region and the U.S. benchmark,
-- and spot (hub) prices are masked for them. Market analysts see everything.
-- Run after the gold pipeline: tags can only be set on tables that exist.
-- Parameters: :catalog, :gold_schema

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:gold_schema);

-- Tags say what a column is; the policies below match columns by tag, not by table.
ALTER MATERIALIZED VIEW fact_periodic_supply_weekly
  ALTER COLUMN area_sk SET TAGS ('refinery_access_key' = 'area');

ALTER MATERIALIZED VIEW fact_periodic_price_weekly
  ALTER COLUMN area_sk SET TAGS ('refinery_access_key' = 'area');
ALTER MATERIALIZED VIEW fact_periodic_price_weekly
  ALTER COLUMN hub_sk SET TAGS ('refinery_access_key' = 'hub');
ALTER MATERIALIZED VIEW fact_periodic_price_weekly
  ALTER COLUMN price SET TAGS ('refinery_sensitivity' = 'price');


-- Row filter: a regional analyst sees the rows of the PADD region named by their group
-- (refinery_region_<area_bk>, subregions included) and the U.S. benchmark; nothing else.
CREATE OR REPLACE FUNCTION area_rows(p_area_sk BIGINT)
RETURNS BOOLEAN
COMMENT 'TRUE when the current user may see this area: the U.S., or the region of a refinery_region_<area_bk> group they belong to'
RETURN EXISTS (
  SELECT 1
  FROM dim_area a
  LEFT JOIN dim_area p ON a.parent_area_sk = p.area_sk
  WHERE a.area_sk = p_area_sk
    AND (
      a.area_bk = 'NUS'
      OR is_account_group_member(
        concat('refinery_region_', CASE WHEN a.area_level = 'subregion' THEN p.area_bk ELSE a.area_bk END)
      )
    )
);


-- Column mask: retail prices stay visible (question 6: my region vs the U.S.), spot prices at hubs are hidden.
-- A retail price has hub_sk = -2 (Not applicable) in the fact; any other hub, known or not, is masked.
CREATE OR REPLACE FUNCTION price_mask(p_price DECIMAL(9,3), p_hub_sk BIGINT)
RETURNS DECIMAL(9,3)
COMMENT 'Shows a retail price (hub_sk = -2) and hides a spot price at a hub; who sees every price is set by the policy'
RETURN CASE WHEN p_hub_sk = -2 THEN p_price END;


-- Policies,
-- for everyone except market analysts, who see every row and every price.
CREATE OR REPLACE POLICY area_rows
ON SCHEMA IDENTIFIER(:gold_schema)
COMMENT 'Regional analysts see their PADD region and the U.S.; market analysts see every area'
ROW FILTER area_rows
TO `account users`
EXCEPT `refinery_market_analysts`
FOR TABLES
MATCH COLUMNS has_tag_value('refinery_access_key', 'area') AS area
USING COLUMNS (area);

CREATE OR REPLACE POLICY price_mask
ON SCHEMA IDENTIFIER(:gold_schema)
COMMENT 'Spot prices at hubs are hidden from everyone except market analysts; retail prices stay visible'
COLUMN MASK price_mask
TO `account users`
EXCEPT `refinery_market_analysts`
FOR TABLES
MATCH COLUMNS
  has_tag_value('refinery_sensitivity', 'price') AS price,
  has_tag_value('refinery_access_key', 'hub') AS hub
ON COLUMN price
USING COLUMNS (hub);