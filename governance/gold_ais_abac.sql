-- Gold AIS access tags: the port-call fact joins the gold ABAC policies by tag (policies are in gold_abac.sql).
-- PROD only: the fact exists only there. Run after the refinery-ais-gold pipeline: tags can only be set on tables that exist.
-- Parameters: :catalog, :gold_schema

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:gold_schema);

ALTER MATERIALIZED VIEW fact_accumulating_port_call
  ALTER COLUMN area_sk SET TAGS ('refinery_access_key' = 'area');