-- Terminal zones as the live map shows them: the reference rows plus the coast and PADD labels.
-- The zone list itself comes from live_ref_terminal_zone (generated from data/reference/ref_terminal_zone.csv).
-- Coast follows the EIA PADD of the zone: PADD 1 = East, PADD 3 = Gulf, PADD 5 = West.

CREATE VIEW live_terminal_zone
COMMENT 'Terminal zones with coast and PADD labels, for the live map'
AS
SELECT
  port_bk,
  port_name,
  terminal_type,
  area_bk,
  concat('PADD ', substr(area_bk, 2, 1))   AS padd,
  CASE substr(area_bk, 2, 1)
    WHEN '1' THEN 'East'
    WHEN '3' THEN 'Gulf'
    WHEN '5' THEN 'West'
  END                                      AS coast,
  boundary_wkt
FROM live_ref_terminal_zone;
