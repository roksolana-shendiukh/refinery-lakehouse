-- One row per terminal zone for the Terminals tab and the "most congested" figure on the map:
-- how many tankers are waiting in the zone, at berth in it, and inbound to it.
-- Every zone appears, also with no tankers, so the ranking always lists the same terminals.
-- Congestion level (pipeline settings congestion_high_waiting, congestion_elevated_waiting):
--   high      waiting >= congestion_high_waiting
--   elevated  waiting >= congestion_elevated_waiting
--   normal    otherwise

CREATE VIEW live_terminal_congestion
COMMENT 'Tankers waiting, at berth and inbound per terminal zone, with a congestion level, for the live map'
AS
WITH counts AS (
  SELECT
    z.port_bk,
    z.port_name,
    z.terminal_type,
    z.coast,
    z.padd,
    count_if(t.zone_port_bk = z.port_bk AND t.status = 'waiting')   AS waiting,
    count_if(t.zone_port_bk = z.port_bk AND t.status = 'at_berth')  AS at_berth,
    count_if(t.inbound_port_bk = z.port_bk)                         AS inbound
  FROM live_terminal_zone AS z
  LEFT JOIN live_tankers AS t
    ON t.zone_port_bk = z.port_bk
    OR t.inbound_port_bk = z.port_bk
  GROUP BY z.port_bk, z.port_name, z.terminal_type, z.coast, z.padd
)
SELECT
  *,
  CASE
    WHEN waiting >= ${congestion_high_waiting} THEN 'high'
    WHEN waiting >= ${congestion_elevated_waiting} THEN 'elevated'
    ELSE 'normal'
  END AS congestion_level
FROM counts;
