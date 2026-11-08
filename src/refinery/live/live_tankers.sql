-- What the live map shows: tankers (AIS ship type 80-89) with their latest position and static data.
-- Joins two tables that already hold one row per ship, so the view stays small and cheap;
-- it does no deduplication itself. Positions older than 6 hours are left out: the ship has
-- left the covered area or stopped transmitting, and an old dot on a live map would mislead.

CREATE VIEW live_tankers
COMMENT 'Tankers with their latest AIS position and static data, positions from the last 6 hours, for the live map'
AS
SELECT
  p.mmsi,
  i.imo,
  coalesce(i.vessel_name, 'Unknown')                             AS vessel_name,
  i.ship_type,
  i.length_m,
  i.draught_m,
  i.destination,
  p.latitude,
  p.longitude,
  p.sog_knots,
  p.cog_degrees,
  p.heading_degrees,
  p.nav_status,
  p.event_ts                                                     AS position_at,
  timestampdiff(MINUTE, p.event_ts, current_timestamp())         AS minutes_since_update
FROM live_vessel_position AS p
JOIN live_vessel_info AS i
  ON i.mmsi = p.mmsi
WHERE i.ship_type BETWEEN 80 AND 89
  AND p.event_ts >= current_timestamp() - INTERVAL 6 HOURS;
