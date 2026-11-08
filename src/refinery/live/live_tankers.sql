-- What the live map shows: tankers (AIS ship type 80-89) with their latest position and static data,
-- a status, the terminal zone they are in (if any) and their flag country.
-- Joins tables that already hold one row per ship, so the view stays small and cheap.
-- Positions older than 6 hours are left out: the ship has left the covered area or stopped
-- transmitting, and an old dot on a live map would mislead.
--
-- Status (pipeline setting stationary_speed_knots):
--   at_berth  navigational status 5 (moored)
--   waiting   navigational status 1 (at anchor), or slower than stationary_speed_knots
--   underway  everything else, including ships that do not report speed
-- Zone: the terminal zone polygon that contains the position. Zones can touch, so a point on a
-- shared edge goes to the smaller zone. Inbound: destination contains "US" + the zone's port code
-- (UN/LOCODE style, e.g. "USHOU"), and the ship is underway outside that zone. AIS destinations are
-- free text typed by the crew, so "inbound" is a best-effort count, not a complete one.

CREATE VIEW live_tankers
COMMENT 'Tankers with their latest AIS position, status, terminal zone and flag, positions from the last 6 hours, for the live map'
AS
WITH tankers AS (
  SELECT
    p.mmsi,
    i.imo,
    coalesce(i.vessel_name, 'Unknown')                             AS vessel_name,
    i.ship_type,
    i.length_m,
    i.draught_m,
    i.destination,
    f.country_name                                                 AS flag_country,
    p.latitude,
    p.longitude,
    p.sog_knots,
    p.cog_degrees,
    p.heading_degrees,
    p.nav_status,
    CASE
      WHEN p.nav_status = 5 THEN 'at_berth'
      WHEN p.nav_status = 1 OR p.sog_knots < ${stationary_speed_knots} THEN 'waiting'
      ELSE 'underway'
    END                                                            AS status,
    p.event_ts                                                     AS position_at,
    timestampdiff(MINUTE, p.event_ts, current_timestamp())         AS minutes_since_update
  FROM live_vessel_position AS p
  JOIN live_vessel_info AS i
    ON i.mmsi = p.mmsi
  LEFT JOIN live_ref_mid AS f
    ON f.mid = CAST(div(p.mmsi, 1000000) AS SMALLINT)
  WHERE i.ship_type BETWEEN 80 AND 89
    AND p.event_ts >= current_timestamp() - INTERVAL 6 HOURS
    AND p.latitude IS NOT NULL
    AND p.longitude IS NOT NULL
),
zone_match AS (
  SELECT
    t.*,
    z.port_bk     AS zone_port_bk,
    z.port_name   AS zone_name,
    z.coast       AS zone_coast
  FROM tankers AS t
  LEFT JOIN live_terminal_zone AS z
    ON st_contains(st_geomfromwkt(z.boundary_wkt), st_point(t.longitude, t.latitude))
  QUALIFY row_number() OVER (
    PARTITION BY t.mmsi
    ORDER BY st_area(st_geomfromwkt(z.boundary_wkt)) ASC NULLS LAST
  ) = 1
)
SELECT
  m.*,
  inbound.port_bk AS inbound_port_bk
FROM zone_match AS m
LEFT JOIN live_terminal_zone AS inbound
  ON m.status = 'underway'
 AND m.zone_port_bk IS DISTINCT FROM inbound.port_bk
 AND contains(regexp_replace(upper(m.destination), '[^A-Z]', ''), concat('US', inbound.port_bk))
-- One row per ship, also when a destination names two ports.
QUALIFY row_number() OVER (PARTITION BY m.mmsi ORDER BY inbound.port_bk ASC NULLS LAST) = 1;
