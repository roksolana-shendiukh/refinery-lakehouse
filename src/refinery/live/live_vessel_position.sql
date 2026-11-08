-- Latest position per ship for the live map.
-- Source: the Zerobus landing table (pipeline setting source_table), PositionReport messages only.
-- AUTO CDC keeps one row per MMSI; SEQUENCE BY event_ts means an older or duplicate message
-- never overwrites a newer one. Parsing follows the silver STM, so live and weekly data agree.

CREATE TEMPORARY VIEW ais_position_reports AS
SELECT
  raw_message:MetaData.MMSI::INT                                              AS mmsi,
  to_timestamp(substr(raw_message:MetaData.time_utc::STRING, 1, 26),
               'yyyy-MM-dd HH:mm:ss.SSSSSS')                                  AS event_ts,
  raw_message:Message.PositionReport.Latitude::DOUBLE                         AS latitude,
  raw_message:Message.PositionReport.Longitude::DOUBLE                        AS longitude,
  nullif(raw_message:Message.PositionReport.Sog::DECIMAL(4, 1), 102.3)        AS sog_knots,
  nullif(raw_message:Message.PositionReport.Cog::DECIMAL(4, 1), 360.0)        AS cog_degrees,
  nullif(raw_message:Message.PositionReport.TrueHeading::SMALLINT, 511)       AS heading_degrees,
  nullif(raw_message:Message.PositionReport.NavigationalStatus::SMALLINT, 15) AS nav_status,
  received_at
FROM STREAM(${source_table})
WHERE message_type = 'PositionReport';

CREATE OR REFRESH STREAMING TABLE live_vessel_position
COMMENT 'Latest AIS position per ship (one row per MMSI), for the live map';

CREATE FLOW live_vessel_position_flow AS AUTO CDC INTO live_vessel_position
FROM STREAM(ais_position_reports)
KEYS (mmsi)
SEQUENCE BY event_ts
STORED AS SCD TYPE 1;
