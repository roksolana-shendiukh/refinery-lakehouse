-- Latest static data per ship (name, type, size, destination) for the live map.
-- Source: the Zerobus landing table (pipeline setting source_table), ShipStaticData messages only.
-- AUTO CDC keeps one row per MMSI; SEQUENCE BY event_ts means an older message never overwrites a newer one.
-- IGNORE NULL UPDATES: a report with an empty field keeps the last known value instead of erasing it.
-- Parsing follows the silver STM (ais_vessel_static), so live and weekly data agree.

CREATE TEMPORARY VIEW ais_static_reports AS
SELECT
  raw_message:MetaData.MMSI::INT                                                    AS mmsi,
  to_timestamp(substr(raw_message:MetaData.time_utc::STRING, 1, 26),
               'yyyy-MM-dd HH:mm:ss.SSSSSS')                                        AS event_ts,
  nullif(raw_message:Message.ShipStaticData.ImoNumber::INT, 0)                      AS imo,
  nullif(trim(raw_message:Message.ShipStaticData.Name::STRING), '')                 AS vessel_name,
  raw_message:Message.ShipStaticData.Type::SMALLINT                                 AS ship_type,
  nullif(raw_message:Message.ShipStaticData.Dimension.A::SMALLINT
         + raw_message:Message.ShipStaticData.Dimension.B::SMALLINT, 0)             AS length_m,
  nullif(raw_message:Message.ShipStaticData.MaximumStaticDraught::DECIMAL(4, 1), 0) AS draught_m,
  nullif(trim(raw_message:Message.ShipStaticData.Destination::STRING), '')          AS destination,
  received_at
FROM STREAM(${source_table})
WHERE message_type = 'ShipStaticData';

CREATE OR REFRESH STREAMING TABLE live_vessel_info
COMMENT 'Latest AIS static data per ship (one row per MMSI), for the live map';

CREATE FLOW live_vessel_info_flow AS AUTO CDC INTO live_vessel_info
FROM STREAM(ais_static_reports)
KEYS (mmsi)
IGNORE NULL UPDATES
SEQUENCE BY event_ts
STORED AS SCD TYPE 1;
