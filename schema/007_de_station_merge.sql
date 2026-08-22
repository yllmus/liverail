-- Repair: Germany's stations were split into two id namespaces, which
-- orphaned every DE stop_times row and silently halved every DE station's
-- history.
--
-- WHAT HAPPENED. The 2026-08-20 DE re-import replaced gtfs_de_fv/ and
-- gtfs_de_rv/'s stops.txt, routes.txt and trips.txt but NOT stop_times.txt
-- (confirmed on disk: those three carry an Aug-20 mtime, stop_times.txt does
-- not). gtfs.de had changed its stop_id scheme between the two downloads -
-- "Aachen, Hbf" is stop_id 437028 in the Aug-01 feed and 10337 in the Aug-20
-- one - and because `stations` is keyed UNIQUE (country_code, gtfs_stop_id),
-- the re-import INSERTED a second row for all ~7,600 stations rather than
-- updating them.
--
-- WHAT IT BROKE, measured before this migration:
--   * delay_observations.station_id -> stop_times.station_id joined for
--     7 of 195,528 DE rows (0.0%). PL and DE are supposed to be the two
--     countries where that join is 100%; DE's was dead.
--   * germany_poller's position interpolation depends on that same join and
--     had been degrading to "no estimated position" for every DE train,
--     silently, exactly as its own comment says it would.
--   * DE history split at the cutover: 1,243,698 observations on the old ids
--     (2026-08-01..21), 192,690 on the new (2026-08-20..21). A DE station
--     page showed whichever half its id happened to hit.
--
-- THE REPAIR, and why this direction. The 7,633 duplicate groups are an
-- exact 1:1 pairing on (name, lat, lon) - same name, same display_name, same
-- coordinates, genuinely the same physical station. The OLD row is kept as
-- canonical because it already holds both the observations (1.24M vs 192k)
-- and every stop_times row, so keeping it means updating 192k rows instead
-- of 1.24M and leaves stop_times untouched and correct. The NEW row's
-- gtfs_stop_id is then moved onto the kept row, because that is the id the
-- live feed and germany_poller's parent_map now use - without that the
-- poller would go straight back to creating a second row.
--
-- The 465 unpaired DE stations (237 old-only, 228 new-only) are deliberately
-- untouched: a station present in only one of the two feed versions is not a
-- duplicate.
--
-- Idempotent: re-running finds no pairs and changes nothing.

BEGIN;

-- The old/new split is a contiguous id boundary because the new rows were
-- all INSERTed by one import run. Derived here rather than hardcoded, so
-- this reads correctly if the ids ever differ: the old namespace is exactly
-- the set stop_times still references.
CREATE TEMP TABLE de_ns AS
SELECT max(st.station_id) AS max_old_id
FROM stop_times st
JOIN trips t ON t.id = st.trip_id
WHERE t.country_code = 'DE';

CREATE TEMP TABLE de_merge AS
SELECT keep.id            AS keep_id,
       drop_row.id        AS drop_id,
       drop_row.gtfs_stop_id AS current_gtfs_stop_id
FROM stations keep
JOIN stations drop_row
  ON  drop_row.country_code = 'DE'
  AND keep.country_code = 'DE'
  AND drop_row.name = keep.name
  AND drop_row.lat IS NOT DISTINCT FROM keep.lat
  AND drop_row.lon IS NOT DISTINCT FROM keep.lon
  AND keep.id     <= (SELECT max_old_id FROM de_ns)
  AND drop_row.id >  (SELECT max_old_id FROM de_ns);

-- 1. Move the newer rows' observations onto the canonical row.
UPDATE delay_observations o
SET station_id = m.keep_id
FROM de_merge m
WHERE o.station_id = m.drop_id;

-- 2. Nothing should reference the duplicates now; drop them. This must
--    happen before step 3 - UNIQUE (country_code, gtfs_stop_id) would
--    otherwise reject moving the id onto the kept row.
DELETE FROM stations s USING de_merge m WHERE s.id = m.drop_id;

-- 3. Adopt the id the live feed actually publishes today, so the poller
--    resolves to the canonical row instead of re-creating a duplicate.
UPDATE stations s
SET gtfs_stop_id = m.current_gtfs_stop_id
FROM de_merge m
WHERE s.id = m.keep_id;

COMMIT;
