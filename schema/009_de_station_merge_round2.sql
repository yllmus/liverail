-- Second DE station merge: the re-import that attached stop_times to Germany
-- re-split 91 stations that 007 had just merged.
--
-- WHY THIS EXISTS AT ALL, given 007. 007 merged 7,633 duplicate pairs and
-- adopted the then-current gtfs_stop_id onto each kept row, specifically so
-- the poller would stop re-creating duplicates. That worked. What it could
-- not anticipate is that gtfs.de reissues stop_ids again on later exports:
-- importing gtfs_de_fv/ and gtfs_de_rv/ (with --with-stop-times, which is
-- what this whole pass was for) presented 91 stations under an id the DB had
-- never seen, and UNIQUE (country_code, gtfs_stop_id) means "new id" is
-- indistinguishable from "new station" - so 91 second rows were INSERTed,
-- exactly as in 007. This is not a bug in either import; it is the standing
-- cost of a feed whose only stable identity is (name, lat, lon).
--
-- WHICH ROW IS CANONICAL, and why the answer differs from 007's. In 007 the
-- old row held every stop_times row, so keeping it left stop_times untouched.
-- That is no longer true: measured before writing this, the 91 old rows hold
-- 13,507 stop_times and the 91 new rows hold 15,961 - the fresh import wrote
-- against whichever row its stop_id resolved to. So stop_times has to be
-- moved either way, and the tiebreak falls to history: the old rows hold
-- 10,595 delay_observations against the new rows' 139. Keeping the old row
-- moves 139 observation rows instead of 10,595, and keeps every DE station
-- page continuous across the cutover rather than restarting it.
--
-- WHICH gtfs_stop_id TO ADOPT. Checked against the stops.txt files actually
-- on disk - the same files germany_poller's load_parent_map() reads - rather
-- than assumed from the id ordering: in 86 of 91 groups only the NEW row's id
-- appears in today's feed. In the remaining 5 both ids appear, but the old
-- one has been RECYCLED onto an unrelated station - e.g. "Cheb-Skalka" is
-- stop 305256 at 50.074/12.348, while its old id 587771 is now a child stop
-- in Berlin at 52.530/13.299. Recycled ids make the old value actively wrong
-- to keep, not merely stale, so all 91 adopt the new id.
--
-- Moving station_id in stop_times cannot violate its UNIQUE (trip_id,
-- stop_sequence) - that key does not include station_id.
--
-- Idempotent: re-running finds no (name, lat, lon) group with two rows.

BEGIN;

CREATE TEMP TABLE de_merge2 AS
SELECT min(id) AS keep_id,
       max(id) AS drop_id,
       (array_agg(gtfs_stop_id ORDER BY id DESC))[1] AS current_gtfs_stop_id
FROM stations
WHERE country_code = 'DE'
GROUP BY name, lat, lon
HAVING count(*) > 1;

-- Guard: this merge assumes each group is an exact pair. A group of three
-- would silently lose its middle row, so refuse rather than half-merge.
DO $$
DECLARE n integer;
BEGIN
    SELECT count(*) INTO n FROM (
        SELECT 1 FROM stations WHERE country_code = 'DE'
        GROUP BY name, lat, lon HAVING count(*) > 2) x;
    IF n > 0 THEN
        RAISE EXCEPTION 'Expected only pairs, found % group(s) with 3+ rows', n;
    END IF;
END $$;

-- 1. Fold both child tables onto the canonical row.
UPDATE stop_times st SET station_id = m.keep_id
FROM de_merge2 m WHERE st.station_id = m.drop_id;

UPDATE delay_observations o SET station_id = m.keep_id
FROM de_merge2 m WHERE o.station_id = m.drop_id;

-- 2. Drop the duplicates before step 3: UNIQUE (country_code, gtfs_stop_id)
--    would otherwise reject moving the id onto the kept row.
DELETE FROM stations s USING de_merge2 m WHERE s.id = m.drop_id;

-- 3. Adopt the id today's feed publishes, so the next poll resolves to the
--    canonical row instead of re-creating the duplicate this just removed.
UPDATE stations s SET gtfs_stop_id = m.current_gtfs_stop_id
FROM de_merge2 m WHERE s.id = m.keep_id;

COMMIT;
