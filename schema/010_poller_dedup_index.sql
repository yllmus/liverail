-- The PL/DE poller dedup lookup had no index that covered its date filter,
-- so it re-read the heap ~21,000 times every five minutes, forever.
--
-- WHAT IT COSTS TODAY. Both poland_poller.py and germany_poller.py skip an
-- INSERT when a trip's delay has not changed since the last poll (the fix that
-- took delay_observations from 54M rows to 15.3M). The lookup behind it is:
--
--     SELECT DISTINCT ON (trip_id) trip_id, delay_minutes, is_cancelled
--     FROM delay_observations
--     WHERE trip_id = ANY(%s) AND observed_date = %s
--     ORDER BY trip_id, id DESC
--
-- with ~21,000 trip ids in that array. The only usable index was
-- idx_delay_obs_trip_id, on (trip_id) alone - so observed_date could only be
-- applied as a post-filter, meaning every historical row for each of those
-- 21,000 trips was fetched from the heap and then discarded. Caught by looking
-- at pg_stat_activity while wondering why an otherwise idle box sat at 92-95%
-- iowait reading ~21 MB/s continuously: this query, mid-flight, one minute in,
-- across three parallel workers. It is the box's dominant background I/O load,
-- and it predates the work that found it.
--
-- WHY PARTIAL. trip_id is set on only 12.9% of the table (checked in
-- pg_stats, not estimated): every country wrote NULL until 2026-08-02, and
-- HU/AT still resolve only ~50-60% of vehicles. `trip_id = ANY(...)` can never
-- match a NULL, so restricting the index to non-NULL rows costs nothing and
-- makes it roughly an eighth the size - a few hundred MB instead of a few GB,
-- on a box with 1.9GB of RAM where index size decides what stays cached.
--
-- WHY THE INCLUDE. delay_minutes and is_cancelled are the only other columns
-- the query reads. Carrying them in the index turns this into an index-only
-- scan - the heap is not touched at all, which is the entire point.
--
-- id DESC matches the ORDER BY so DISTINCT ON can take the first row per
-- trip_id without a sort.
--
-- CONCURRENTLY because four pollers write to this table every five minutes and
-- a plain CREATE INDEX would lock them out for the duration. The cost is that
-- this cannot run inside a transaction block - do not wrap it in BEGIN/COMMIT,
-- and if it fails it leaves an INVALID index behind that must be dropped
-- before retrying:
--     SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
--
-- NOT FIXED HERE, deliberately: /station/{name}/board has the same shape of
-- problem (observed_date filtered but not indexed) and needs a *different*
-- index, led by station_id rather than trip_id. See docs/testing.md.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_delay_obs_trip_date
    ON delay_observations (trip_id, observed_date, id DESC)
    INCLUDE (delay_minutes, is_cancelled)
    WHERE trip_id IS NOT NULL;

-- idx_delay_obs_trip_id (trip_id) alone is left in place on purpose: it still
-- serves /train/{country}/{id}'s history lookups, which do not filter by date
-- and would not use this partial index for rows outside it.
