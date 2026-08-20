-- Migration: store GTFS trips.trip_short_name.
--
-- Why: this reopens per-train identity for Austria, which NOTES.md had
-- written off as structurally impossible. That conclusion rested on ÖBB's
-- live HAFAS line.fahrtNr being 0/652 filled - true, and checked - but the
-- *static* side was never examined. It turns out ÖBB's own GTFS fills
-- trip_short_name for 14690/15165 trips, with values like "RJ 658", "D 1130",
-- in the same shape as the live feed's line.name ("RJX 639", "REX 1822").
-- So there is a train number on both sides after all; it just isn't where
-- the earlier investigation looked.
--
-- HU's own trip_short_name is confirmed 0/34349 filled (that part of the
-- earlier finding stands), so HU keeps resolving via routes.name; this column
-- is simply NULL there and costs nothing.
--
-- Additive and nullable; nothing reads it until a resolver opts in.
-- Safe to re-run.

ALTER TABLE trips ADD COLUMN IF NOT EXISTS trip_short_name TEXT;

-- The AT resolver's lookup: "trips in this country whose short name carries
-- this train number".
CREATE INDEX IF NOT EXISTS idx_trips_country_short_name
    ON trips (country_code, trip_short_name);
