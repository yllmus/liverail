-- Migration: per-trip service calendar, for per-train identity resolution.
--
-- Why: HU's live feed gives a train number, which matches routes.name, but a
-- number routinely matches many candidate trips (measured 2026-08-20: 112 of
-- 272 live vehicles were ambiguous). Importing stop_times was expected to
-- break the tie by scheduled time and did not - checked against real data,
-- the candidates have *identical* scheduled times at the same station (train
-- 19778 at Tapolca: 8 candidates, all 21:12:00). They are the same physical
-- service duplicated once per calendar pattern, which is exactly what GTFS
-- service_id encodes and what we never imported.
--
-- With the calendar, "which of these trips actually runs today" is a real
-- question with a real answer: only 53 of MAV's 1036 services are active on
-- 2026-08-20.
--
-- Additive only: a nullable column plus a new table. Nothing existing reads
-- either, so this cannot affect the running pollers or API until code opts in.
-- Safe to re-run.

ALTER TABLE trips ADD COLUMN IF NOT EXISTS service_id TEXT;

CREATE TABLE IF NOT EXISTS service_calendar (
    id           SERIAL PRIMARY KEY,
    country_code TEXT NOT NULL REFERENCES countries(code),
    service_id   TEXT NOT NULL,
    monday       BOOLEAN NOT NULL DEFAULT false,
    tuesday      BOOLEAN NOT NULL DEFAULT false,
    wednesday    BOOLEAN NOT NULL DEFAULT false,
    thursday     BOOLEAN NOT NULL DEFAULT false,
    friday       BOOLEAN NOT NULL DEFAULT false,
    saturday     BOOLEAN NOT NULL DEFAULT false,
    sunday       BOOLEAN NOT NULL DEFAULT false,
    start_date   DATE,
    end_date     DATE,
    UNIQUE (country_code, service_id)
);

-- The resolver's lookup shape: "candidate trips for this country whose
-- service runs today".
CREATE INDEX IF NOT EXISTS idx_trips_country_service
    ON trips (country_code, service_id);
CREATE INDEX IF NOT EXISTS idx_service_calendar_lookup
    ON service_calendar (country_code, service_id);

-- Same ownership gap as every other table here: created as postgres, used by
-- trainuser.
GRANT ALL ON service_calendar TO trainuser;
GRANT ALL ON SEQUENCE service_calendar_id_seq TO trainuser;
