-- Phase 2 of the connection-risk plan: precomputed delay distributions.
--
-- Connection risk is P(feeder arrival delay - connector departure delay <=
-- buffer - interchange time). Both delay terms are DISTRIBUTIONS, not
-- averages: a train usually 2 minutes late but occasionally 40 behaves
-- completely differently at a 10-minute transfer than one reliably 8 minutes
-- late, and a mean hides exactly that. This table holds those distributions.
--
-- WHY A TABLE AND NOT A QUERY. These are whole-table aggregates over 15.8M
-- rows. Computing one inside a request is how /routes/summary and
-- /sitemap.xml became multi-second endpoints. Refreshed from
-- the existing background warmer thread instead, exactly like search_index -
-- same pattern, same reasoning, same thread.
--
-- THE FALLBACK LADDER. A specific train at a specific station is the figure
-- you actually want, and the one least likely to have enough samples. So
-- three rungs are stored, coarsest always populated:
--     scope='trip'    one scheduled train at one station   (best, thinnest)
--     scope='route'   any train on that route at that station
--     scope='station' everything calling at that station   (worst, deepest)
-- A lookup walks down until a rung clears the sample minimum, and returns
-- WHICH rung answered. A figure whose provenance is invisible is worse than
-- no figure - the caller has to be able to say "based on this exact train"
-- versus "based on the station generally".
--
-- SAMPLE COUNT IS STORED WITH EVERY ROW, and callers must honour it. A
-- probability computed from four observations is a lie with a decimal point.
--
-- HOUR BAND AND WEEKDAY TYPE are in the schema and deliberately NOT populated
-- yet. The plan specifies them and they are right eventually - an 07:12
-- commuter and a 23:12 last service are different populations. But AT, PL and
-- DE hold ~21 days of history, and splitting those into 24 hour-bands x 2
-- weekday-types would take cells that are already thin and make them empty.
-- Adding the columns now means no migration later; populating them now would
-- manufacture precision that the data cannot support. Revisit when the
-- per-cell counts justify it, not on a date.
--
-- WHICH DELAY COLUMN. delay_minutes, not the Phase 1 split columns, because
-- depth is the binding constraint: delay_minutes covers the whole archive
-- while delay_arrival_minutes/delay_departure_minutes only exist from
-- 2026-08-21. The plan's own accepted simplification is to read a train's
-- delay at a station as its departure delay there. The split columns are
-- aggregated alongside where present, so this table improves on its own as
-- Phase 1's data accumulates.

CREATE TABLE IF NOT EXISTS delay_distribution (
    id             bigserial PRIMARY KEY,
    country_code   text    NOT NULL REFERENCES countries(code),
    -- 'trip' | 'route' | 'station' - the ladder rung this row answers.
    scope          text    NOT NULL,
    -- trips.id as text for 'trip', route_name for 'route', '' for 'station'.
    -- Text rather than a union of typed columns to keep one uniqueness key.
    scope_key      text    NOT NULL,
    station_id     integer NOT NULL REFERENCES stations(id),
    -- Both NULL for now; see the note above.
    hour_band      smallint,
    weekday_type   text,

    sample_count   bigint  NOT NULL,
    days_seen      integer NOT NULL,
    p50            double precision,
    p75            double precision,
    p90            double precision,
    p95            double precision,
    worst          double precision,
    -- From Phase 1's split columns where present, NULL where the feed does not
    -- publish them (AT never publishes arrival delays at all - structural, not
    -- a thin sample).
    arr_p50        double precision,
    arr_p90        double precision,
    dep_p50        double precision,
    dep_p90        double precision,
    arr_samples    bigint  NOT NULL DEFAULT 0,
    dep_samples    bigint  NOT NULL DEFAULT 0,

    cancellation_rate double precision NOT NULL DEFAULT 0,
    refreshed_at   timestamp NOT NULL DEFAULT now(),

    UNIQUE (country_code, scope, scope_key, station_id, hour_band, weekday_type)
);

-- The lookup walks the ladder for one station, so station leads. Country is
-- second because every query filters it and it keeps the rungs of one station
-- contiguous.
CREATE INDEX IF NOT EXISTS idx_delay_dist_lookup
    ON delay_distribution (station_id, country_code, scope, scope_key);

-- Used by the refresh to expire rows for a country in one pass.
CREATE INDEX IF NOT EXISTS idx_delay_dist_country
    ON delay_distribution (country_code, refreshed_at);

GRANT ALL ON delay_distribution TO trainuser;
GRANT ALL ON SEQUENCE delay_distribution_id_seq TO trainuser;
