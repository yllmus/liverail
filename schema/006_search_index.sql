-- Migration: a small, refreshable index of every searchable name, with a
-- popularity figure attached.
--
-- Two separate problems in the 2026-08-21 competitive-gap plan turn out to
-- need exactly the same table, which is why there is one rather than two:
--
--   TYPO-TOLERANT SEARCH (plan phase 3). The frontend's search is a
--   client-side substring match over the full /stations and /routes lists,
--   so "Budpest" or "Munchen" finds nothing at all. pg_trgm gives real
--   similarity ranking, but it needs something indexable to rank *over* -
--   and route names live in delay_observations.route_name, a 15M-row table
--   whose DISTINCT scan already had to be cached to keep /routes from
--   timing out (NOTES.md, Compare/Rankings bugfix). Searching that
--   directly per keystroke is not an option.
--
--   SEO SAFETY (worklist item 8). /sitemap.xml currently submits every
--   station and route regardless of whether we hold any data for it.
--   Measured before writing this: of 21,168 station pages in the sitemap,
--   10,245 have ZERO observations - DE alone contributes 6,467 empty pages
--   and 4,344 more with fewer than 20. Those are thin/doorway pages, the
--   kind search engines demote a whole domain for. Filtering them needs a
--   per-name observation count, which is the same aggregate the search
--   ranking wants.
--
-- Rows are keyed by NAME, not by station id, and that is deliberate: DE has
-- 15,731 station rows sharing only 8,013 distinct names (genuinely different
-- GTFS stop_ids - "Harras" and "Oelsnitz, Bahnhof" each exist four times),
-- and a name-keyed search would otherwise offer the same station four times
-- as four indistinguishable results. GROUP BY name collapses them, which
-- also matches how every downstream lookup already works: /station/{name}/*
-- matches on stations.name.
--
-- This is a cache, not a source of truth. Every column is derivable from
-- stations/delay_observations by re-running refresh_search_index() in
-- api.py; nothing reads it that cannot fall back. It is refreshed by the
-- existing background warmer thread rather than on request, because the
-- aggregate behind it costs ~41s for DE (measured) - the same disk-bound
-- whole-table scan class as the Rankings summaries it sits beside.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS search_index (
    id            serial PRIMARY KEY,
    country_code  text   NOT NULL REFERENCES countries(code),
    -- 'station' | 'route'. Kept as text rather than an enum to match the
    -- rest of this schema, which uses plain text discriminators throughout.
    kind          text   NOT NULL,
    -- The join key downstream endpoints expect: stations.name for a station,
    -- delay_observations.route_name for a route. Never the display label.
    name          text   NOT NULL,
    -- Rider-facing label where one exists (stations.display_name, or a
    -- route's long_name). Searched *alongside* name, never instead of it.
    display_name  text,
    -- Both of the above, folded to lowercase ASCII and concatenated - what
    -- similarity() is actually computed against. See search_text.py for why
    -- the folding happens in Python rather than via Postgres' unaccent().
    -- Without this, PL's `Kraków Główny` was unreachable by anyone typing
    -- `Krakow`: measured zero results, for the busiest station in southern
    -- Poland.
    search_text   text   NOT NULL DEFAULT '',
    observations  bigint NOT NULL DEFAULT 0,
    refreshed_at  timestamp NOT NULL DEFAULT now(),
    UNIQUE (country_code, kind, name)
);

-- Applied separately so an already-created table (this migration ran once
-- before search_text existed) picks the column up too. Idempotent.
ALTER TABLE search_index ADD COLUMN IF NOT EXISTS search_text text NOT NULL DEFAULT '';

-- A GIN trigram index is what makes similarity()/word_similarity() fast
-- enough to run per keystroke. Only search_text is indexed: name and
-- display_name are both folded into it, so indexing them separately would
-- cost three index writes per refresh to answer the same question.
CREATE INDEX IF NOT EXISTS idx_search_index_text_trgm
    ON search_index USING gin (search_text gin_trgm_ops);
-- Every query filters country first, and the sitemap additionally filters on
-- the observation threshold.
CREATE INDEX IF NOT EXISTS idx_search_index_country_kind
    ON search_index (country_code, kind, observations DESC);

GRANT ALL ON search_index TO trainuser;
GRANT ALL ON SEQUENCE search_index_id_seq TO trainuser;
