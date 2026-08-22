-- Track the resolve ratio over time, so DECAY is visible, not just collapse.
--
-- monitor.py already alerts when a country's resolve ratio falls below a fixed
-- floor, and that floor would have caught Germany's 2026-08-08..19 outage,
-- where resolution fell to 6 rows in 5,724 (0.1%) for twelve days. It did not,
-- only because monitor.py was first committed on 2026-08-20 - the day the
-- outage ended.
--
-- A fixed floor cannot catch the other shape of this failure, and Poland is
-- the proof: its resolve ratio sat at 70.9% for roughly three weeks against a
-- floor of 50%, so nothing fired, while 29% of Polish trains went unrecorded.
-- The cause was a static import three weeks stale. Both incidents are the same
-- underlying fault - a static feed drifting away from the live one - and a
-- fixed threshold only sees the violent version.
--
-- So the ratio is recorded per country per run, and the check compares today
-- against this country's OWN trailing median. A ratio that is normal for
-- Germany (13.6%, because the feed is mostly bus and tram the rail-only
-- import excludes by design) is a crisis for Poland, and neither number means
-- anything except relative to itself.

CREATE TABLE IF NOT EXISTS resolve_history (
    id            bigserial PRIMARY KEY,
    country_code  text      NOT NULL REFERENCES countries(code),
    measured_at   timestamp NOT NULL DEFAULT now(),
    feed_rows     integer   NOT NULL,
    resolved_rows integer   NOT NULL,
    ratio         double precision NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_resolve_history_lookup
    ON resolve_history (country_code, measured_at DESC);

GRANT ALL ON resolve_history TO trainuser;
GRANT ALL ON SEQUENCE resolve_history_id_seq TO trainuser;
