-- Poland's feed has no calendar.txt at all, so nothing knew which of its
-- 21,535 trips run on a given day.
--
-- schema/002 added service_calendar from calendar.txt - weekly patterns plus a
-- validity window - and that covers HU (1,036 services) and AT (1,340). It
-- silently covered nothing for Poland, because mkuran.pl's polish_trains feed
-- ships **calendar_dates.txt only**: every service day is listed explicitly as
-- an exception_type=1 row, 7,557 of them, with no weekly pattern anywhere.
-- That is valid GTFS - calendar.txt is optional when calendar_dates.txt fully
-- enumerates the dates - and it is why import.py's calendar step, which looks
-- for calendar.txt and skips quietly when absent, left PL with zero rows.
--
-- Discovered by building the Polish journey planner: the connection scan found
-- no running services on any date and returned nothing, for every query.
--
-- WHY A SEPARATE TABLE rather than expanding the dates into service_calendar.
-- The two describe genuinely different things and GTFS treats them as
-- complementary, not alternative: a service can have BOTH a weekly pattern and
-- exceptions that add or remove individual dates (a holiday timetable, an
-- engineering weekend). Flattening one into the other would lose the ability
-- to express "runs every Tuesday EXCEPT this one". Keeping them apart means a
-- caller answers "does this service run on date D" as:
--     a calendar row matching D's weekday and inside its validity window,
--     OR  an exception row for D with added = true,
--     AND NOT an exception row for D with added = false
-- which is exactly what the specification says.
--
-- exception_type in GTFS is 1 = service added, 2 = service removed; stored as
-- a boolean because there are only ever those two, and `added` reads better at
-- the call site than `exception_type = 1`.

CREATE TABLE IF NOT EXISTS service_date (
    id           bigserial PRIMARY KEY,
    country_code text    NOT NULL REFERENCES countries(code),
    service_id   text    NOT NULL,
    service_date date    NOT NULL,
    added        boolean NOT NULL,
    UNIQUE (country_code, service_id, service_date)
);

-- The planner asks "which services run on this date" for one country, which is
-- this index exactly. Partial on `added` is not worth it: removals are a tiny
-- minority and the caller needs both.
CREATE INDEX IF NOT EXISTS idx_service_date_lookup
    ON service_date (country_code, service_date, added);

GRANT ALL ON service_date TO trainuser;
GRANT ALL ON SEQUENCE service_date_id_seq TO trainuser;
