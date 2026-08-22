-- Phase 1 of connection-risk work: give every observation a stop identity.
--
-- Until now a delay_observations row said "this train was N minutes late at
-- this station" and nothing more. Everything downstream of connection risk
-- needs three things it could not answer:
--
--   WHICH STOP of the trip this was. A train calls at a station once, usually
--   - but the buffer before a transfer is a property of a position in a
--   journey, not of a station. Recoverable today only by joining
--   trip_id + station_id into stop_times, which fails wherever trip_id is
--   null (AT 21.9%, HU 67.8% of rows carry one).
--
--   WHAT TIME IT WAS DUE. Delay distributions must bucket by the timetable
--   slot, not by when the poller happened to fire. A 07:12 commuter train and
--   a 23:12 last service are different populations and the observation
--   timestamp cannot separate them - the poller samples both trains all day.
--
--   ARRIVAL vs DEPARTURE. These are different numbers and a transfer needs
--   both: the feeder's ARRIVAL delay and the connector's DEPARTURE delay.
--   Every one of the four feeds publishes them separately and every one of
--   the four pollers was collapsing them into one figure with
--   `arrival_delay or departure_delay`. Checked directly, not assumed:
--     HU  OTP  prevOrCurrentStop { arrivalDelay departureDelay }
--     AT  HAFAS stopover.arrivalDelay / .departureDelay
--     DE  GTFS-RT stu.arrival.delay / stu.departure.delay
--     PL  GTFS-RT stu.arrival.time / .departure.time (vs scheduled)
--   delay_minutes is NOT touched or redefined - it stays exactly what it has
--   always been, so every existing query, index and aggregate is unaffected.
--   The two new columns are additive and nullable.
--
-- NOTE ON stop_sequence. This is the GTFS stop_sequence from OUR stop_times,
-- i.e. the value that joins on (trip_id, stop_sequence). It is deliberately
-- NOT the position index a live feed reports. MAV's OTP exposes
-- prevOrCurrentStop.stopPosition, which is tempting and wrong: HU's static
-- stop_sequence values are sparse, running to 99,990, and only 56 of 2,000
-- sampled HU trips number their stops consecutively at all - so OTP's
-- pattern index and our stop_sequence are different numbering schemes.
-- Writing one into the other is precisely the mistake that produced PL's
-- 20-hour phantom delays (see NOTES.md, MAX_PLAUSIBLE_DELAY_MIN).
--
-- NOTE ON scheduled_time. Stored as the raw GTFS wall-clock string
-- ("HH:MM:SS", which legitimately exceeds 24:00:00 for services continuing
-- past midnight), NOT as a timestamp. Converting to an absolute instant
-- needs the trip's service date, which only PL's feed actually publishes;
-- for the other three it would have to be guessed from observed_date, and
-- that guess is wrong for exactly the overnight trains where it matters
-- most. Storing the honest string keeps the ambiguity visible to whoever
-- converts it, instead of baking a silent off-by-one-day into 15M rows.

ALTER TABLE delay_observations
    ADD COLUMN IF NOT EXISTS stop_sequence            integer,
    ADD COLUMN IF NOT EXISTS scheduled_arrival        text,
    ADD COLUMN IF NOT EXISTS scheduled_departure      text,
    ADD COLUMN IF NOT EXISTS delay_arrival_minutes    double precision,
    ADD COLUMN IF NOT EXISTS delay_departure_minutes  double precision;

-- Phase 2 reads "every observation of this trip at this stop", and the
-- backfill below writes against the same shape. Partial, because a row with
-- no stop_sequence is exactly the row this index can never help.
CREATE INDEX IF NOT EXISTS idx_delay_obs_trip_stop
    ON delay_observations (country_code, trip_id, stop_sequence)
    WHERE stop_sequence IS NOT NULL;

GRANT ALL ON delay_observations TO trainuser;
