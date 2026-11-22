-- Proactive delay alerts, delivered by Web Push.
--
-- WHY WEB PUSH AND NOT EMAIL. Email needs SMTP credentials this box does not
-- have, and an alert that arrives in an inbox twenty minutes later is not an
-- alert. Web Push needs no mail server, no account, and no password: the
-- browser's push subscription IS the identity. That matters here because
-- `users` and `saved_trains` exist but hold zero rows - they were built for a
-- login flow nobody has written, and requiring one would mean nobody ever
-- receives an alert.
--
-- WHY NOT REUSE saved_trains. It is NOT NULL on user_id. Watching a train from
-- a browser with no account cannot populate that column, and loosening it
-- would leave a table that means two different things depending on the row.
--
-- WHAT A WATCH IS KEYED ON, and why it differs per country. The same per-train
-- identity /train/{country}/{id} already uses:
--     HU, AT   live_trip_id  - the raw feed id, opaque, not joinable
--     PL, DE   gtfs_trip_id  - joins to trips, a real schedule identity
-- Deliberately not normalised into one scheme: that would be the wrong
-- abstraction, and it would fail anyway, because HAFAS's tripId is a composite
-- internal key that cannot be made into a GTFS id.
--
-- WATCHES EXPIRE, AND MUST. A train id is not a train forever:
--   * gtfs.de reissues trip ids per service day - 0 of 228,547 DE
--     trip-station combinations appear on 8+ days (docs/delay-forecasting.md).
--   * 931 of 5,328 HU live_trip_ids have carried more than one route_name,
--     i.e. MÁV's OTP recycles them onto different trains.
-- A watch that outlived its journey would therefore alert about a DIFFERENT
-- train wearing the same id. expires_at defaults to 24 hours and the checker
-- refuses to act on an expired row, so the failure mode is a missed alert
-- rather than a confidently wrong one.

CREATE TABLE IF NOT EXISTS push_subscription (
    id          bigserial PRIMARY KEY,
    -- The push service's own URL for this browser. Unique because
    -- re-subscribing the same browser must update, never duplicate.
    endpoint    text        NOT NULL UNIQUE,
    p256dh      text        NOT NULL,
    auth        text        NOT NULL,
    user_agent  text,
    created_at  timestamp   NOT NULL DEFAULT now(),
    last_ok_at  timestamp,
    -- A push service answering 404/410 means the browser is gone for good.
    -- Retrying it forever wastes a request every five minutes per dead row.
    failures    integer     NOT NULL DEFAULT 0,
    disabled_at timestamp
);

CREATE TABLE IF NOT EXISTS push_watch (
    id              bigserial PRIMARY KEY,
    subscription_id bigint    NOT NULL REFERENCES push_subscription(id) ON DELETE CASCADE,
    country_code    text      NOT NULL REFERENCES countries(code),
    train_id        text      NOT NULL,
    -- What to call it in the notification. The id is unreadable in every
    -- country and a notification saying "1|#VN#1#ST#..." helps nobody.
    label           text,
    -- Optional: alert only about this stop, for someone waiting at one station
    -- rather than riding the whole way.
    station         text,
    created_at      timestamp NOT NULL DEFAULT now(),
    expires_at      timestamp NOT NULL DEFAULT now() + interval '24 hours',
    UNIQUE (subscription_id, country_code, train_id)
);

-- One row per notification actually delivered. This is what stops the checker
-- re-sending the same fact every five minutes: it compares the current state
-- against the last row here, not against nothing.
CREATE TABLE IF NOT EXISTS push_sent (
    id            bigserial PRIMARY KEY,
    watch_id      bigint    NOT NULL REFERENCES push_watch(id) ON DELETE CASCADE,
    kind          text      NOT NULL,   -- delayed | worsened | cancelled | recovered
    delay_minutes double precision,
    sent_at       timestamp NOT NULL DEFAULT now()
);

-- Not a partial index on expires_at > now(): now() is not IMMUTABLE, so
-- Postgres rejects it in a predicate. The checker filters on expiry anyway.
CREATE INDEX IF NOT EXISTS idx_push_watch_live
    ON push_watch (country_code, train_id, expires_at);
CREATE INDEX IF NOT EXISTS idx_push_sent_watch
    ON push_sent (watch_id, sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_push_sub_active
    ON push_subscription (id) WHERE disabled_at IS NULL;

GRANT ALL ON push_subscription, push_watch, push_sent TO trainuser;
GRANT ALL ON SEQUENCE push_subscription_id_seq, push_watch_id_seq, push_sent_id_seq TO trainuser;
