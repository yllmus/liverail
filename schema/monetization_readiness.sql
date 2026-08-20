-- Additive schema for the three monetization paths (affiliate sales,
-- compensation-claim referral, freemium tracking). Deliberately schema
-- only - no API endpoints, no auth logic, no payment integration wired up
-- yet (see MONETIZATION_READINESS.md for why that's a separate, later,
-- more carefully-reviewed pass, especially for anything touching
-- passwords/payments). Every table here is new; nothing here alters or
-- reads delay_observations/stations/routes/trips, so this is safe to run
-- against the live DB with zero risk to existing functionality.
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    tier TEXT NOT NULL DEFAULT 'free',  -- 'free' | 'premium' - freemium gate
    stripe_customer_id TEXT,            -- populated once payments exist
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Freemium: "track my train/route" watchlist. A free-tier cap on row count
-- per user_id is exactly how a freemium limit ("track up to 3 trains free")
-- gets enforced later - the table itself doesn't encode the limit.
CREATE TABLE IF NOT EXISTS saved_trains (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    country_code TEXT NOT NULL REFERENCES countries(code),
    identifier TEXT NOT NULL,  -- route_name or a train number, not a hard FK -
                                -- HU/AT don't have a resolvable trip_id for
                                -- every train (see NOTES.md "Live per-train
                                -- identity"), so this stays a display string,
                                -- same convention delay_observations.route_name
                                -- already uses.
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, country_code, identifier)
);
CREATE INDEX IF NOT EXISTS idx_saved_trains_user ON saved_trains(user_id);

-- Affiliate sales + compensation-claim partnerships share one shape: a
-- sponsored/partner link scoped to a station, a route, or the whole site.
-- kind distinguishes "book a ticket" (affiliate) from "file a compensation
-- claim" (referral) placements so the frontend can render them differently
-- without needing two separate tables.
CREATE TABLE IF NOT EXISTS partner_links (
    id SERIAL PRIMARY KEY,
    country_code TEXT REFERENCES countries(code),  -- NULL = global/all countries
    scope TEXT NOT NULL,       -- 'station' | 'route' | 'global'
    scope_key TEXT,            -- station/route name when scope isn't 'global'
    kind TEXT NOT NULL,        -- 'affiliate_booking' | 'compensation_partner' | 'sponsor'
    label TEXT NOT NULL,
    target_url TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_partner_links_scope ON partner_links(country_code, scope, scope_key) WHERE active;

-- Click-through tracking for affiliate attribution/reporting. user_id is
-- nullable - most clicks will be anonymous (no login required to click a
-- booking link), same as any real affiliate program.
CREATE TABLE IF NOT EXISTS outbound_clicks (
    id SERIAL PRIMARY KEY,
    partner_link_id INTEGER NOT NULL REFERENCES partner_links(id),
    user_id INTEGER REFERENCES users(id),
    referer_path TEXT,
    clicked_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_outbound_clicks_link ON outbound_clicks(partner_link_id, clicked_at);

-- EU rail passenger-rights compensation thresholds, per country. Seeded
-- with placeholder/approximate values below - MUST be verified against
-- each country's actual current operator/regulatory terms (MÁV/ÖBB/PKP/DB
-- each publish their own compensation tables under EU Regulation
-- 2021/782) before this is ever surfaced to a real user as advice. This
-- table's job is to make "does this delay likely qualify for compensation"
-- a lookup instead of hardcoded logic once verified figures exist.
CREATE TABLE IF NOT EXISTS compensation_rules (
    id SERIAL PRIMARY KEY,
    country_code TEXT NOT NULL REFERENCES countries(code),
    min_delay_minutes INTEGER NOT NULL,
    refund_pct NUMERIC(5,2) NOT NULL,
    source_url TEXT,
    verified BOOLEAN NOT NULL DEFAULT false,  -- false until someone confirms
                                               -- the figure against the real
                                               -- operator/regulatory source
    notes TEXT,
    UNIQUE (country_code, min_delay_minutes)
);

GRANT ALL ON users, saved_trains, partner_links, outbound_clicks, compensation_rules TO trainuser;
GRANT ALL ON users_id_seq, saved_trains_id_seq, partner_links_id_seq, outbound_clicks_id_seq, compensation_rules_id_seq TO trainuser;
