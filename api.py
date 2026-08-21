from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from html import escape as _esc
from urllib.parse import quote as _urlquote
import json
import os
import psycopg2
import psycopg2.extras
import requests as req
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from austria_poller import call_radar, call_trip, parse_at_vehicles
from poland_poller import fetch_feed, parse_pl_vehicles, resolve_delays, _scheduled_epoch
from germany_poller import (
    fetch_feed as fetch_de_feed,
    parse_de_vehicles,
    resolve_delays as resolve_de_delays,
    load_parent_map,
)
from station_names import clean_display_name
from hu_route_names import clean_route_name
from config import DB_PASSWORD

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": DB_PASSWORD,
    "host": "localhost"
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

COUNTRY_ATTRIBUTION = {
    "AT": "Data: ÖBB (data.oebb.at), CC BY 4.0",
    # PL was a placeholder ("Data: PKP PLK (public sector data), via
    # mkuran.pl") from launch until 2026-08-20, flagged as unresolved in
    # NOTES.md. The real terms, fetched from the sources themselves:
    #   - mkuran.pl licenses the realtime feed this project polls
    #     (polish_trains/updates.pb) as CC BY 4.0 *plus* PKP PLK's public
    #     sector terms. CC BY requires crediting the author, which the old
    #     string did not do at all.
    #   - the static feed (polish_trains.zip) carries PKP PLK's *and*
    #     Koleje Mazowieckie's public sector terms; KM was never credited.
    #   - PKP PLK's own reuse page (bip.plk-sa.pl/ponowne-wykorzystywanie)
    #     requires stating "dokladnego zrodla, czasu wytworzenia i
    #     pozyskania informacji" - the exact source AND when the data was
    #     obtained. That last part is why /live/board returns retrieved_at
    #     and the frontend renders it next to this text.
    "PL": (
        "Schedules: PKP Polskie Linie Kolejowe S.A. and Koleje Mazowieckie, "
        "reused as public sector information under Poland's Open Data Act "
        "(11 Aug 2021). Live delays: mkuran.pl (Mikołaj Kuranowski), "
        "CC BY 4.0, derived from PKP PLK. Provided as is, without warranty."
    ),
    "DE": "Data: DELFI e.V., via gtfs.de - schedule CC BY 4.0, realtime CC BY-SA 4.0",
}

# /routes, /routes/summary, /stations/summary all aggregate over an entire
# country's delay_observations - for HU (5.66M rows) and PL (8.29M rows)
# that's genuinely I/O-bound on this box regardless of indexing (confirmed:
# even a forced index-only scan on a (country_code, route_name) index still
# took 5.3s, vs 8.3s for the planner's own chosen parallel seq scan - the
# table just doesn't fit in this box's page cache). These are all
# "historical reliability" views, not live data, so there's no reason to
# recompute on every request - cached the same way /live already is, one
# lock per (kind, country) so concurrent callers share a computation
# instead of each independently re-running an 8s+ query.
_SUMMARY_CACHE = {}  # (kind, country) -> {"data": ..., "expires": epoch}
_SUMMARY_TTL = 900  # 15min - all-time reliability barely shifts minute to minute
_SUMMARY_LOCKS = {}

# The three Rankings/Compare queries are the expensive ones (measured through
# nginx: PL 27.8s/34.1s, DE 25.4s/23.3s, AT 12.1s/9.2s cold), and a 15min TTL
# meant any visitor arriving after a quiet quarter-hour paid that in full - the
# Rankings tab just sat empty while they waited, which is what "Rankings is
# blank" actually was. These are all-time historical aggregates over millions
# of rows; they do not meaningfully change within a day, so they get a long
# TTL and a background thread that refreshes them before it expires. Result: a
# request should never compute one of these itself.
_WARM_TTL = 21600      # 6h
_WARM_INTERVAL = 18000  # 5h - comfortably inside _WARM_TTL, so no gap opens
_WARM_KINDS = ("routes", "stations", "routes_list")

def _cached_summary(kind, country, compute_fn, ttl=None):
    cache_key = (kind, country)
    cached = _SUMMARY_CACHE.get(cache_key)
    if cached is not None and time.time() < cached["expires"]:
        return cached["data"]
    lock = _SUMMARY_LOCKS.setdefault(cache_key, threading.Lock())
    with lock:
        cached = _SUMMARY_CACHE.get(cache_key)
        if cached is not None and time.time() < cached["expires"]:
            return cached["data"]
        data = compute_fn()
        expires = time.time() + (ttl if ttl is not None else _SUMMARY_TTL)
        _SUMMARY_CACHE[cache_key] = {"data": data, "expires": expires}
        return data

def _warm_summaries():
    """Precompute the Rankings/Compare aggregates for every country.

    Runs in a daemon thread so it never blocks startup or a request. Countries
    are done one at a time on purpose - this box has a single vCPU shared with
    four pollers, and firing 12 full-table aggregates at once would starve
    exactly the requests this is meant to speed up."""
    while True:
        for country in ("HU", "AT", "PL", "DE"):
            for kind, compute in (
                ("routes", lambda c=country: _routes_summary_query(c)),
                ("stations", lambda c=country: _stations_summary_query(c)),
                ("routes_list", lambda c=country: _routes_query(c)),
            ):
                try:
                    _cached_summary(kind, country, compute, ttl=_WARM_TTL)
                except Exception as exc:
                    print(f"summary warm {kind}/{country} failed: {exc}")
        time.sleep(_WARM_INTERVAL)

@app.on_event("startup")
def _start_warmer():
    threading.Thread(target=_warm_summaries, daemon=True).start()

@app.get("/countries")
def get_countries():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT code, name FROM countries ORDER BY name")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{**dict(r), "attribution": COUNTRY_ATTRIBUTION.get(r["code"])} for r in rows]

@app.get("/stations")
def get_stations(country: str = "HU"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # display_name IS NOT NULL excludes confirmed non-rail stops (see
    # station_names.py's clean_display_name) - name stays the matching key
    # for /station/{name}/... lookups, display_name is for showing to a
    # rider. Not called anywhere in the current frontend, so this response
    # shape is free to change (see /stations/summary below for the
    # endpoint that isn't).
    cur.execute("""
        SELECT DISTINCT name, display_name FROM stations
        WHERE country_code = %s AND display_name IS NOT NULL
        ORDER BY display_name
    """, (country,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"name": r["name"], "display_name": r["display_name"]} for r in rows]

@app.get("/station/{station_name:path}/delays")
def get_station_delays(station_name: str, country: str = "HU"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            COUNT(*) as total,
            ROUND(AVG(delay_minutes)::numeric, 2) as avg_delay,
            ROUND(MAX(delay_minutes)::numeric, 2) as max_delay,
            ROUND(100.0 * SUM(CASE WHEN delay_minutes > 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_late
        FROM delay_observations o
        JOIN stations s ON o.station_id = s.id
        WHERE s.name = %s AND o.country_code = %s
    """, (station_name, country))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def _routes_query(country):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT DISTINCT route_name FROM delay_observations
        WHERE route_name IS NOT NULL AND country_code = %s
        ORDER BY route_name
    """, (country,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r["route_name"] for r in rows]

@app.get("/routes")
def get_routes(country: str = "HU"):
    return _cached_summary("routes_list", country, lambda: _routes_query(country), ttl=_WARM_TTL)

def _routes_summary_query(country):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            route_name,
            COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_on_time,
            ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delay_observations
        WHERE route_name IS NOT NULL AND country_code = %s
        GROUP BY route_name
        HAVING COUNT(*) >= 5
        ORDER BY pct_on_time DESC
    """, (country,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    # route_display: friendly corridor/operator name alongside the raw
    # route_name join key, additive only - same convention as
    # stations/summary's display_name (never substituted, never reordered
    # relative to the existing route_name field index.html already keys
    # click-through off). See _route_display_map for why this is real data
    # for AT/PL and legitimately absent for DE/HU.
    route_mapping = _route_display_map(country)
    out = [dict(r) for r in rows]
    for r in out:
        r["route_display"] = _display_route(route_mapping, r["route_name"])
    return out

@app.get("/routes/summary")
def get_routes_summary(country: str = "HU"):
    return _cached_summary("routes", country, lambda: _routes_summary_query(country), ttl=_WARM_TTL)

@app.get("/route/{route_name:path}/delays")
def get_route_delays(route_name: str, country: str = "HU"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            COUNT(*) as total,
            ROUND(AVG(delay_minutes)::numeric, 2) as avg_delay,
            ROUND(MAX(delay_minutes)::numeric, 2) as max_delay,
            ROUND(100.0 * SUM(CASE WHEN delay_minutes > 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_late
        FROM delay_observations
        WHERE route_name = %s AND country_code = %s
    """, (route_name, country))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

@app.get("/route/{route_name:path}/stats")
def get_route_stats(route_name: str, country: str = "HU"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_on_time,
            ROUND(AVG(delay_minutes)::numeric, 2) as avg_delay
        FROM delay_observations
        WHERE route_name = %s AND country_code = %s
    """, (route_name, country))
    summary = cur.fetchone()
    cur.execute("""
        SELECT EXTRACT(HOUR FROM actual_arrival)::int as hour,
               COUNT(*) as total,
               ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delay_observations
        WHERE route_name = %s AND country_code = %s AND actual_arrival IS NOT NULL
        GROUP BY hour ORDER BY hour
    """, (route_name, country))
    by_hour = [dict(r) for r in cur.fetchall()]
    cur.execute("""
        SELECT EXTRACT(DOW FROM actual_arrival)::int as dow,
               COUNT(*) as total,
               ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delay_observations
        WHERE route_name = %s AND country_code = %s AND actual_arrival IS NOT NULL
        GROUP BY dow ORDER BY dow
    """, (route_name, country))
    by_dow = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return {**dict(summary), "by_hour": by_hour, "by_dow": by_dow}

@app.get("/station/{station_name:path}/stats")
def get_station_stats(station_name: str, country: str = "HU"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_on_time,
            ROUND(AVG(delay_minutes)::numeric, 2) as avg_delay
        FROM delay_observations o
        JOIN stations s ON o.station_id = s.id
        WHERE s.name = %s AND o.country_code = %s
    """, (station_name, country))
    summary = cur.fetchone()
    cur.execute("""
        SELECT EXTRACT(HOUR FROM actual_arrival)::int as hour,
               COUNT(*) as total,
               ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delay_observations o
        JOIN stations s ON o.station_id = s.id
        WHERE s.name = %s AND o.country_code = %s AND actual_arrival IS NOT NULL
        GROUP BY hour ORDER BY hour
    """, (station_name, country))
    by_hour = [dict(r) for r in cur.fetchall()]
    cur.execute("""
        SELECT EXTRACT(DOW FROM actual_arrival)::int as dow,
               COUNT(*) as total,
               ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delay_observations o
        JOIN stations s ON o.station_id = s.id
        WHERE s.name = %s AND o.country_code = %s AND actual_arrival IS NOT NULL
        GROUP BY dow ORDER BY dow
    """, (station_name, country))
    by_dow = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return {**dict(summary), "by_hour": by_hour, "by_dow": by_dow}

@app.get("/heatmap")
def get_heatmap(station: str = None, route: str = None, country: str = "HU"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if station:
        cur.execute("""
            SELECT EXTRACT(DOW FROM actual_arrival)::int as dow,
                   EXTRACT(HOUR FROM actual_arrival)::int as hour,
                   COUNT(*) as total,
                   ROUND(AVG(delay_minutes)::numeric,1) as avg_delay
            FROM delay_observations o
            JOIN stations s ON o.station_id=s.id
            WHERE s.name=%s AND o.country_code = %s AND actual_arrival IS NOT NULL
            GROUP BY dow,hour ORDER BY dow,hour
        """, (station, country))
    elif route:
        cur.execute("""
            SELECT EXTRACT(DOW FROM actual_arrival)::int as dow,
                   EXTRACT(HOUR FROM actual_arrival)::int as hour,
                   COUNT(*) as total,
                   ROUND(AVG(delay_minutes)::numeric,1) as avg_delay
            FROM delay_observations
            WHERE route_name=%s AND country_code = %s AND actual_arrival IS NOT NULL
            GROUP BY dow,hour ORDER BY dow,hour
        """, (route, country))
    else:
        cur.close(); conn.close(); return []
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]

def _stations_summary_query(country):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # station_name stays s.name (the live click-through lookup key index.html
    # passes straight into /station/{name}/stats) - display_name is added
    # additively for a rider-facing label, never substituted in. The
    # display_name IS NOT NULL filter is a real, live-facing improvement:
    # it keeps confirmed non-rail stops (e.g. DE's Bushaltestelle rows) off
    # this leaderboard if they ever cross the 20-observation threshold.
    cur.execute("""
        SELECT s.name as station_name, s.display_name,
               COUNT(*) as total,
               ROUND(100.0 * SUM(CASE WHEN o.delay_minutes <= 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_on_time,
               ROUND(AVG(o.delay_minutes)::numeric, 1) as avg_delay
        FROM delay_observations o
        JOIN stations s ON o.station_id = s.id
        WHERE o.country_code = %s AND s.display_name IS NOT NULL
        GROUP BY s.name, s.display_name
        HAVING COUNT(*) >= 20
        ORDER BY pct_on_time DESC
    """, (country,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]

@app.get("/stations/summary")
def get_stations_summary(country: str = "HU"):
    return _cached_summary("stations", country, lambda: _stations_summary_query(country), ttl=_WARM_TTL)

@app.get("/trend")
def get_trend(route: str = None, station: str = None, days: int = 30, country: str = "HU"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cutoff = datetime.now() - timedelta(days=days)
    if route:
        cur.execute("""
            SELECT DATE(actual_arrival) as day,
                   ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay,
                   COUNT(*) as total
            FROM delay_observations
            WHERE route_name = %s AND country_code = %s AND actual_arrival >= %s
            GROUP BY day ORDER BY day
        """, (route, country, cutoff))
    elif station:
        cur.execute("""
            SELECT DATE(o.actual_arrival) as day,
                   ROUND(AVG(o.delay_minutes)::numeric, 1) as avg_delay,
                   COUNT(*) as total
            FROM delay_observations o
            JOIN stations s ON o.station_id = s.id
            WHERE s.name = %s AND o.country_code = %s AND o.actual_arrival >= %s
            GROUP BY day ORDER BY day
        """, (station, country, cutoff))
    else:
        cur.close(); conn.close(); return []
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"day": str(r["day"]), "avg_delay": float(r["avg_delay"] or 0), "total": int(r["total"])} for r in rows]

GRAPHQL_URL = "https://mavplusz.hu/otp2-backend/otp/routers/default/index/graphql"
GRAPHQL_QUERY = """
{
  vehiclePositions(
    swLat: 45.7, swLon: 16.0,
    neLat: 48.6, neLon: 22.9,
    modes: [RAIL, RAIL_REPLACEMENT_BUS, SUBURBAN_RAILWAY, TRAMTRAIN]
  ) {
    lat
    lon
    trip {
      gtfsId
      tripShortName
      tripHeadsign
      route { longName }
      stoptimes { stop { name lat lon } scheduledDeparture departureDelay scheduledArrival arrivalDelay }
    }
    stopRelationship { stop { name lat lon } status }
    prevOrCurrentStop { arrivalDelay departureDelay }
  }
}
"""

def _fmt_time(sec):
    if sec is None:
        return None
    h = (sec // 3600) % 24
    m = (sec % 3600) // 60
    return f"{h:02d}:{m:02d}"

# Registry-driven /live dispatch. Each non-HU country provides a zero-arg
# fetch function that returns rows already shaped for the response; adding a
# country means writing one small _live_xx() function and one entry in
# _LIVE_PROVIDERS, not another copy-pasted cache+fetch+reshape block. HU
# stays a special case outside the registry since get_live_hu() has no
# cache/TTL (see its own definition below).
_LIVE_CACHE = {}  # country_code -> {"data": [...], "expires": epoch}

def _live_result(rows, stop_key):
    # headsign/lat/lon are populated for AT (HAFAS radar's direction/location,
    # both confirmed ~100% filled in real data) and headsign-only for PL (via
    # the trips.headsign static join in resolve_delays); DE's underlying rows
    # carry neither (no headsign in gtfs.de's static feed) so .get() falls
    # through to None for it, same as before this existed.
    #
    # position_source distinguishes AT's real GPS ("live") from PL/DE's
    # schedule-interpolated marker ("estimated") - see resolve_delays() in
    # poland_poller.py/germany_poller.py and PLAN.md for why no live GPS
    # exists for either country. Never silently blend the two - a consumer
    # must always be able to tell which kind of position a row holds.
    return [
        {
            "route_name": row["route_name"],
            "train_number": row.get("train_number") or "",
            "headsign": row.get("headsign"),
            # Whatever identifier this country's feed uses to address one
            # specific train, passed through opaquely so /train/{country}/{id}
            # can look the same train up again. Deliberately NOT normalized
            # into a shared scheme - AT's is a HAFAS composite key, PL/DE's is
            # a GTFS trip id, and pretending they're interchangeable is the
            # exact mistake documented in NOTES.md's per-train identity note.
            "train_id": row.get("live_trip_id") or row.get("gtfs_trip_id"),
            "current_stop": row[stop_key],
            "delay_minutes": row["delay_minutes"],
            "status": "IN_TRANSIT_TO",
            "stops": [],
            "stop_idx": None,
            "lat": row.get("lat"),
            "lon": row.get("lon"),
            "position_source": row.get("position_source"),
        }
        for row in rows
    ]

def _live_at():
    rows = parse_at_vehicles(call_radar())
    for row in rows:
        row["position_source"] = "live" if row.get("lat") is not None else None
    return _live_result(rows, "stop_name")

def _live_pl():
    conn = get_conn()
    cur = conn.cursor()
    try:
        rows = resolve_delays(cur, parse_pl_vehicles(fetch_feed()))
    finally:
        cur.close()
        conn.close()
    return _live_result(rows, "station_name")

def _live_de():
    # PL/DE's /live is schedule-vs-actual for whatever's currently in the
    # TripUpdates feed, not a vehicle-position snapshot the way HU/AT's
    # radar-style calls are - and DE's resolve rate is ~16% of the raw feed
    # (most of gtfs.de's realtime-free.pb is non-rail traffic outside DE's
    # rail-only static import, same rail-only scope as HU/AT/PL).
    conn = get_conn()
    cur = conn.cursor()
    try:
        rows = resolve_de_delays(cur, parse_de_vehicles(fetch_de_feed()))
    finally:
        cur.close()
        conn.close()
    return _live_result(rows, "station_name")

_LIVE_PROVIDERS = {
    # AT's radar() call used to be cheap (~1-2s), which is why 25s was fine.
    # Raising radar.js's results cap 256 -> 2000 (it was silently truncating
    # ~90% of the national feed) made the payload ~9x bigger and the cold
    # call ~13-16s, so a 25s TTL against the frontend's 30s auto-refresh
    # meant every single refresh tick paid the full cost. 60s, matching DE.
    "AT": (_live_at, 60),
    # PL/DE's fetch_fn is a fresh GTFS-RT pull + a per-row schedule join
    # (~30s for PL's ~22k trip updates, confirmed) - not cheap like AT's
    # radar() call. A 25s TTL (matched almost exactly to the frontend's old
    # 30s auto-refresh interval) meant this expensive path reran on nearly
    # every refresh tick. Longer TTL since the underlying data doesn't
    # actually move faster than our own poller samples it (every 5min).
    "PL": (_live_pl, 90),
    "DE": (_live_de, 60),
}
# One lock per non-HU provider so concurrent requests for the same country
# (board + map fire together on every tab switch and every 30s auto-refresh)
# don't independently duplicate the same ~30-90s PL/DE resolve - found while
# investigating the PL map timeout: two callers racing a cold cache each ran
# the full fetch+resolve concurrently, roughly doubling load on this box's
# single vCPU instead of one computing while the other waits and reuses it.
_LIVE_LOCKS = {country: threading.Lock() for country in _LIVE_PROVIDERS}

@app.get("/live")
def get_live(country: str = "HU"):
    if country == "HU":
        return get_live_hu()
    provider = _LIVE_PROVIDERS.get(country)
    if provider is None:
        return []
    fetch_fn, ttl = provider
    cached = _LIVE_CACHE.get(country)
    if cached is not None and time.time() < cached["expires"]:
        return cached["data"]
    with _LIVE_LOCKS[country]:
        # Re-check: another thread may have refreshed the cache while this
        # one waited on the lock.
        cached = _LIVE_CACHE.get(country)
        if cached is not None and time.time() < cached["expires"]:
            return cached["data"]
        data = fetch_fn()
        _LIVE_CACHE[country] = {"data": data, "expires": time.time() + ttl}
        return data

# name -> display_name per country, for turning the raw station strings the
# live feeds emit into rider-facing labels. Cached because it only changes on
# a GTFS re-import, and DE's map alone is ~7.8k rows.
_DISPLAY_CACHE = {}  # country_code -> {"map": {...}, "expires": epoch}
_DISPLAY_TTL = 3600

def _display_map(country):
    now = time.time()
    cached = _DISPLAY_CACHE.get(country)
    if cached is not None and now < cached["expires"]:
        return cached["map"]
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT name, display_name FROM stations
            WHERE country_code = %s AND display_name IS NOT NULL
        """, (country,))
        mapping = dict(cur.fetchall())
    finally:
        cur.close()
        conn.close()
    _DISPLAY_CACHE[country] = {"map": mapping, "expires": now + _DISPLAY_TTL}
    return mapping

def _display_stop(mapping, raw, country):
    # AT is the reason for the fallback: its live HAFAS names disagree with
    # its own GTFS names for most stations (3/40 matched in real data - the
    # documented name-matching limitation), so an unmatched name still gets
    # the same formatting rules applied rather than being shown raw.
    if not raw:
        return raw
    return mapping.get(raw) or clean_display_name(raw, country) or raw

# routes.name (short code) -> routes.long_name (friendly corridor/operator
# name), for the raw AT/PL line codes item 7 flags. Same cache shape/TTL as
# _display_map. DE's routes.long_name is confirmed 0/1061 filled (gtfs.de's
# two static feeds simply don't carry it - see schema/004_route_long_name.sql)
# so this mapping is legitimately empty for DE and every route falls through
# to raw-code-only, same as before this existed - not a bug.
_ROUTE_DISPLAY_CACHE = {}  # country_code -> {"map": {...}, "expires": epoch}

def _route_display_map(country):
    now = time.time()
    cached = _ROUTE_DISPLAY_CACHE.get(country)
    if cached is not None and now < cached["expires"]:
        return cached["map"]
    conn = get_conn()
    cur = conn.cursor()
    try:
        # A short code can legitimately belong to more than one gtfs_route_id
        # (confirmed: 10 PL / 55 AT / 187 DE names each cover multiple route
        # rows - direction/branch variants sharing one line code). Rather
        # than guess which is "the" long name, DISTINCT ON picks one
        # deterministically (lowest id) per item 7's rule: the raw code stays
        # authoritative everywhere this is used, the friendly name is best-
        # effort supplementary context, never a replacement.
        cur.execute("""
            SELECT DISTINCT ON (name) name, long_name FROM routes
            WHERE country_code = %s AND long_name IS NOT NULL
            ORDER BY name, id
        """, (country,))
        mapping = dict(cur.fetchall())
        if country == "AT":
            # AT needs a second, different join, and PL/DE must not use it.
            # AT's *live* route_name is a train identity ("NJ 456", "RJ 215"),
            # not a line code - confirmed 0/40 live names match routes.name
            # ("REX81", "S3") because they're separate namespaces, so the
            # lookup above resolves nothing at all for AT's live board. The
            # trip_short_name column imported for item 4's identity work
            # bridges them: it holds exactly those live-style train
            # identities and joins to a route. Ambiguous identities (one
            # train number reaching corridors that disagree - 263/7982,
            # 3.3%) are dropped rather than guessed, per item 7's "where
            # ambiguous, keep the raw code" rule.
            cur.execute("""
                SELECT tsn, MIN(long_name) FROM (
                    SELECT t.trip_short_name AS tsn, r.long_name
                    FROM trips t JOIN routes r ON r.id = t.route_id
                    WHERE t.country_code = %s
                      AND t.trip_short_name IS NOT NULL AND r.long_name IS NOT NULL
                    GROUP BY t.trip_short_name, r.long_name
                ) x
                GROUP BY tsn HAVING COUNT(*) = 1
            """, (country,))
            # Line-code entries stay as a fallback for anything the
            # train-identity join misses; live rows hit the identity map.
            mapping.update(dict(cur.fetchall()))
    finally:
        cur.close()
        conn.close()
    _ROUTE_DISPLAY_CACHE[country] = {"map": mapping, "expires": now + _DISPLAY_TTL}
    return mapping

def _display_route(mapping, raw):
    # Unlike _display_stop, no clean_display_name-style fallback formatting -
    # an unmapped route code just has no friendly name available (this is
    # the norm for DE, and for AT/PL codes with no static match), and there
    # is nothing to reformat a bare line code into. None here means "show
    # only the raw code", handled by the caller/frontend.
    if not raw:
        return None
    return mapping.get(raw)

@app.get("/live/board")
def get_live_board(country: str = "HU", limit: int = 300, q: str = ""):
    """Live board for the frontend: full-feed aggregates plus a capped,
    delay-ranked slice of trains.

    /live stays untouched (a bare array, whatever size the feed is). This
    exists because that shape can't drive a real UI for PL/DE: PL's feed is
    22k+ TripUpdates (a 4.8MB response), so the client needs server-side
    ranking, searching, and capping - and it needs the aggregates computed
    over the *whole* feed, not just the slice it renders.
    """
    rows = get_live(country)
    mapping = _display_map(country)
    route_mapping = _route_display_map(country)

    delays = [r["delay_minutes"] or 0 for r in rows]
    delayed = sum(1 for d in delays if d > 2)
    positioned = [r for r in rows if r.get("lat") is not None]
    sources = {}
    for r in positioned:
        key = r.get("position_source") or "unknown"
        sources[key] = sources.get(key, 0) + 1

    needle = q.strip().lower()
    if needle:
        def matches(r):
            for key in ("route_name", "current_stop", "headsign", "train_number"):
                val = r.get(key)
                if val and needle in str(val).lower():
                    return True
            return False
        rows = [r for r in rows if matches(r)]

    ranked = sorted(rows, key=lambda r: r["delay_minutes"] or 0, reverse=True)
    shown = ranked[:limit] if limit and limit > 0 else ranked

    trains = []
    for r in shown:
        t = dict(r)
        # current_stop stays the raw name - index.html passes it straight to
        # /station/{name}/stats, which matches on stations.name. Only ever
        # add the display label alongside it, never substitute it in.
        t["current_stop_display"] = _display_stop(mapping, r.get("current_stop"), country)
        # Raw operator line code (AT/PL/DE) alongside a friendly name where
        # one exists (route_name itself is left untouched - it's the join
        # key /route/{name}/* uses). None for DE and any AT/PL code with no
        # static match - see _route_display_map.
        t["route_display"] = _display_route(route_mapping, r.get("route_name"))
        if r.get("stops"):
            t["stops"] = [
                {**s, "display": _display_stop(mapping, s.get("name"), country)}
                for s in r["stops"]
            ]
        trains.append(t)

    return {
        "meta": {
            "country": country,
            "total": len(delays),
            "matched": len(ranked),
            "shown": len(trains),
            "delayed": delayed,
            "on_time_pct": round(100.0 * (len(delays) - delayed) / len(delays), 1) if delays else None,
            "avg_delay": round(sum(delays) / len(delays), 1) if delays else None,
            "worst_delay": max(delays) if delays else None,
            "with_position": len(positioned),
            "position_sources": sources,
            "attribution": COUNTRY_ATTRIBUTION.get(country),
            # PKP PLK's reuse terms require stating when the information was
            # obtained, not just its source - see COUNTRY_ATTRIBUTION. Emitted
            # for every country because it costs nothing and is honest
            # everywhere; the frontend renders it beside the attribution.
            "retrieved_at": datetime.now().isoformat(timespec="seconds"),
        },
        "trains": trains,
    }

@app.get("/live/positions")
def get_live_positions(country: str = "HU"):
    """Only the trains that actually resolved a coordinate - what the map
    needs. Separate from /live/board because the board is delay-ranked and
    capped, while the map wants every positioned train regardless of delay
    (and for PL/DE that's a small subset: 441/22353 and 1292/5533 in real
    polls, since their positions are schedule-interpolated, not GPS)."""
    mapping = _display_map(country)
    out = []
    for r in get_live(country):
        if r.get("lat") is None or r.get("lon") is None:
            continue
        out.append({
            "route_name": r["route_name"],
            "train_number": r.get("train_number") or "",
            "headsign": r.get("headsign"),
            "current_stop": r.get("current_stop"),
            "current_stop_display": _display_stop(mapping, r.get("current_stop"), country),
            "delay_minutes": r["delay_minutes"],
            "status": r.get("status"),
            "lat": r["lat"],
            "lon": r["lon"],
            "position_source": r.get("position_source"),
        })
    return out

@app.get("/live/worst")
def get_live_worst(country: str = "HU", kind: str = "route", limit: int = 10):
    """"Worst right now" - ranked by *current* delay, not the Rankings tab's
    historical reliability (see /routes/summary, /stations/summary). Item 6
    of the 2026-08-20 worklist: no rolling-window query existed for this at
    all before, and the frontend's Rankings tab/footer said so explicitly.

    Deliberately reuses the same get_live(country) snapshot /live/board
    already fetches (same cache, same TTL) rather than a new fetch or a DB
    query - this is a live-only view, computed by grouping the in-memory
    snapshot, not a persisted-history aggregate.
    """
    if kind not in ("route", "station"):
        raise HTTPException(status_code=400, detail="kind must be 'route' or 'station'")
    key = "route_name" if kind == "route" else "current_stop"
    groups = {}
    for r in get_live(country):
        name = r.get(key)
        delay = r.get("delay_minutes")
        if not name or delay is None:
            continue
        groups.setdefault(name, []).append(delay)

    mapping = _display_map(country) if kind == "station" else _route_display_map(country)
    worst = []
    for name, delays in groups.items():
        entry = {
            "name": name,
            "train_count": len(delays),
            "avg_delay": round(sum(delays) / len(delays), 1),
            "max_delay": round(max(delays), 1),
        }
        if kind == "station":
            entry["display_name"] = _display_stop(mapping, name, country)
        else:
            entry["display_name"] = _display_route(mapping, name)
        worst.append(entry)
    worst.sort(key=lambda e: e["avg_delay"], reverse=True)

    return {
        "meta": {
            "country": country,
            "kind": kind,
            "groups": len(worst),
            "attribution": COUNTRY_ATTRIBUTION.get(country),
            "retrieved_at": datetime.now().isoformat(timespec="seconds"),
        },
        "worst": worst[:limit] if limit and limit > 0 else worst,
    }

def get_live_hu():
    try:
        resp = req.post(GRAPHQL_URL, json={"query": GRAPHQL_QUERY}, timeout=20)
        vehicles = resp.json().get("data", {}).get("vehiclePositions", [])
    except Exception:
        return []

    result = []
    for v in vehicles:
        trip = v.get("trip")
        stop_rel = v.get("stopRelationship")
        prev = v.get("prevOrCurrentStop")
        if not trip or not stop_rel or not prev:
            continue
        route_name = clean_route_name(trip.get("route", {}).get("longName"), trip.get("tripShortName"))
        stop_name = stop_rel.get("stop", {}).get("name")
        delay_sec = prev.get("arrivalDelay") or prev.get("departureDelay") or 0
        if not route_name or not stop_name:
            continue

        raw_stops = trip.get("stoptimes") or []
        stops = []
        stop_idx = None
        for i, st in enumerate(raw_stops):
            stop_info = st.get("stop") or {}
            name = stop_info.get("name") or ""
            # A train doesn't depart its final stop, so MÁV reports
            # departureDelay 0 there - which rendered a 52-minutes-late train
            # as arriving "on time" at its destination, the one stop a rider
            # cares most about. Use the arrival figures for the last stop.
            is_last = i == len(raw_stops) - 1
            if is_last and st.get("arrivalDelay") is not None:
                delay_sec_stop = st.get("arrivalDelay")
                sched = st.get("scheduledArrival")
            else:
                delay_sec_stop = st.get("departureDelay") or 0
                sched = st.get("scheduledDeparture")
            # Checked against the raw feed: MÁV reports 0 for *both* arrival and
            # departure delay at a trip's final stop even when the train is
            # running an hour late (every intermediate stop carries the real
            # figure). That's the feed declining to predict the final arrival,
            # not a prediction of punctuality - so it's reported as unknown
            # rather than telling a rider on a 52-late train that it arrives on
            # time, which is the one stop they care most about.
            stop_delay_min = round(delay_sec_stop / 60, 1)
            if is_last and stop_delay_min == 0 and (delay_sec or 0) / 60 > 2:
                stop_delay_min = None
            stops.append({
                "name": name,
                "dep": _fmt_time(sched),
                "delay_min": stop_delay_min,
                # Carried so /train/HU/{id} can draw the journey on a map,
                # the same way AT's stopovers and PL/DE's stations do.
                "lat": stop_info.get("lat"),
                "lon": stop_info.get("lon"),
            })
            if name == stop_name and stop_idx is None:
                stop_idx = i

        stop_obj = stop_rel.get("stop") or {}
        lat = v.get("lat") or stop_obj.get("lat")
        lon = v.get("lon") or stop_obj.get("lon")
        result.append({
            "route_name": route_name,
            "train_number": trip.get("tripShortName") or "",
            "headsign": trip.get("tripHeadsign"),
            # See _live_result()'s train_id comment. HU's is OTP's own
            # trip.gtfsId - opaque and unjoinable to our static trips table
            # (that's why live_trip_id exists at all), but it does reliably
            # address one train within the live feed, which is all
            # /train/HU/{id} needs.
            "train_id": trip.get("gtfsId"),
            "current_stop": stop_name,
            "delay_minutes": round(delay_sec / 60, 1),
            "status": stop_rel.get("status") or "IN_TRANSIT_TO",
            "stops": stops,
            "stop_idx": stop_idx,
            "lat": lat,
            "lon": lon,
            # Real position off the live feed (either the vehicle's own
            # reported lat/lon, or its current stop's known location as a
            # fallback) - not a schedule-interpolated estimate, so "live"
            # either way. See _live_result()'s comment for why this field
            # exists: PL/DE fall back to "estimated", never silently blended.
            "position_source": "live" if lat is not None else None,
        })

    return result

@app.get("/health")
def health():
    """Last result from monitor.py, served as JSON.

    monitor.py writes the file on a schedule rather than this endpoint doing
    the checks: they hit all four upstream feeds, which is far too expensive
    to run per request, and an endpoint that only reports when polled is not
    monitoring anyway (see monitor.py's own docstring for the failures this
    exists to catch). Reading a file also means /health still answers when
    the database is the thing that's broken."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor_status.json")
    try:
        with open(path) as f:
            doc = json.load(f)
    except FileNotFoundError:
        return {"status": "unknown", "detail": "monitor.py has not run yet"}
    except Exception as exc:
        return {"status": "unknown", "detail": f"could not read status: {exc}"}
    age_min = None
    try:
        checked = datetime.fromisoformat(doc["checked_at"])
        age_min = round((datetime.now() - checked).total_seconds() / 60, 1)
    except Exception:
        pass
    # A stale status file is itself a failure - it means the monitor stopped.
    if age_min is not None and age_min > 60:
        doc = {**doc, "status": "critical",
               "detail": f"monitor.py last ran {age_min:.0f} min ago"}
    doc["age_minutes"] = age_min
    return doc

# --- One train's full journey -----------------------------------------
# /train/{country}/{id} is what turns this from a delay-statistics site into
# an actual train tracker: given one train, where is it now and what is its
# whole remaining timetable, stop by stop, scheduled vs. expected.
#
# Every country can answer this, but no two answer it the same way, and the
# differences are real rather than cosmetic (checked against each live feed
# directly before any of this was written):
#   HU  - the OTP GraphQL response already carries the full stoptimes array,
#         so get_live_hu() has been building it all along; this just finds
#         the right train in that snapshot. No extra network call.
#   AT  - radar() returns only ~4 upcoming stopovers, so the full journey
#         needs a second HAFAS call, client.trip(), one train at a time
#         (hafas/trip.js). Real GPS position and platform numbers included.
#   PL  - the GTFS-RT feed carries every stop_time_update for the trip (mean
#         17, max 77), but with no stop_id, no name, and no delay - only
#         stop_sequence + an absolute timestamp. Names and scheduled times
#         come from our own stop_times/stations tables, which is exactly
#         what poland_poller's resolve_delays() already does for the current
#         stop; this does it for all of them.
#   DE   - the feed carries stop_id, delay AND time per stop, so the journey
#         needs no schedule join at all - but its stop_ids are always
#         platform-level children, so they go through the same
#         load_parent_map() translation germany_poller already depends on.
#
# The per-country fetch is cheap enough to do on demand (measured: PL 0.5s,
# DE 0.8s for a full raw feed fetch+parse - the expensive part of /live was
# never the feed, it was the ~22k-row DB join, which this does for one trip
# only). So there is no new cache here.
_JOURNEY_ROUTE_STATS_TTL = 900

# A delay this large is not a delay, it's a schedule-version mismatch. PL's
# live feed and our imported static snapshot mostly agree on stop_sequence
# numbering (measured: 399/400 sampled trips have every feed sequence inside
# the static trip's own range), but a handful of trips - international ones
# especially - are renumbered upstream, so matching sequence-to-sequence pairs
# the wrong stops and yields ~20-hour "delays". One real example:
# PLK_LEO_2026_516260411, numbered 15-31 in the feed and 0-16 in stop_times.
# Rather than show a rider "1191 minutes late", these are reported as unknown -
# the same graceful-skip-rather-than-guess convention the pollers use for rows
# they can't resolve.
_MAX_PLAUSIBLE_DELAY_MIN = 720

def _plausible_delay(delay_min):
    if delay_min is None or abs(delay_min) > _MAX_PLAUSIBLE_DELAY_MIN:
        return None
    return delay_min

def _journey_stop(name, mapping, country, scheduled=None, expected=None,
                  delay_min=None, lat=None, lon=None, platform=None):
    return {
        "name": name,
        "display": _display_stop(mapping, name, country),
        "scheduled": scheduled,
        "expected": expected,
        "delay_min": delay_min,
        "lat": lat,
        "lon": lon,
        "platform": platform,
    }

def _epoch_to_hhmm(epoch, tz):
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, tz).strftime("%H:%M")

def _iso_to_hhmm(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).strftime("%H:%M")
    except ValueError:
        return None

def _journey_hu(train_id, mapping):
    for t in get_live_hu():
        if t.get("train_id") != train_id:
            continue
        stops = [
            _journey_stop(s["name"], mapping, "HU", scheduled=s.get("dep"),
                          delay_min=s.get("delay_min"),
                          lat=s.get("lat"), lon=s.get("lon"))
            for s in t.get("stops") or []
        ]
        # HU's feed gives a scheduled departure and a per-stop delay but no
        # expected time, so derive it rather than leaving the column blank.
        for s in stops:
            s["expected"] = _shift_hhmm(s["scheduled"], s["delay_min"])
        return {**t, "stops": stops}
    return None

def _shift_hhmm(hhmm, delay_min):
    if not hhmm or delay_min is None:
        return hhmm
    try:
        h, m = (int(x) for x in hhmm.split(":"))
    except ValueError:
        return hhmm
    total = (h * 60 + m + int(round(delay_min))) % (24 * 60)
    return f"{total // 60:02d}:{total % 60:02d}"

def _journey_at(train_id, mapping):
    live = None
    for t in get_live("AT"):
        if t.get("train_id") == train_id:
            live = t
            break
    trip = call_trip(train_id, live.get("route_name") if live else None)
    if trip is None:
        return None
    stops = []
    for s in trip.get("stopovers") or []:
        # Prefer arrival for an intermediate stop, departure for the origin
        # (which has no arrival at all) - matching what a rider reads off a
        # platform display.
        delay_sec = s.get("arrivalDelay")
        if delay_sec is None:
            delay_sec = s.get("departureDelay")
        planned = s.get("plannedArrival") or s.get("plannedDeparture")
        actual = s.get("arrival") or s.get("departure")
        stops.append(_journey_stop(
            s.get("name"), mapping, "AT",
            scheduled=_iso_to_hhmm(planned),
            expected=_iso_to_hhmm(actual) or _iso_to_hhmm(planned),
            delay_min=round(delay_sec / 60, 1) if delay_sec is not None else None,
            lat=s.get("latitude"), lon=s.get("longitude"),
            platform=s.get("platform"),
        ))
    loc = trip.get("currentLocation") or {}
    return {
        "route_name": trip.get("line") or (live or {}).get("route_name"),
        "train_number": trip.get("line") or "",
        "headsign": trip.get("direction") or trip.get("destination"),
        "train_id": train_id,
        "current_stop": (live or {}).get("current_stop"),
        "delay_minutes": (live or {}).get("delay_minutes"),
        "status": "CANCELLED" if trip.get("cancelled") else "IN_TRANSIT_TO",
        "stops": stops,
        "lat": loc.get("latitude") or (live or {}).get("lat"),
        "lon": loc.get("longitude") or (live or {}).get("lon"),
        "position_source": "live",
    }

def _journey_pl(train_id, mapping):
    feed = fetch_feed()
    update = None
    for entity in feed.entity:
        if entity.HasField("trip_update") and entity.trip_update.trip.trip_id == train_id:
            update = entity.trip_update
            break
    if update is None:
        return None
    start_date = update.trip.start_date
    tz = ZoneInfo("Europe/Warsaw")
    conn = get_conn()
    cur = conn.cursor()
    try:
        # One query for the trip's whole schedule, rather than PL's
        # per-stop-sequence lookup - this is a single trip, so the whole
        # thing is ~17 rows.
        cur.execute("""
            SELECT st.stop_sequence, s.name, st.scheduled_arrival,
                   st.scheduled_departure, s.lat, s.lon
            FROM stop_times st
            JOIN stations s ON s.id = st.station_id
            WHERE st.trip_id = (
                SELECT id FROM trips WHERE gtfs_trip_id = %s AND country_code = 'PL'
            )
            ORDER BY st.stop_sequence
        """, (train_id,))
        schedule = {row[0]: row for row in cur.fetchall()}
        cur.execute("""
            SELECT r.name, t.headsign
            FROM trips t LEFT JOIN routes r ON r.id = t.route_id
            WHERE t.gtfs_trip_id = %s AND t.country_code = 'PL'
        """, (train_id,))
        meta = cur.fetchone() or (None, None)
    finally:
        cur.close()
        conn.close()

    actual_by_seq = {}
    for stu in update.stop_time_update:
        epoch = None
        if stu.HasField("arrival") and stu.arrival.HasField("time"):
            epoch = stu.arrival.time
        elif stu.HasField("departure") and stu.departure.HasField("time"):
            epoch = stu.departure.time
        if epoch:
            actual_by_seq[stu.stop_sequence] = epoch

    stops = []
    for seq in sorted(schedule):
        _, name, sched_arr, sched_dep, lat, lon = schedule[seq]
        sched_str = sched_dep or sched_arr
        sched_epoch = _scheduled_epoch(start_date, sched_str) if sched_str else None
        actual_epoch = actual_by_seq.get(seq)
        delay = None
        if sched_epoch and actual_epoch:
            delay = _plausible_delay(round((actual_epoch - sched_epoch) / 60, 1))
        stops.append(_journey_stop(
            name, mapping, "PL",
            scheduled=_epoch_to_hhmm(sched_epoch, tz),
            # If the delay was rejected as implausible the "expected" time it
            # implies is equally wrong, so fall back to the scheduled time
            # rather than showing a confidently wrong arrival.
            expected=(_epoch_to_hhmm(actual_epoch, tz) if delay is not None
                      else _epoch_to_hhmm(sched_epoch, tz)),
            delay_min=delay, lat=lat, lon=lon,
        ))
    return {
        "route_name": meta[0], "train_number": "", "headsign": meta[1],
        "train_id": train_id, "stops": stops, "position_source": "estimated",
        # Full route: the stop list comes from our own stop_times, so it spans
        # origin to destination even where the feed only predicts part of it.
        "journey_scope": "full",
    }

def _journey_de(train_id, mapping):
    feed = fetch_de_feed()
    update = None
    for entity in feed.entity:
        if entity.HasField("trip_update") and entity.trip_update.trip.trip_id == train_id:
            update = entity.trip_update
            break
    if update is None:
        return None
    parent_map = load_parent_map()
    tz = ZoneInfo("Europe/Berlin")
    stop_ids = [
        parent_map.get(stu.stop_id, stu.stop_id)
        for stu in update.stop_time_update if stu.stop_id
    ]
    names = {}
    conn = get_conn()
    cur = conn.cursor()
    try:
        if stop_ids:
            cur.execute("""
                SELECT gtfs_stop_id, name, lat, lon FROM stations
                WHERE country_code = 'DE' AND gtfs_stop_id = ANY(%s)
            """, (stop_ids,))
            names = {row[0]: row[1:] for row in cur.fetchall()}
        cur.execute("""
            SELECT r.name, t.headsign
            FROM trips t LEFT JOIN routes r ON r.id = t.route_id
            WHERE t.gtfs_trip_id = %s AND t.country_code = 'DE'
        """, (train_id,))
        meta = cur.fetchone() or (None, None)
    finally:
        cur.close()
        conn.close()

    stops = []
    for stu in update.stop_time_update:
        parent = parent_map.get(stu.stop_id, stu.stop_id)
        row = names.get(parent)
        if not row:
            # Same graceful-skip convention as the pollers: a stop we can't
            # name is left out rather than shown as a blank row.
            continue
        name, lat, lon = row
        delay_sec = None
        actual = None
        if stu.HasField("arrival"):
            delay_sec = stu.arrival.delay if stu.arrival.HasField("delay") else None
            actual = stu.arrival.time if stu.arrival.HasField("time") else None
        if actual is None and stu.HasField("departure"):
            delay_sec = stu.departure.delay if stu.departure.HasField("delay") else delay_sec
            actual = stu.departure.time if stu.departure.HasField("time") else None
        # gtfs.de gives actual time and delay, so the scheduled time is the
        # difference - no stop_times import needed for DE (it has none).
        sched_epoch = (actual - delay_sec) if (actual and delay_sec is not None) else actual
        stops.append(_journey_stop(
            name, mapping, "DE",
            scheduled=_epoch_to_hhmm(sched_epoch, tz),
            expected=_epoch_to_hhmm(actual, tz),
            delay_min=round(delay_sec / 60, 1) if delay_sec is not None else None,
            lat=lat, lon=lon,
        ))
    return {
        "route_name": meta[0], "train_number": "", "headsign": meta[1],
        "train_id": train_id, "stops": stops, "position_source": "estimated",
        # DE is the one country with no stop_times imported, so the journey can
        # only be as complete as the feed itself - and gtfs.de's TripUpdates
        # carry only a trip's *remaining* stops (confirmed: a train near the end
        # of its run reports 2, one just starting reports up to 70). Flagged so
        # the UI can say "remaining stops" instead of implying a full route.
        "journey_scope": "remaining",
    }

_JOURNEY_PROVIDERS = {
    "HU": _journey_hu,
    "AT": _journey_at,
    "PL": _journey_pl,
    "DE": _journey_de,
}

@app.get("/train/{country}/{train_id:path}")
def get_train(country: str, train_id: str):
    country = country.upper()
    provider = _JOURNEY_PROVIDERS.get(country)
    if provider is None:
        return {"found": False, "error": f"Unknown country {country}"}
    mapping = _display_map(country)
    try:
        journey = provider(train_id, mapping)
    except Exception as exc:
        # A train tracker that 500s when one upstream feed hiccups is worse
        # than one that says so - the frontend renders this as a real message.
        return {"found": False, "error": str(exc)}
    if journey is None:
        return {"found": False, "error": "This train is no longer in the live feed."}

    stops = journey.get("stops") or []
    # Which stop is the train at/heading for: the live current_stop if the
    # feed named one, else the last stop whose expected time has passed.
    idx = None
    current = journey.get("current_stop")
    if current:
        for i, s in enumerate(stops):
            if s["name"] == current:
                idx = i
                break
    if idx is None and stops:
        now_hhmm = datetime.now().strftime("%H:%M")
        passed = [i for i, s in enumerate(stops) if s.get("expected") and s["expected"] <= now_hhmm]
        idx = passed[-1] if passed else 0
    for i, s in enumerate(stops):
        s["passed"] = idx is not None and i < idx
        s["current"] = i == idx

    delay = journey.get("delay_minutes")
    if delay is None and idx is not None and stops:
        delay = stops[idx].get("delay_min")

    route_name = journey.get("route_name")
    return {
        "found": True,
        "country": country,
        "train_id": journey.get("train_id") or train_id,
        "train_number": journey.get("train_number") or "",
        "route_name": route_name,
        "headsign": journey.get("headsign"),
        "origin": stops[0]["display"] if stops else None,
        "destination": stops[-1]["display"] if stops else None,
        "current_stop": _display_stop(mapping, current, country),
        "delay_minutes": delay,
        "status": journey.get("status") or "IN_TRANSIT_TO",
        "stop_idx": idx,
        "stops": stops,
        "lat": journey.get("lat"),
        "lon": journey.get("lon"),
        "position_source": journey.get("position_source"),
        "route_stats": _train_route_stats(route_name, country),
        "attribution": COUNTRY_ATTRIBUTION.get(country),
    }

def _train_route_stats(route_name, country):
    """This route's historical reliability, to sit under the live journey.

    The whole point of pairing a tracker with this project's data: not just
    "your train is 12 minutes late" but "and this route runs late 40% of the
    time". Cached per route because it's the same whole-country-scan class of
    query as /route/{name}/stats (see the Compare bugfix in NOTES.md)."""
    if not route_name:
        return None
    def compute():
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                SELECT COUNT(*) AS total,
                       ROUND(AVG(delay_minutes)::numeric, 1) AS avg_delay,
                       ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END)
                             / NULLIF(COUNT(*), 0), 1) AS pct_on_time,
                       MAX(delay_minutes) AS worst_delay
                FROM delay_observations
                WHERE route_name = %s AND country_code = %s
            """, (route_name, country))
            row = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        return dict(row) if row and row["total"] else None
    return _cached_summary("route_stats:" + route_name, country, compute)

# --- SEO / growth surface ---------------------------------------------
# Everything above this line is the JSON API the LiveRail SPA (index.html)
# consumes - a single client-rendered page with no per-entity URLs, which
# search engines have nothing to index and nothing distinct to rank. These
# endpoints render plain server-side HTML per station/route so each one is
# a real, crawlable, shareable URL - the foundation SEO/social growth needs
# (see MONETIZATION_READINESS.md). Deliberately minimal (no JS, no build
# step, inline styling) - this is a landing page for search engines and
# link previews, not a UI; the "Open the live map" link sends a real human
# straight into the interactive SPA.
#
# Absolute URLs (canonical/OG/sitemap) are built from the incoming
# request's own Host header rather than a hardcoded domain - this project
# already broke production once by hardcoding a host that changed (see
# NOTES.md's "Outage found and fixed 2026-08-02"); this endpoint set
# deliberately doesn't repeat that.

_COUNTRY_NAMES = {"HU": "Hungary", "AT": "Austria", "PL": "Poland", "DE": "Germany"}

def _base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.headers.get('host', request.url.netloc)}"

def _seo_page(request: Request, title: str, description: str, path: str, body_html: str, json_ld: dict = None) -> str:
    base = _base_url(request)
    canonical = f"{base}{path}"
    json_ld_tag = ""
    if json_ld is not None:
        import json as _json
        # station/route names are GTFS data, not user input, but this is
        # cheap insurance: json.dumps doesn't escape "</script>", which
        # would otherwise let a value like that break out of the tag.
        safe_json = _json.dumps(json_ld).replace("</", "<\\/")
        json_ld_tag = f'<script type="application/ld+json">{safe_json}</script>'
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_esc(title)}</title>
<meta name="description" content="{_esc(description)}">
<link rel="canonical" href="{_esc(canonical)}">
<meta property="og:type" content="website">
<meta property="og:title" content="{_esc(title)}">
<meta property="og:description" content="{_esc(description)}">
<meta property="og:url" content="{_esc(canonical)}">
<meta name="twitter:card" content="summary">
<meta name="twitter:title" content="{_esc(title)}">
<meta name="twitter:description" content="{_esc(description)}">
<style>
body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:640px;margin:2rem auto;padding:0 1rem;color:#1b1d22;line-height:1.5}}
a{{color:#1d4e89}} table{{border-collapse:collapse;width:100%;margin:1rem 0}}
td,th{{text-align:left;padding:.35rem .6rem;border-bottom:1px solid #e1e6e8}}
.cta{{display:inline-block;margin-top:1rem;padding:.6rem 1.1rem;background:#1d4e89;color:#fff;border-radius:4px;text-decoration:none}}
footer{{margin-top:2rem;font-size:.85rem;color:#565c64}}
</style>
{json_ld_tag}
</head>
<body>
{body_html}
<footer>Live delay tracking, rankings, and route comparison for Hungary, Austria, Poland, and Germany — <a href="/">open the live map</a>.</footer>
</body>
</html>"""

@app.get("/t/{country}/station/{name:path}", response_class=HTMLResponse)
def seo_station_page(request: Request, country: str, name: str):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT s.display_name,
               COUNT(o.id) as total,
               ROUND(100.0 * SUM(CASE WHEN o.delay_minutes <= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(o.id), 0)::numeric, 1) as pct_on_time,
               ROUND(AVG(o.delay_minutes)::numeric, 1) as avg_delay
        FROM stations s
        LEFT JOIN delay_observations o ON o.station_id = s.id
        WHERE s.name = %s AND s.country_code = %s
        GROUP BY s.display_name
    """, (name, country))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        return HTMLResponse(_seo_page(request, "Station not found", "This station isn't in our data.", request.url.path, "<h1>Station not found</h1>"), status_code=404)
    display = row["display_name"] or name
    country_name = _COUNTRY_NAMES.get(country, country)
    title = f"{display} train delays — {country_name} | LiveRail"
    if row["total"]:
        description = f"{display} ({country_name}): {row['pct_on_time']}% of trains on time, {row['avg_delay']} min average delay, based on {row['total']} tracked observations."
    else:
        description = f"Live and historical train delay tracking for {display}, {country_name}."
    body = f"""<h1>{_esc(display)}</h1>
<p>{_esc(country_name)} — live and historical train delay tracking.</p>
<table>
<tr><th>Observations tracked</th><td>{row['total'] or 0}</td></tr>
<tr><th>On-time rate (≤2 min)</th><td>{row['pct_on_time'] if row['pct_on_time'] is not None else '—'}%</td></tr>
<tr><th>Average delay</th><td>{row['avg_delay'] if row['avg_delay'] is not None else '—'} min</td></tr>
</table>
<a class="cta" href="/?country={_esc(country)}&station={_esc(name)}">Open live map for {_esc(display)}</a>"""
    json_ld = {
        "@context": "https://schema.org",
        "@type": "TrainStation",
        "name": display,
        "address": {"@type": "PostalAddress", "addressCountry": country},
    }
    return _seo_page(request, title, description, request.url.path, body, json_ld)

@app.get("/t/{country}/route/{name:path}", response_class=HTMLResponse)
def seo_route_page(request: Request, country: str, name: str):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT COUNT(*) as total,
               ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)::numeric, 1) as pct_on_time,
               ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delay_observations
        WHERE route_name = %s AND country_code = %s
    """, (name, country))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row or not row["total"]:
        return HTMLResponse(_seo_page(request, "Route not found", "This route isn't in our data.", request.url.path, "<h1>Route not found</h1>"), status_code=404)
    country_name = _COUNTRY_NAMES.get(country, country)
    title = f"{name} delays — {country_name} | LiveRail"
    description = f"{name} ({country_name}): {row['pct_on_time']}% of trains on time, {row['avg_delay']} min average delay, based on {row['total']} tracked observations."
    body = f"""<h1>{_esc(name)}</h1>
<p>{_esc(country_name)} — live and historical train delay tracking.</p>
<table>
<tr><th>Observations tracked</th><td>{row['total']}</td></tr>
<tr><th>On-time rate (≤2 min)</th><td>{row['pct_on_time']}%</td></tr>
<tr><th>Average delay</th><td>{row['avg_delay']} min</td></tr>
</table>
<a class="cta" href="/?country={_esc(country)}&route={_esc(name)}">Open live map for {_esc(name)}</a>"""
    json_ld = {
        "@context": "https://schema.org",
        "@type": "TrainTrip",
        "name": name,
    }
    return _seo_page(request, title, description, request.url.path, body, json_ld)

@app.get("/sitemap.xml")
def sitemap(request: Request):
    # Reuses the same _cached_summary()-wrapped queries /stations and
    # /routes already use - a fresh uncached DISTINCT route_name scan
    # across all four countries' delay_observations hit the exact
    # whole-table-scan timeout already documented in NOTES.md's Compare/
    # Rankings bugfix notes (PL/HU: seconds-to-timeout, not milliseconds).
    base = _base_url(request)
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    urls = [f"{base}/"]
    for country in _COUNTRY_NAMES:
        cur.execute("SELECT name FROM stations WHERE country_code = %s AND display_name IS NOT NULL", (country,))
        for r in cur.fetchall():
            urls.append(f"{base}/t/{country}/station/{_urlquote(r['name'])}")
        for name in _cached_summary("routes_list", country, lambda c=country: _routes_query(c)):
            if name:
                urls.append(f"{base}/t/{country}/route/{_urlquote(name)}")
    cur.close(); conn.close()
    body = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    body += [f"<url><loc>{_esc(u)}</loc></url>" for u in urls]
    body.append("</urlset>")
    return PlainTextResponse("\n".join(body), media_type="application/xml")

@app.get("/robots.txt")
def robots_txt(request: Request):
    base = _base_url(request)
    return PlainTextResponse(f"User-agent: *\nAllow: /\nSitemap: {base}/sitemap.xml\n")

# --- Compensation eligibility + claim assist -----------------------------
# Real feature, not a schema stub: backed by compensation_rules, seeded
# 2026-08-20 with sourced EU Regulation 2021/782 figures per country (see
# schema/compensation_rules_seed.sql for the full sourcing). No third-party
# "AirHelp for trains" service exists in Europe (checked directly, not
# assumed) - passengers claim straight from the operator - so this doesn't
# try to be a claims broker. It computes eligibility from a delay already
# visible in this app, and helps a user assemble what their operator's own
# claim form asks for. No login, no payment, no data stored - purely
# computed from parameters the frontend already has in hand (a live board
# row or a history row), consistent with keeping auth/payments out of this
# pass entirely.

_COUNTRY_CURRENCY = {"HU": "HUF", "AT": "EUR", "DE": "EUR", "PL": "PLN"}
_COUNTRY_CLAIM_URL = {
    "HU": "https://www.mavcsoport.hu/mav-szemelyszallitas/gyik/kesesigazolas-karterites",
    "AT": "https://www.oebb.at/en/fragen-und-antworten/rechte-pflichten/fahrgastrechte",
    "DE": "https://int.bahn.de/en/help/contact/passengers-rights-claim-form",
    "PL": "https://www.intercity.pl/en/dokumenty/wniosek_miedzynarodowy.pdf",
}

def _compensation_lookup(country: str, delay_minutes: float):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT min_delay_minutes, refund_pct, source_url, verified, notes
        FROM compensation_rules
        WHERE country_code = %s AND min_delay_minutes <= %s
        ORDER BY min_delay_minutes DESC
        LIMIT 1
    """, (country, delay_minutes))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row

@app.get("/compensation/check")
def compensation_check(country: str = "HU", delay_minutes: float = 0, ticket_price: float = None):
    rule = _compensation_lookup(country, delay_minutes)
    result = {
        "country": country,
        "delay_minutes": delay_minutes,
        "currency": _COUNTRY_CURRENCY.get(country),
        "claim_url": _COUNTRY_CLAIM_URL.get(country),
    }
    if not rule:
        result["eligible"] = False
        result["message"] = "This delay is below the compensation threshold for this country."
        return result
    result["eligible"] = True
    result["refund_pct"] = float(rule["refund_pct"])
    result["threshold_minutes"] = rule["min_delay_minutes"]
    result["source_url"] = rule["source_url"]
    result["notes"] = rule["notes"]
    if ticket_price:
        result["estimated_refund"] = round(ticket_price * float(rule["refund_pct"]) / 100, 2)
    result["message"] = f"This delay likely qualifies for a {rule['refund_pct']:g}% refund under EU passenger-rights rules."
    return result

@app.get("/compensation/claim")
def compensation_claim(country: str = "HU", route_name: str = "", station_name: str = "",
                        delay_minutes: float = 0, actual_arrival: str = None, ticket_price: float = None):
    """Assembles what the operator's own claim form asks for, from data
    this app already shows the user - not a submission, just a
    fill-in-the-blanks aid. actual_arrival is an ISO timestamp string from
    the frontend (already displayed there); scheduled time is derived by
    subtracting the delay, good enough for a claim aid, not a legal
    timestamp authority."""
    rule = _compensation_lookup(country, delay_minutes)
    scheduled = None
    if actual_arrival:
        try:
            actual_dt = datetime.fromisoformat(actual_arrival)
            scheduled = (actual_dt - timedelta(minutes=delay_minutes)).isoformat()
        except ValueError:
            pass
    summary = {
        "country": _COUNTRY_NAMES.get(country, country),
        "train": route_name,
        "station": station_name,
        "scheduled_time": scheduled,
        "actual_time": actual_arrival,
        "delay_minutes": delay_minutes,
        "eligible": bool(rule),
        "refund_pct": float(rule["refund_pct"]) if rule else None,
        "estimated_refund": round(ticket_price * float(rule["refund_pct"]) / 100, 2) if (rule and ticket_price) else None,
        "currency": _COUNTRY_CURRENCY.get(country),
        "claim_url": _COUNTRY_CLAIM_URL.get(country),
        "checklist": [
            "Train/route number and date of travel",
            "Departure and arrival station names",
            "Scheduled vs. actual arrival time (shown above)",
            "Your ticket (or booking reference, if bought online)",
            "Bank details or voucher preference, if the claim is approved",
        ],
    }
    return summary

@app.get("/partner-links")
def get_partner_links(country: str = "HU", kind: str = None):
    """Active sponsored/partner links for a country, so the frontend can
    render a real "book tickets" / "file a claim" CTA without hardcoding
    URLs client-side - swapping a target_url (e.g. once a real Trainline/
    Omio affiliate account exists) never needs a frontend deploy."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if kind:
        cur.execute("""
            SELECT id, kind, label FROM partner_links
            WHERE active AND scope = 'global' AND (country_code = %s OR country_code IS NULL) AND kind = %s
        """, (country, kind))
    else:
        cur.execute("""
            SELECT id, kind, label FROM partner_links
            WHERE active AND scope = 'global' AND (country_code = %s OR country_code IS NULL)
        """, (country,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

@app.get("/out/{link_id}")
def outbound_click(link_id: int, request: Request):
    """Logs the click (for affiliate reporting) then redirects. Anonymous -
    no login exists yet, user_id stays NULL, same as any real affiliate
    program's non-logged-in click flow."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT target_url FROM partner_links WHERE id = %s AND active", (link_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return PlainTextResponse("Link not found", status_code=404)
    referer = request.headers.get("referer", "")
    cur.execute(
        "INSERT INTO outbound_clicks (partner_link_id, referer_path) VALUES (%s, %s)",
        (link_id, referer),
    )
    conn.commit()
    cur.close(); conn.close()
    return RedirectResponse(row[0], status_code=302)
