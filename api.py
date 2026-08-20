from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from html import escape as _esc
from urllib.parse import quote as _urlquote
import psycopg2
import psycopg2.extras
import requests as req
import threading
import time
from datetime import datetime, timedelta
from austria_poller import call_radar, parse_at_vehicles
from poland_poller import fetch_feed, parse_pl_vehicles, resolve_delays
from germany_poller import fetch_feed as fetch_de_feed, parse_de_vehicles, resolve_delays as resolve_de_delays
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
    "PL": "Data: PKP PLK (public sector data), via mkuran.pl",
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

def _cached_summary(kind, country, compute_fn):
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
        _SUMMARY_CACHE[cache_key] = {"data": data, "expires": time.time() + _SUMMARY_TTL}
        return data

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
    return _cached_summary("routes_list", country, lambda: _routes_query(country))

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
    return [dict(r) for r in rows]

@app.get("/routes/summary")
def get_routes_summary(country: str = "HU"):
    return _cached_summary("routes", country, lambda: _routes_summary_query(country))

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
    return _cached_summary("stations", country, lambda: _stations_summary_query(country))

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
      tripShortName
      tripHeadsign
      route { longName }
      stoptimes { stop { name } scheduledDeparture departureDelay }
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
    "AT": (_live_at, 25),
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
            name = (st.get("stop") or {}).get("name") or ""
            dep_delay = st.get("departureDelay") or 0
            stops.append({
                "name": name,
                "dep": _fmt_time(st.get("scheduledDeparture")),
                "delay_min": round(dep_delay / 60, 1),
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
