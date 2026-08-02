from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import requests as req
import time
from datetime import datetime, timedelta
from austria_poller import call_radar, parse_at_vehicles
from poland_poller import fetch_feed, parse_pl_vehicles, resolve_delays
from germany_poller import fetch_feed as fetch_de_feed, parse_de_vehicles, resolve_delays as resolve_de_delays
from station_names import clean_display_name

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
    "password": "changeme",
    "host": "localhost"
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

COUNTRY_ATTRIBUTION = {
    "AT": "Data: ÖBB (data.oebb.at), CC BY 4.0",
    "PL": "Data: PKP PLK (public sector data), via mkuran.pl",
    "DE": "Data: DELFI e.V., via gtfs.de - schedule CC BY 4.0, realtime CC BY-SA 4.0",
}

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

@app.get("/station/{station_name}/delays")
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

@app.get("/routes")
def get_routes(country: str = "HU"):
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

@app.get("/routes/summary")
def get_routes_summary(country: str = "HU"):
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

@app.get("/route/{route_name}/delays")
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

@app.get("/route/{route_name}/stats")
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

@app.get("/station/{station_name}/stats")
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

@app.get("/stations/summary")
def get_stations_summary(country: str = "HU"):
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
    "PL": (_live_pl, 25),
    "DE": (_live_de, 25),
}

@app.get("/live")
def get_live(country: str = "HU"):
    if country == "HU":
        return get_live_hu()
    provider = _LIVE_PROVIDERS.get(country)
    if provider is None:
        return []
    fetch_fn, ttl = provider
    now = time.time()
    cached = _LIVE_CACHE.get(country)
    if cached is not None and now < cached["expires"]:
        return cached["data"]
    data = fetch_fn()
    _LIVE_CACHE[country] = {"data": data, "expires": now + ttl}
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
        route_name = trip.get("route", {}).get("longName")
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
