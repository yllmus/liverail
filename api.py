from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import requests as req
import time
from datetime import datetime, timedelta
from austria_poller import call_radar, parse_at_vehicles
from poland_poller import fetch_feed, parse_pl_vehicles, resolve_delays

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
    cur.execute("SELECT DISTINCT name FROM stations WHERE country_code = %s ORDER BY name", (country,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r["name"] for r in rows]

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
    cur.execute("""
        SELECT s.name as station_name,
               COUNT(*) as total,
               ROUND(100.0 * SUM(CASE WHEN o.delay_minutes <= 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_on_time,
               ROUND(AVG(o.delay_minutes)::numeric, 1) as avg_delay
        FROM delay_observations o
        JOIN stations s ON o.station_id = s.id
        WHERE o.country_code = %s
        GROUP BY s.name
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

@app.get("/live")
def get_live(country: str = "HU"):
    if country == "AT":
        return get_live_at()
    if country == "PL":
        return get_live_pl()
    return get_live_hu()

_AT_LIVE_CACHE = {"data": None, "expires": 0}
AT_LIVE_CACHE_TTL = 25

def get_live_at():
    now = time.time()
    if _AT_LIVE_CACHE["data"] is not None and now < _AT_LIVE_CACHE["expires"]:
        return _AT_LIVE_CACHE["data"]

    movements = call_radar()
    result = [
        {
            "route_name": row["route_name"],
            "train_number": "",
            "current_stop": row["stop_name"],
            "delay_minutes": row["delay_minutes"],
            "status": "IN_TRANSIT_TO",
            "stops": [],
            "stop_idx": None,
            "lat": None,
            "lon": None,
        }
        for row in parse_at_vehicles(movements)
    ]
    _AT_LIVE_CACHE["data"] = result
    _AT_LIVE_CACHE["expires"] = now + AT_LIVE_CACHE_TTL
    return result

_PL_LIVE_CACHE = {"data": None, "expires": 0}
PL_LIVE_CACHE_TTL = 25

def get_live_pl():
    now = time.time()
    if _PL_LIVE_CACHE["data"] is not None and now < _PL_LIVE_CACHE["expires"]:
        return _PL_LIVE_CACHE["data"]

    feed = fetch_feed()
    raw_rows = parse_pl_vehicles(feed)

    conn = get_conn()
    cur = conn.cursor()
    resolved = resolve_delays(cur, raw_rows)
    cur.close()
    conn.close()

    result = [
        {
            "route_name": row["route_name"],
            "train_number": "",
            "current_stop": row["station_name"],
            "delay_minutes": row["delay_minutes"],
            "status": "IN_TRANSIT_TO",
            "stops": [],
            "stop_idx": None,
            "lat": None,
            "lon": None,
        }
        for row in resolved
    ]

    _PL_LIVE_CACHE["data"] = result
    _PL_LIVE_CACHE["expires"] = now + PL_LIVE_CACHE_TTL
    return result

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
            "current_stop": stop_name,
            "delay_minutes": round(delay_sec / 60, 1),
            "status": stop_rel.get("status") or "IN_TRANSIT_TO",
            "stops": stops,
            "stop_idx": stop_idx,
            "lat": lat,
            "lon": lon,
        })

    return result
