import csv
import requests
import psycopg2
from datetime import date
from google.transit import gtfs_realtime_pb2

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": "changeme",
    "host": "localhost"
}

COUNTRY_CODE = "DE"

GTFS_RT_URL = "https://realtime.gtfs.de/realtime-free.pb"

# Confirmed against the real feed: every stop_id it sets on a
# stop_time_update is a platform/track-level child row (parent_station set)
# from the static feed, never the top-level station id import.py's
# parent_station filter kept in `stations` - a sample of 1686 resolvable
# RT stop_ids came back 1686/1686 child rows, 0 top-level. So resolution
# needs one extra step PL/AT didn't: translate the feed's child stop_id to
# its parent_station before looking it up in `stations`. Built from the
# same static files import.py imported from, kept on disk after import
# (same convention as gtfs_at/gtfs_pl not being deleted post-import).
GTFS_STOPS_FILES = ["gtfs_de_fv/stops.txt", "gtfs_de_rv/stops.txt"]

def load_parent_map():
    parent_map = {}
    for path in GTFS_STOPS_FILES:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                parent = (row.get("parent_station") or "").strip()
                if parent:
                    parent_map[row["stop_id"]] = parent
    return parent_map

def fetch_feed():
    resp = requests.get(GTFS_RT_URL, timeout=30)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed

def parse_de_vehicles(feed):
    # Pure extraction, no DB. Unlike Poland's feed, gtfs.de's realtime-free.pb
    # DOES expose arrival/departure delay directly on each stop_time_update
    # (confirmed against the real feed: ~313k/~313k stop_time_updates that
    # set arrival.time also set arrival.delay) - no schedule join needed to
    # compute a delay. route_id is always empty on trip.route_id though, so
    # route name still comes from our imported trips->routes, same as PL.
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        gtfs_trip_id = trip_update.trip.trip_id
        if not gtfs_trip_id or not trip_update.stop_time_update:
            continue
        # First stop_time_update only - one "current" delay figure per trip
        # per poll, same choice AT's nextStopovers[0] and PL's stu[0] make.
        stu = trip_update.stop_time_update[0]
        gtfs_stop_id = stu.stop_id
        delay_seconds = None
        if stu.HasField("arrival") and stu.arrival.HasField("delay"):
            delay_seconds = stu.arrival.delay
        elif stu.HasField("departure") and stu.departure.HasField("delay"):
            delay_seconds = stu.departure.delay
        if not gtfs_stop_id or delay_seconds is None:
            continue
        rows.append({
            "gtfs_trip_id": gtfs_trip_id,
            "gtfs_stop_id": gtfs_stop_id,
            "delay_minutes": round(delay_seconds / 60, 2),
        })
    return rows

def resolve_delays(cur, rows, parent_map=None):
    # DB-dependent step. gtfs_trip_id resolves directly against our trips
    # table. gtfs_stop_id does not: it's always a platform-level child id
    # (see load_parent_map() above), so it's translated to its parent
    # station id first, then that's looked up in `stations`. Station and
    # route are two unrelated single-row lookups (the feed gives no
    # stop_times-style link between them), hence CROSS JOIN rather than a
    # real relational join. Silently resolves to nothing if either id isn't
    # one we imported (e.g. a non-rail trip/stop from the feed's bus/tram
    # operators, or a stop with no parent_station at all in the static
    # feed) - same graceful-skip behavior as HU/AT/PL.
    if parent_map is None:
        parent_map = load_parent_map()
    resolved = []
    for row in rows:
        parent_stop_id = parent_map.get(row["gtfs_stop_id"])
        if not parent_stop_id:
            continue
        cur.execute("""
            SELECT s.id, s.name, r.name
            FROM stations s
            CROSS JOIN trips t
            JOIN routes r ON r.id = t.route_id
            WHERE s.gtfs_stop_id = %s AND s.country_code = %s
              AND t.gtfs_trip_id = %s AND t.country_code = %s
        """, (parent_stop_id, COUNTRY_CODE, row["gtfs_trip_id"], COUNTRY_CODE))
        match = cur.fetchone()
        if not match:
            continue
        station_id, station_name, route_name = match
        resolved.append({
            "station_id": station_id,
            "station_name": station_name,
            "route_name": route_name,
            "delay_minutes": row["delay_minutes"],
        })
    return resolved

def poll():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Polling gtfs.de...")
    feed = fetch_feed()
    raw_rows = parse_de_vehicles(feed)
    print(f"Got {len(raw_rows)} raw stop-time updates")
    resolved = resolve_delays(cur, raw_rows)
    print(f"Resolved {len(resolved)}/{len(raw_rows)} against known DE stations/trips")
    count = 0
    for row in resolved:
        print(f"  {row['route_name']} at {row['station_name']}: {row['delay_minutes']}")
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code)
            VALUES (%s, CURRENT_DATE, NOW(), %s, false, %s, %s)
        """, (row["station_id"], row["delay_minutes"], row["route_name"], COUNTRY_CODE))
        count += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations.")

if __name__ == "__main__":
    poll()
