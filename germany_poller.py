import csv
import time
import requests
import psycopg2
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2

from config import DB_PASSWORD

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": DB_PASSWORD,
    "host": "localhost"
}

COUNTRY_CODE = "DE"

GTFS_RT_URL = "https://realtime.gtfs.de/realtime-free.pb"

BERLIN_TZ = ZoneInfo("Europe/Berlin")

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
            # Needed to turn a scheduled stop_times HH:MM:SS into an actual
            # epoch for position estimation (see resolve_delays) - not used
            # for the delay figure itself, which stays feed-native.
            "start_date": trip_update.trip.start_date or None,
        })
    return rows

def _scheduled_epoch(start_date, hms):
    # Mirrors poland_poller.py's helper (Europe/Berlin instead of Warsaw).
    # start_date: "YYYYMMDD" (GTFS-RT TripDescriptor format). hms: GTFS's
    # "HH:MM:SS" local wall-clock time, which may exceed 24:00:00 for a
    # service continuing past midnight relative to start_date.
    if not start_date or not hms:
        return None
    h, m, s = (int(x) for x in hms.split(":"))
    extra_days, h = divmod(h, 24)
    base = datetime.strptime(start_date, "%Y%m%d")
    naive = base.replace(hour=h, minute=m, second=s) + timedelta(days=extra_days)
    return naive.replace(tzinfo=BERLIN_TZ).timestamp()

def _interpolate_position(prev_lat, prev_lon, prev_epoch, target_lat, target_lon, target_epoch, now_epoch):
    # Duplicated from poland_poller.py rather than shared - matches this
    # codebase's existing convention (DB_CONFIG etc. are duplicated per
    # poller too) rather than adding a new shared module for six lines. No
    # live GPS exists for DE either (gtfs.de's own realtime docs page
    # states its stream is TripUpdates + ServiceAlerts only, and both
    # HAFAS-based paths into DB were already confirmed dead - see
    # PLAN.md). Estimates a marker between the previous scheduled stop and
    # the target one, proportional to elapsed vs scheduled travel time -
    # the same technique Deutsche Bahn's own official "Zugradar" used for
    # years (also not real GPS) before being retired. Callers must label
    # this distinctly from HU/AT's real GPS - see position_source.
    if prev_lat is None or prev_lon is None or prev_epoch is None or target_epoch <= prev_epoch:
        return None, None
    fraction = max(0.0, min(1.0, (now_epoch - prev_epoch) / (target_epoch - prev_epoch)))
    lat = prev_lat + fraction * (target_lat - prev_lat)
    lon = prev_lon + fraction * (target_lon - prev_lon)
    return lat, lon

def resolve_delays(cur, rows, parent_map=None, now_epoch=None):
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
    #
    # The LEFT JOIN to stop_times is only for position estimation (see
    # _interpolate_position) - it's optional (target_lat/lon/route/station
    # resolution above don't depend on it), so a trip/station combo missing
    # from stop_times (e.g. mid-import, or a genuinely unscheduled service)
    # degrades to no estimated position rather than losing the observation.
    if parent_map is None:
        parent_map = load_parent_map()
    if now_epoch is None:
        now_epoch = time.time()
    resolved = []
    for row in rows:
        parent_stop_id = parent_map.get(row["gtfs_stop_id"])
        if not parent_stop_id:
            continue
        cur.execute("""
            SELECT s.id, s.name, s.lat, s.lon, r.name, t.id,
                   st.scheduled_arrival, st.scheduled_departure
            FROM stations s
            CROSS JOIN trips t
            JOIN routes r ON r.id = t.route_id
            LEFT JOIN stop_times st ON st.trip_id = t.id AND st.station_id = s.id
            WHERE s.gtfs_stop_id = %s AND s.country_code = %s
              AND t.gtfs_trip_id = %s AND t.country_code = %s
        """, (parent_stop_id, COUNTRY_CODE, row["gtfs_trip_id"], COUNTRY_CODE))
        match = cur.fetchone()
        if not match:
            continue
        (station_id, station_name, target_lat, target_lon, route_name,
         trip_id, target_sched_arrival, target_sched_departure) = match

        lat = lon = None
        position_source = None
        target_scheduled_epoch = _scheduled_epoch(
            row.get("start_date"), target_sched_arrival or target_sched_departure
        )
        if target_scheduled_epoch is not None:
            target_actual_epoch = target_scheduled_epoch + row["delay_minutes"] * 60
            cur.execute("""
                SELECT s2.lat, s2.lon, st2.scheduled_arrival, st2.scheduled_departure
                FROM stop_times st2
                JOIN stations s2 ON s2.id = st2.station_id
                WHERE st2.trip_id = %s
                  AND st2.stop_sequence < (
                      SELECT stop_sequence FROM stop_times WHERE trip_id = %s AND station_id = %s LIMIT 1
                  )
                ORDER BY st2.stop_sequence DESC
                LIMIT 1
            """, (trip_id, trip_id, station_id))
            prev = cur.fetchone()
            if prev:
                prev_lat, prev_lon, prev_sched_arrival, prev_sched_departure = prev
                prev_epoch = _scheduled_epoch(
                    row.get("start_date"), prev_sched_departure or prev_sched_arrival
                )
                lat, lon = _interpolate_position(
                    prev_lat, prev_lon, prev_epoch, target_lat, target_lon, target_actual_epoch, now_epoch
                )
                if lat is not None:
                    position_source = "estimated"

        resolved.append({
            "station_id": station_id,
            "station_name": station_name,
            "route_name": route_name,
            "delay_minutes": row["delay_minutes"],
            "lat": lat,
            "lon": lon,
            "position_source": position_source,
            "trip_id": trip_id,
            # See the same field in poland_poller.resolve_delays(): the feed's
            # own trip id, needed by /train/{country}/{id} to locate the rest
            # of this trip's stop_time_updates in a fresh feed.
            "gtfs_trip_id": row["gtfs_trip_id"],
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

    # Same fix as poland_poller.py: gtfs.de's feed re-reports an unchanged
    # delay for a trip across many consecutive polls, so only write a row
    # when the delay actually changed since the trip's last observation.
    # Scoped to today - see the same block in poland_poller.py for why the
    # unscoped version silently dropped every recurring trip whose delay
    # matched its last recorded value (43.2% of DE trip_ids recur across days).
    trip_ids = [row["trip_id"] for row in resolved]
    cur.execute("""
        SELECT DISTINCT ON (trip_id) trip_id, delay_minutes
        FROM delay_observations
        WHERE trip_id = ANY(%s) AND observed_date = %s
        ORDER BY trip_id, id DESC
    """, (trip_ids, date.today()))
    last_delay = dict(cur.fetchall())

    count = 0
    skipped = 0
    for row in resolved:
        if last_delay.get(row["trip_id"]) == row["delay_minutes"]:
            skipped += 1
            continue
        print(f"  {row['route_name']} at {row['station_name']}: {row['delay_minutes']}")
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, lat, lon, position_source, trip_id)
            VALUES (%s, CURRENT_DATE, NOW(), %s, false, %s, %s, %s, %s, %s, %s)
        """, (
            row["station_id"], row["delay_minutes"], row["route_name"], COUNTRY_CODE,
            row["lat"], row["lon"], row["position_source"], row["trip_id"],
        ))
        count += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations, skipped {skipped} unchanged.")

if __name__ == "__main__":
    poll()
