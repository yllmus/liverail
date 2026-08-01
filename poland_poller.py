import requests
import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": "changeme",
    "host": "localhost"
}

COUNTRY_CODE = "PL"

GTFS_RT_URL = "https://mkuran.pl/gtfs/polish_trains/updates.pb"

WARSAW_TZ = ZoneInfo("Europe/Warsaw")

def fetch_feed():
    resp = requests.get(GTFS_RT_URL, timeout=30)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed

def parse_pl_vehicles(feed):
    # Pure extraction, no DB. Confirmed against the real feed: mkuran.pl's
    # TripUpdates never set stop_id, route_id, or a delay field - only
    # stop_sequence and absolute Unix timestamps (arrival.time/departure.time).
    # Resolving a station/schedule and computing delay needs the paired
    # static feed's stop_times (imported via `import.py --with-stop-times`),
    # done separately in resolve_delays() so this stays DB-independent and
    # unit-testable, same as parse_vehicles()/parse_at_vehicles().
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        gtfs_trip_id = trip_update.trip.trip_id
        start_date = trip_update.trip.start_date
        if not gtfs_trip_id or not start_date or not trip_update.stop_time_update:
            continue
        # Take the first stop_time_update only, same choice the original
        # design made (and the same choice AT's nextStopovers[0] makes) -
        # one "current" delay figure per trip per poll, not a prediction
        # for every remaining stop on the route.
        stu = trip_update.stop_time_update[0]
        actual_epoch = None
        if stu.HasField("arrival") and stu.arrival.HasField("time"):
            actual_epoch = stu.arrival.time
        elif stu.HasField("departure") and stu.departure.HasField("time"):
            actual_epoch = stu.departure.time
        if actual_epoch is None:
            continue
        rows.append({
            "gtfs_trip_id": gtfs_trip_id,
            "start_date": start_date,
            "stop_sequence": stu.stop_sequence,
            "actual_epoch": actual_epoch,
        })
    return rows

def _scheduled_epoch(start_date, hms):
    # start_date: "YYYYMMDD" (GTFS-RT TripDescriptor format). hms: GTFS's
    # "HH:MM:SS" local wall-clock time, which may exceed 24:00:00 for a
    # service continuing past midnight relative to start_date.
    if not hms:
        return None
    h, m, s = (int(x) for x in hms.split(":"))
    extra_days, h = divmod(h, 24)
    base = datetime.strptime(start_date, "%Y%m%d")
    naive = base.replace(hour=h, minute=m, second=s) + timedelta(days=extra_days)
    return naive.replace(tzinfo=WARSAW_TZ).timestamp()

def resolve_delays(cur, rows):
    # DB-dependent step. Joins each raw (trip_id, stop_sequence) against our
    # stop_times table to get the scheduled stop and scheduled time, then
    # delay = actual - scheduled. Silently resolves to nothing if the trip
    # isn't in our PL trips (unimported/stale) or the stop_id it references
    # was one of the platform-level rows import.py's parent_station filter
    # excluded from stations - same graceful-skip behavior as HU/AT.
    resolved = []
    for row in rows:
        cur.execute("""
            SELECT st.station_id, st.scheduled_arrival, st.scheduled_departure,
                   s.name, r.name, t.headsign
            FROM trips t
            JOIN stop_times st ON st.trip_id = t.id AND st.stop_sequence = %s
            JOIN stations s ON s.id = st.station_id
            JOIN routes r ON r.id = t.route_id
            WHERE t.gtfs_trip_id = %s AND t.country_code = %s
        """, (row["stop_sequence"], row["gtfs_trip_id"], COUNTRY_CODE))
        match = cur.fetchone()
        if not match:
            continue
        station_id, sched_arrival, sched_departure, station_name, route_name, headsign = match
        scheduled_epoch = _scheduled_epoch(row["start_date"], sched_arrival or sched_departure)
        if scheduled_epoch is None:
            continue
        resolved.append({
            "station_id": station_id,
            "station_name": station_name,
            "route_name": route_name,
            "headsign": headsign,
            "delay_minutes": round((row["actual_epoch"] - scheduled_epoch) / 60, 2),
        })
    return resolved

def poll():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Polling PKP...")
    feed = fetch_feed()
    raw_rows = parse_pl_vehicles(feed)
    print(f"Got {len(raw_rows)} raw stop-time updates")
    resolved = resolve_delays(cur, raw_rows)
    print(f"Resolved {len(resolved)}/{len(raw_rows)} against known stations/schedule")
    count = 0
    for row in resolved:
        print(f"  {row['route_name']} at {row['station_name']}: {row['delay_minutes']}")
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, headsign)
            VALUES (%s, CURRENT_DATE, NOW(), %s, false, %s, %s, %s)
        """, (row["station_id"], row["delay_minutes"], row["route_name"], COUNTRY_CODE, row["headsign"]))
        count += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations.")

if __name__ == "__main__":
    poll()
