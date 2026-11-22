import csv
import os
import time
import requests
import psycopg2
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2

from config import DB_CONFIG

COUNTRY_CODE = "DE"
# How many of a trip's remaining stops to record on each poll. This does not
# limit how many stops of a journey are recorded: gtfs.de lists only the stops
# still ahead, and the window slides forward as the train runs.
#
# The cap drops far-future predictions (the delay at stop 40 while the train is
# at stop 1, revised every poll). They were a large share of the writes and the
# least reliable numbers in the feed. Recording every stop cost about six times
# the writes. See docs/deployment.md.
STOPS_PER_POLL = 3
# Ignore changes smaller than this. gtfs.de often restates the same delay with
# a few seconds' difference, which is noise for a delay distribution and costs
# a row each time.
MIN_DELAY_CHANGE_MIN = 1.0

# The same guard poland_poller.py and api.py apply. gtfs.de occasionally
# publishes nonsense figures, usually large negative ones; a train is not 21
# hours early, and one such row skews every statistic built on it.
MAX_PLAUSIBLE_DELAY_MIN = 720

GTFS_RT_URL = "https://realtime.gtfs.de/realtime-free.pb"

BERLIN_TZ = ZoneInfo("Europe/Berlin")

# Every stop_id the realtime feed sets is a platform-level child row, never the
# top-level station that import.py keeps in `stations`, so it is translated to
# its parent station first. The map is built from the same static stops.txt
# files import.py loaded, which refresh_static_feeds.py extracts to the home
# directory.
GTFS_STOPS_FILES = [os.path.expanduser("~/gtfs_de_fv/stops.txt"),
                    os.path.expanduser("~/gtfs_de_rv/stops.txt")]

def load_parent_map():
    parent_map = {}
    for path in GTFS_STOPS_FILES:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                parent = (row.get("parent_station") or "").strip()
                if parent:
                    parent_map[row["stop_id"]] = parent
    return parent_map

def load_platform_map():
    # Because the feed's stop_id is a platform row, the static feed also gives
    # the platform the train calls at, where gtfs.de fills platform_code. That
    # is only a small fraction of rows, so the column is mostly NULL rather
    # than guessed from the stop name.
    platform_map = {}
    for path in GTFS_STOPS_FILES:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                code = (row.get("platform_code") or "").strip()
                if code:
                    platform_map[row["stop_id"]] = code
    return platform_map

def fetch_feed():
    resp = requests.get(GTFS_RT_URL, timeout=30)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed

def parse_de_vehicles(feed):
    # Pure extraction, no database. Unlike Poland's feed, gtfs.de sets the
    # delay directly on each stop_time_update, so no schedule join is needed
    # for it. route_id is always empty, so the route name still comes from our
    # trips and routes tables.
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        gtfs_trip_id = trip_update.trip.trip_id
        if not gtfs_trip_id or not trip_update.stop_time_update:
            continue
        # Several stops per trip, not only the first. gtfs.de lists only the
        # remaining stops, so recording just the first scattered observations
        # thinly along each journey and never built up depth at any stop.
        #
        # The write volume is bounded by STOPS_PER_POLL and by the dedup in
        # poll(), which writes a stop only when its own predicted delay
        # changes.
        for stu in trip_update.stop_time_update[:STOPS_PER_POLL]:
            _emit_stop(rows, trip_update, gtfs_trip_id, stu)
    return rows

def _emit_stop(rows, trip_update, gtfs_trip_id, stu):
    """Append at most one row for a single stop_time_update."""
    gtfs_stop_id = stu.stop_id

    # gtfs.de never sets the trip-level CANCELED flag, so a fully cancelled
    # train appears only as one whose stops are all SKIPPED. A skipped stop has
    # no delay, so it is handled before the delay check below.
    skipped = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
    if stu.schedule_relationship == skipped:
        all_skipped = all(
            s.schedule_relationship == skipped for s in trip_update.stop_time_update
        )
        if gtfs_stop_id:
            rows.append({
                "gtfs_trip_id": gtfs_trip_id,
                "gtfs_stop_id": gtfs_stop_id,
                "delay_minutes": None,
                "start_date": trip_update.trip.start_date or None,
                "cancellation_kind": "trip" if all_skipped else "stop",
            })
        return

    # Keep arrival and departure delays separately, as in poller.py.
    arrival_delay = stu.arrival.delay if (stu.HasField("arrival") and stu.arrival.HasField("delay")) else None
    departure_delay = stu.departure.delay if (stu.HasField("departure") and stu.departure.HasField("delay")) else None
    delay_seconds = arrival_delay if arrival_delay is not None else departure_delay
    if not gtfs_stop_id or delay_seconds is None:
        return
    delay_minutes = round(delay_seconds / 60, 2)
    if abs(delay_minutes) > MAX_PLAUSIBLE_DELAY_MIN:
        return
    rows.append({
        "gtfs_trip_id": gtfs_trip_id,
        "gtfs_stop_id": gtfs_stop_id,
        "delay_minutes": delay_minutes,
        "delay_arrival_minutes": None if arrival_delay is None else round(arrival_delay / 60, 2),
        "delay_departure_minutes": None if departure_delay is None else round(departure_delay / 60, 2),
        # Converts a scheduled HH:MM:SS into an epoch for position estimation.
        # The delay itself comes straight from the feed.
        "start_date": trip_update.trip.start_date or None,
        "cancellation_kind": None,
    })
def _scheduled_epoch(start_date, hms):
    # Same as poland_poller.py's helper, in Europe/Berlin time. start_date is
    # "YYYYMMDD"; hms is local "HH:MM:SS" and can exceed 24:00:00.
    if not start_date or not hms:
        return None
    h, m, s = (int(x) for x in hms.split(":"))
    extra_days, h = divmod(h, 24)
    base = datetime.strptime(start_date, "%Y%m%d")
    naive = base.replace(hour=h, minute=m, second=s) + timedelta(days=extra_days)
    return naive.replace(tzinfo=BERLIN_TZ).timestamp()

def _interpolate_position(prev_lat, prev_lon, prev_epoch, target_lat, target_lon, target_epoch, now_epoch):
    # No live GPS exists for Germany either (gtfs.de publishes trip updates and
    # alerts only), so the position is estimated between scheduled stops, as
    # for Poland. Callers must label it as an estimate (see position_source).
    if prev_lat is None or prev_lon is None or prev_epoch is None or target_epoch <= prev_epoch:
        return None, None
    fraction = max(0.0, min(1.0, (now_epoch - prev_epoch) / (target_epoch - prev_epoch)))
    lat = prev_lat + fraction * (target_lat - prev_lat)
    lon = prev_lon + fraction * (target_lon - prev_lon)
    return lat, lon

def resolve_delays(cur, rows, parent_map=None, now_epoch=None, platform_map=None):
    # gtfs_trip_id joins directly to our trips table. gtfs_stop_id is a
    # platform-level child id, so it is translated to its parent station first
    # (see load_parent_map). The feed links station and route only through the
    # trip, hence two independent lookups joined with CROSS JOIN. Ids we did
    # not import, such as bus and tram stops, resolve to nothing and are
    # skipped.
    #
    # The LEFT JOIN to stop_times only feeds position estimation, so a missing
    # schedule row costs the position, not the observation.
    if parent_map is None:
        parent_map = load_parent_map()
    if platform_map is None:
        platform_map = load_platform_map()
    if now_epoch is None:
        now_epoch = time.time()
    resolved = []
    for row in rows:
        parent_stop_id = parent_map.get(row["gtfs_stop_id"])
        if not parent_stop_id:
            continue
        platform = platform_map.get(row["gtfs_stop_id"])
        cur.execute("""
            SELECT s.id, s.name, s.lat, s.lon, r.name, t.id,
                   st.scheduled_arrival, st.scheduled_departure, st.stop_sequence
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
         trip_id, target_sched_arrival, target_sched_departure, stop_sequence) = match

        if row.get("cancellation_kind"):
            # As in poland_poller.py: a train that does not call here has a
            # NULL delay, not 0, and no position is estimated toward this stop.
            resolved.append({
                "station_id": station_id,
                "station_name": station_name,
                "route_name": route_name,
                "delay_minutes": None,
                "stop_sequence": stop_sequence,
                "scheduled_arrival": target_sched_arrival,
                "scheduled_departure": target_sched_departure,
                "delay_arrival_minutes": None,
                "delay_departure_minutes": None,
                "lat": None,
                "lon": None,
                "position_source": None,
                "trip_id": trip_id,
                "gtfs_trip_id": row["gtfs_trip_id"],
                "is_cancelled": True,
                "cancellation_kind": row["cancellation_kind"],
                "platform": platform,
            })
            continue

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
            "stop_sequence": stop_sequence,
            "scheduled_arrival": target_sched_arrival,
            "scheduled_departure": target_sched_departure,
            "delay_arrival_minutes": row.get("delay_arrival_minutes"),
            "delay_departure_minutes": row.get("delay_departure_minutes"),
            "lat": lat,
            "lon": lon,
            "position_source": position_source,
            "trip_id": trip_id,
            # The feed's own trip id, as in poland_poller.py:
            # /train/{country}/{id} needs it to find the rest of this trip in a
            # fresh feed.
            "gtfs_trip_id": row["gtfs_trip_id"],
            "is_cancelled": False,
            "cancellation_kind": None,
            "platform": platform,
        })
    return resolved

def poll():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        print("Polling gtfs.de...")
        feed = fetch_feed()
        raw_rows = parse_de_vehicles(feed)
        print(f"Got {len(raw_rows)} raw stop-time updates")
        resolved = resolve_delays(cur, raw_rows)
        print(f"Resolved {len(resolved)}/{len(raw_rows)} against known DE stations/trips")

        # Write a stop only when its delay changed, as in poland_poller.py:
        # scoped to today, comparing delay and cancellation as a pair.
        #
        # Keyed per (trip, stop), because parse_de_vehicles emits several stops
        # per trip; a per-trip key would compare one stop's delay with
        # another's and write on almost every poll.
        trip_ids = [row["trip_id"] for row in resolved]
        cur.execute("""
            SELECT DISTINCT ON (trip_id, stop_sequence) trip_id, stop_sequence,
                   delay_minutes, is_cancelled
            FROM delay_observations
            WHERE trip_id = ANY(%s) AND observed_date = %s
            ORDER BY trip_id, stop_sequence, id DESC
        """, (trip_ids, date.today()))
        last_state = {(r[0], r[1]): (r[2], bool(r[3])) for r in cur.fetchall()}

        count = 0
        skipped = 0
        cancelled_count = 0
        for row in resolved:
            prev = last_state.get((row["trip_id"], row.get("stop_sequence")))
            if prev is not None and prev[1] == row["is_cancelled"] \
                    and prev[0] is not None and row["delay_minutes"] is not None \
                    and abs(row["delay_minutes"] - prev[0]) < MIN_DELAY_CHANGE_MIN:
                # Unchanged, or changed by less than MIN_DELAY_CHANGE_MIN. A
                # change in cancellation is always written.
                skipped += 1
                continue
            if prev == (row["delay_minutes"], row["is_cancelled"]):
                skipped += 1
                continue
            if row["is_cancelled"]:
                cancelled_count += 1
                print(f"  {row['route_name']} at {row['station_name']}: CANCELLED ({row['cancellation_kind']})")
            else:
                print(f"  {row['route_name']} at {row['station_name']}: {row['delay_minutes']}")
            cur.execute("""
                INSERT INTO delay_observations
                    (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, lat, lon, position_source, trip_id, cancellation_kind, platform,
                     stop_sequence, scheduled_arrival, scheduled_departure, delay_arrival_minutes, delay_departure_minutes)
                VALUES (%s, CURRENT_DATE, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["station_id"], row["delay_minutes"], row["is_cancelled"], row["route_name"],
                COUNTRY_CODE, row["lat"], row["lon"], row["position_source"], row["trip_id"],
                row["cancellation_kind"], row["platform"],
                row.get("stop_sequence"), row.get("scheduled_arrival"), row.get("scheduled_departure"),
                row.get("delay_arrival_minutes"), row.get("delay_departure_minutes"),
            ))
            count += 1
        conn.commit()
        print(f"Saved {count} observations ({cancelled_count} cancelled), skipped {skipped} unchanged.")
    finally:
        conn.close()

if __name__ == "__main__":
    poll()
