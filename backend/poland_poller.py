import time
import requests
import psycopg2
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2

from config import DB_CONFIG

COUNTRY_CODE = "PL"
# See resolve_delays() for why this guard exists.
MAX_PLAUSIBLE_DELAY_MIN = 720

GTFS_RT_URL = "https://mkuran.pl/gtfs/polish_trains/updates.pb"

WARSAW_TZ = ZoneInfo("Europe/Warsaw")

def fetch_feed():
    resp = requests.get(GTFS_RT_URL, timeout=30)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed

def _stu_epoch(stu):
    if stu.HasField("arrival") and stu.arrival.time:
        return stu.arrival.time
    if stu.HasField("departure") and stu.departure.time:
        return stu.departure.time
    return None


def _current_stop_update(updates, now_epoch):
    """The stop this train is heading for right now.

    The first update whose time has not yet passed; if the whole trip is in the
    past, the last one; if nothing carries a time, the first, which is the old
    behaviour and keeps malformed entries working exactly as before.
    """
    timed = [(u, _stu_epoch(u)) for u in updates]
    timed = [(u, t) for u, t in timed if t]
    if not timed:
        return updates[0]
    for u, t in timed:
        if t >= now_epoch:
            return u
    return timed[-1][0]


def parse_pl_vehicles(feed, now_epoch=None):
    # Pure extraction, no database. This feed sets no stop_id, no route_id and
    # no delay, only stop_sequence and absolute timestamps. Finding the station
    # and computing the delay needs our imported stop_times and happens in
    # resolve_delays(), so this function stays unit-testable.
    if now_epoch is None:
        now_epoch = time.time()
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        gtfs_trip_id = trip_update.trip.trip_id
        start_date = trip_update.trip.start_date
        if not gtfs_trip_id or not start_date:
            continue

        # A cancelled trip carries no stop_time_updates, so it is handled
        # before the loop that needs them. It has no current stop either;
        # resolve_delays() attributes it to the trip's scheduled origin.
        if trip_update.trip.schedule_relationship == gtfs_realtime_pb2.TripDescriptor.CANCELED:
            rows.append({
                "gtfs_trip_id": gtfs_trip_id,
                "start_date": start_date,
                "stop_sequence": None,
                "actual_epoch": None,
                "cancellation_kind": "trip",
            })
            continue

        if not trip_update.stop_time_update:
            continue
        # The stop the train is heading for now, not stop_time_update[0]. This
        # feed publishes every trip from its first stop, including trips that
        # have not departed, so [0] is always the origin and never advances as
        # the train runs. Taking the next stop whose time has not passed
        # follows the train along its route, one row per stop it reaches.
        # (Germany's feed lists only the remaining stops, so its [0] already
        # advances.)
        stu = _current_stop_update(trip_update.stop_time_update, now_epoch)

        # A SKIPPED stop means the train runs but does not call here. It has no
        # time, so it would fail the timestamp check below and be lost, and it
        # is exactly what a rider waiting at that stop needs to know.
        if stu.schedule_relationship == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED:
            rows.append({
                "gtfs_trip_id": gtfs_trip_id,
                "start_date": start_date,
                "stop_sequence": stu.stop_sequence,
                "actual_epoch": None,
                "cancellation_kind": "stop",
            })
            continue

        # Keep arrival and departure separately, as in poller.py. actual_epoch
        # prefers the arrival, falling back to the departure.
        arrival_epoch = stu.arrival.time if (stu.HasField("arrival") and stu.arrival.HasField("time")) else None
        departure_epoch = stu.departure.time if (stu.HasField("departure") and stu.departure.HasField("time")) else None
        actual_epoch = arrival_epoch if arrival_epoch is not None else departure_epoch
        if actual_epoch is None:
            continue
        rows.append({
            "gtfs_trip_id": gtfs_trip_id,
            "start_date": start_date,
            "stop_sequence": stu.stop_sequence,
            "actual_epoch": actual_epoch,
            "actual_arrival_epoch": arrival_epoch,
            "actual_departure_epoch": departure_epoch,
            "cancellation_kind": None,
        })
    return rows

def _scheduled_epoch(start_date, hms):
    # start_date is "YYYYMMDD". hms is local "HH:MM:SS" and can exceed 24:00:00
    # for a service that runs past midnight.
    if not hms:
        return None
    h, m, s = (int(x) for x in hms.split(":"))
    extra_days, h = divmod(h, 24)
    base = datetime.strptime(start_date, "%Y%m%d")
    naive = base.replace(hour=h, minute=m, second=s) + timedelta(days=extra_days)
    return naive.replace(tzinfo=WARSAW_TZ).timestamp()

def _interpolate_position(prev_lat, prev_lon, prev_epoch, target_lat, target_lon, target_epoch, now_epoch):
    # No live GPS exists for Poland, so the position is estimated between the
    # previous and the next scheduled stop, in proportion to elapsed and
    # scheduled travel time. Callers must label it as an estimate (see
    # position_source). Pure function: now_epoch is passed in, so it is
    # unit-testable.
    if prev_lat is None or prev_lon is None or prev_epoch is None or target_epoch <= prev_epoch:
        return None, None
    fraction = max(0.0, min(1.0, (now_epoch - prev_epoch) / (target_epoch - prev_epoch)))
    lat = prev_lat + fraction * (target_lat - prev_lat)
    lon = prev_lon + fraction * (target_lon - prev_lon)
    return lat, lon

def resolve_delays(cur, rows, now_epoch=None):
    # Joins each (trip_id, stop_sequence) to our stop_times for the scheduled
    # stop and time, then delay = actual - scheduled. Rows whose trip or stop
    # is not in our tables are skipped, as for the other countries.
    if now_epoch is None:
        now_epoch = time.time()
    resolved = []
    for row in rows:
        kind = row.get("cancellation_kind")
        if kind == "trip":
            # A cancelled trip has no stop_sequence. Attribute it to the
            # scheduled origin, where a rider would have been waiting.
            cur.execute("""
                SELECT st.station_id, st.scheduled_arrival, st.scheduled_departure,
                       s.name, s.lat, s.lon, r.name, t.headsign, t.id, st.stop_sequence
                FROM trips t
                JOIN stop_times st ON st.trip_id = t.id
                JOIN stations s ON s.id = st.station_id
                JOIN routes r ON r.id = t.route_id
                WHERE t.gtfs_trip_id = %s AND t.country_code = %s
                ORDER BY st.stop_sequence
                LIMIT 1
            """, (row["gtfs_trip_id"], COUNTRY_CODE))
        else:
            cur.execute("""
                SELECT st.station_id, st.scheduled_arrival, st.scheduled_departure,
                       s.name, s.lat, s.lon, r.name, t.headsign, t.id, st.stop_sequence
                FROM trips t
                JOIN stop_times st ON st.trip_id = t.id AND st.stop_sequence = %s
                JOIN stations s ON s.id = st.station_id
                JOIN routes r ON r.id = t.route_id
                WHERE t.gtfs_trip_id = %s AND t.country_code = %s
            """, (row["stop_sequence"], row["gtfs_trip_id"], COUNTRY_CODE))
        match = cur.fetchone()
        if not match:
            continue
        # stop_sequence comes from our stop_times, not the feed, because a trip
        # cancellation carries none.
        (station_id, sched_arrival, sched_departure, station_name,
         target_lat, target_lon, route_name, headsign, trip_id,
         stop_sequence) = match

        if kind:
            # A train that does not call here has no delay. Writing 0 would
            # count every cancellation as an on-time arrival. No position is
            # estimated either, since the train is not heading for this stop.
            resolved.append({
                "station_id": station_id,
                "station_name": station_name,
                "route_name": route_name,
                "headsign": headsign,
                "delay_minutes": None,
                "stop_sequence": stop_sequence,
                "scheduled_arrival": sched_arrival,
                "scheduled_departure": sched_departure,
                "delay_arrival_minutes": None,
                "delay_departure_minutes": None,
                "lat": None,
                "lon": None,
                "position_source": None,
                "trip_id": trip_id,
                "gtfs_trip_id": row["gtfs_trip_id"],
                "is_cancelled": True,
                "cancellation_kind": kind,
                "platform": None,
            })
            continue
        scheduled_epoch = _scheduled_epoch(row["start_date"], sched_arrival or sched_departure)
        if scheduled_epoch is None:
            continue

        # A delay beyond 12 hours means the feed and our static snapshot number
        # this trip's stops differently, so the match above paired the wrong
        # stops. Skip it rather than store a 20-hour "delay".
        delay_minutes = round((row["actual_epoch"] - scheduled_epoch) / 60, 2)
        if abs(delay_minutes) > MAX_PLAUSIBLE_DELAY_MIN:
            continue

        lat = lon = None
        position_source = None
        cur.execute("""
            SELECT s.lat, s.lon, st.scheduled_arrival, st.scheduled_departure
            FROM stop_times st
            JOIN stations s ON s.id = st.station_id
            WHERE st.trip_id = (SELECT id FROM trips WHERE gtfs_trip_id = %s AND country_code = %s)
              AND st.stop_sequence < %s
            ORDER BY st.stop_sequence DESC
            LIMIT 1
        """, (row["gtfs_trip_id"], COUNTRY_CODE, row["stop_sequence"]))
        prev = cur.fetchone()
        if prev:
            prev_lat, prev_lon, prev_sched_arrival, prev_sched_departure = prev
            prev_epoch = _scheduled_epoch(row["start_date"], prev_sched_departure or prev_sched_arrival)
            lat, lon = _interpolate_position(
                prev_lat, prev_lon, prev_epoch, target_lat, target_lon, row["actual_epoch"], now_epoch
            )
            if lat is not None:
                position_source = "estimated"

        # The same guard for the split arrival and departure delays.
        def _split_delay(actual, sched_hms):
            if actual is None or not sched_hms:
                return None
            sched = _scheduled_epoch(row["start_date"], sched_hms)
            if sched is None:
                return None
            value = round((actual - sched) / 60, 2)
            return None if abs(value) > MAX_PLAUSIBLE_DELAY_MIN else value

        resolved.append({
            "station_id": station_id,
            "station_name": station_name,
            "route_name": route_name,
            "headsign": headsign,
            "delay_minutes": delay_minutes,
            "stop_sequence": stop_sequence,
            "scheduled_arrival": sched_arrival,
            "scheduled_departure": sched_departure,
            "delay_arrival_minutes": _split_delay(row.get("actual_arrival_epoch"), sched_arrival),
            "delay_departure_minutes": _split_delay(row.get("actual_departure_epoch"), sched_departure),
            "lat": lat,
            "lon": lon,
            "position_source": position_source,
            "trip_id": trip_id,
            # The feed's own trip id, alongside our trips.id.
            # /train/{country}/{id} needs it to find this trip again in a
            # freshly fetched feed.
            "gtfs_trip_id": row["gtfs_trip_id"],
            "is_cancelled": False,
            "cancellation_kind": None,
            # Poland publishes no platform data anywhere. The key is present so
            # every country's rows have the same shape.
            "platform": None,
        })
    # The feed can carry two active entities for one recurring trip (an
    # overnight run still on yesterday's start_date and today's run). Our
    # tables keep one schedule per trip, so both resolve identically and would
    # be written as duplicate rows. Keep one per trip until the schema records
    # start_date.
    return list({r["trip_id"]: r for r in resolved}.values())

def poll():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        print("Polling PKP...")
        feed = fetch_feed()
        raw_rows = parse_pl_vehicles(feed)
        print(f"Got {len(raw_rows)} raw stop-time updates")
        resolved = resolve_delays(cur, raw_rows)
        print(f"Resolved {len(resolved)}/{len(raw_rows)} against known stations/schedule")

        # Only write a row when something changed. The feed repeats the same
        # prediction every poll until the train reaches the stop, so writing
        # every poll stored the same fact hundreds of times.
        #
        # The comparison is scoped to today. Most Polish trip ids recur daily,
        # so an unscoped check would record a punctual daily train once and
        # never again. Delay and cancellation are compared as a pair, so both
        # becoming cancelled and running again are recorded.
        trip_ids = [row["trip_id"] for row in resolved]
        cur.execute("""
            SELECT DISTINCT ON (trip_id, station_id) trip_id, station_id,
                   delay_minutes, is_cancelled
            FROM delay_observations
            WHERE trip_id = ANY(%s) AND observed_date = %s
            ORDER BY trip_id, station_id, id DESC
        """, (trip_ids, date.today()))
        last_state = {(r[0], r[1]): (r[2], bool(r[3])) for r in cur.fetchall()}

        count = 0
        skipped = 0
        cancelled_count = 0
        for row in resolved:
            # Keyed on (trip, station), not the trip alone. Keying on the trip
            # skipped the insert whenever the delay was unchanged, even after
            # the train had moved to a new stop, so no journey was ever
            # recorded. Same train, same stop, same delay is still skipped.
            if last_state.get((row["trip_id"], row["station_id"])) == (
                    row["delay_minutes"], row["is_cancelled"]):
                skipped += 1
                continue
            if row["is_cancelled"]:
                cancelled_count += 1
                print(f"  {row['route_name']} at {row['station_name']}: CANCELLED ({row['cancellation_kind']})")
            else:
                print(f"  {row['route_name']} at {row['station_name']}: {row['delay_minutes']}")
            cur.execute("""
                INSERT INTO delay_observations
                    (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, headsign, lat, lon, position_source, trip_id, cancellation_kind, platform,
                     stop_sequence, scheduled_arrival, scheduled_departure, delay_arrival_minutes, delay_departure_minutes)
                VALUES (%s, CURRENT_DATE, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["station_id"], row["delay_minutes"], row["is_cancelled"], row["route_name"],
                COUNTRY_CODE, row["headsign"], row["lat"], row["lon"], row["position_source"],
                row["trip_id"], row["cancellation_kind"], row["platform"],
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
