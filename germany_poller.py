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
# How many of a trip's remaining stops to record per poll. 1 was the original
# behaviour and left Germany with exactly ONE route-station pair holding 14+
# days of history; unbounded fixed the depth and cost 6.1x the writes. See
# TESTING.md for the measurements behind this number.
MAX_STOPS_PER_TRIP = 3
# Don't write a row for a change smaller than this. Measured on a real day:
# 22% of all DE rows were sub-minute re-predictions - gtfs.de restating the
# same delay in seconds. That is noise for a delay distribution, and it is
# noise that costs a row every time.
MIN_DELAY_CHANGE_MIN = 1.0

# Same guard poland_poller.py and api.py already apply, which germany_poller.py
# was missing - found 2026-08-21 while verifying the cancellation work: a real
# poll wrote "RB33 at Bad Kreuznach: -1299.0", and 7 rows beyond +/-12h had
# landed in a single day. gtfs.de's delay is feed-native (no schedule join, so
# not PL's stop_sequence-renumbering cause) but the feed still occasionally
# publishes a nonsense figure, most often a large negative one. A train is not
# 21 hours early; that is a bad record, and averaging it into a route's
# reliability silently corrupts every statistic built on top of it.
MAX_PLAUSIBLE_DELAY_MIN = 720

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

def load_platform_map():
    # The RT feed's stop_id is always a platform-level child row (see
    # GTFS_STOPS_FILES above), so the platform a train is actually calling at
    # is knowable from the static feed without any extra live data - but only
    # where gtfs.de bothered to fill platform_code, which is 562/9161 child
    # rows (6%). Sparse by source, not by bug: DE simply has far less platform
    # data published than HU (77%) or AT (50%), and the column stays NULL for
    # the rest rather than guessing from the stop name.
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
        # EVERY stop_time_update, not just the first. This diverges from AT's
        # nextStopovers[0] and PL's stu[0] on purpose, because gtfs.de behaves
        # differently from both: it publishes only a trip's REMAINING stops, so
        # the "first" one advances as the train runs. Sampling it alone
        # scattered observations thinly across a journey instead of building
        # depth anywhere - measured before this change, DE held 25,734
        # route-station pairs averaging 3.4 days each and exactly ONE pair with
        # 14+ days, against Poland's 7,544 deep pairs from the same feed
        # format. Per-stop delay distributions cannot be built from that.
        #
        # The write cost is bounded not by this loop but by the dedup below,
        # which is keyed per (trip, stop): a stop is written when its own
        # predicted delay changes, not once per poll.
        # Capped, not unbounded. Sampling every published stop fixed the depth
        # problem and cost 6.1x the write volume - ~1.18M rows/day against
        # 192,380 before, roughly 27 GB/month, which this box cannot carry for
        # long. gtfs.de publishes 6.6 remaining stops per trip on average, so
        # taking the next few keeps most of the coverage benefit at a fraction
        # of the cost: a train passes every one of its stops eventually, so
        # over a journey each stop still gets sampled - just from closer in.
        for stu in trip_update.stop_time_update[:MAX_STOPS_PER_TRIP]:
            _emit_stop(rows, trip_update, gtfs_trip_id, stu)
    return rows

def _emit_stop(rows, trip_update, gtfs_trip_id, stu):
    """Append at most one row for a single stop_time_update."""
    gtfs_stop_id = stu.stop_id

    # gtfs.de never uses the trip-level CANCELED flag (confirmed 0/7229),
    # so a fully cancelled train is only recognizable as one whose stops
    # are *all* SKIPPED - 508 trips in a real poll, against 731 with at
    # least one skipped stop. Both were silently dropped before this: a
    # skipped stop publishes no delay, so it failed the delay check below.
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

    # Split as well as collapsed: a transfer needs the feeder's ARRIVAL
    # and the connector's DEPARTURE, which differ at any stop where the
    # train waits. delay_seconds keeps its original arrival-preferred
    # meaning so delay_minutes and every existing caller are unchanged.
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
        # Needed to turn a scheduled stop_times HH:MM:SS into an actual
        # epoch for position estimation (see resolve_delays) - not used
        # for the delay figure itself, which stays feed-native.
        "start_date": trip_update.trip.start_date or None,
        "cancellation_kind": None,
    })
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

def resolve_delays(cur, rows, parent_map=None, now_epoch=None, platform_map=None):
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
            # Same reasoning as poland_poller: a train that is not calling here
            # has no delay (NULL, not 0, which would read as an on-time
            # arrival in the aggregates), and no position is interpolated
            # toward a stop it will never make.
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
            # See the same field in poland_poller.resolve_delays(): the feed's
            # own trip id, needed by /train/{country}/{id} to locate the rest
            # of this trip's stop_time_updates in a fresh feed.
            "gtfs_trip_id": row["gtfs_trip_id"],
            "is_cancelled": False,
            "cancellation_kind": None,
            "platform": platform,
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
    # Compared as a (delay, cancelled) pair - see the same block in
    # poland_poller.py for why the delay alone is not enough once a row can be
    # cancelled (a cancellation carries delay NULL, and NULL == NULL here
    # would otherwise re-write an identical cancelled row every 5 minutes).
    # Keyed per (trip, stop), not per trip. Since parse_de_vehicles now emits
    # every remaining stop rather than only the current one, a trip-level key
    # would compare a stop's delay against a different stop's last value and
    # write on nearly every comparison - the dedup would stop working exactly
    # when it started mattering. This is also what bounds the extra volume:
    # a stop is written when its own prediction changes, not once per poll.
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
            # Unchanged, or changed by less than a minute - see
            # MIN_DELAY_CHANGE_MIN. A cancellation flag flipping is never
            # suppressed, however small the delay difference.
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
    cur.close()
    conn.close()
    print(f"Saved {count} observations ({cancelled_count} cancelled), skipped {skipped} unchanged.")

if __name__ == "__main__":
    poll()
