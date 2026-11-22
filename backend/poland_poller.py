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
    # Pure extraction, no DB. Confirmed against the real feed: mkuran.pl's
    # TripUpdates never set stop_id, route_id, or a delay field - only
    # stop_sequence and absolute Unix timestamps (arrival.time/departure.time).
    # Resolving a station/schedule and computing delay needs the paired
    # static feed's stop_times (imported via `import.py --with-stop-times`),
    # done separately in resolve_delays() so this stays DB-independent and
    # unit-testable, same as parse_vehicles()/parse_at_vehicles().
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

        # A cancelled trip carries no stop_time_updates at all (confirmed:
        # 55/55 of the feed's CANCELED trips had zero), so the
        # stop_time_update requirement below used to drop every single one -
        # the whole reason is_cancelled was never once written. There is no
        # "current stop" to attribute it to either; resolve_delays() uses the
        # trip's scheduled origin instead.
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
        # The stop the train is actually AT, not stop_time_update[0].
        #
        # [0] was the original choice, by analogy with AT's nextStopovers[0].
        # It is wrong for this feed, and measurably so: mkuran.pl publishes the
        # WHOLE trip starting at stop_sequence 0, including trips that do not
        # depart for another two or three hours. So [0] is always the trip's
        # ORIGIN. It never advances as the train runs, which had two
        # consequences:
        #   * the recorded "delay" was the delay at the origin - frequently a
        #     forecast for a train that had not left yet - rather than where
        #     the train was;
        #   * every trip produced exactly one distinct stop per day. Poland
        #     held 6.45M observations across 182,682 trip-days at 1.00 stops
        #     each, so the country with the most data had no journeys in it at
        #     all, and no per-train forecast could be built from it.
        # Germany never had this because its feed publishes only REMAINING
        # stops, so its [0] advances naturally.
        #
        # Picking the next stop whose time has not yet passed tracks the train
        # along its route, one row per stop it actually reaches.
        stu = _current_stop_update(trip_update.stop_time_update, now_epoch)

        # A SKIPPED stop means the train runs but is not calling here. It has
        # no arrival/departure time (there is no arrival to predict), so it
        # would fail the actual_epoch check below and be discarded - which is
        # exactly the information a rider waiting at that stop needs most.
        if stu.schedule_relationship == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED:
            rows.append({
                "gtfs_trip_id": gtfs_trip_id,
                "start_date": start_date,
                "stop_sequence": stu.stop_sequence,
                "actual_epoch": None,
                "cancellation_kind": "stop",
            })
            continue

        # Arrival and departure are kept apart as well as collapsed. A
        # transfer needs the feeder's ARRIVAL and the connector's DEPARTURE,
        # and they are genuinely different numbers at a stop where a train
        # waits. actual_epoch keeps its original meaning (arrival preferred,
        # departure as fallback) so delay_minutes and every existing caller
        # are untouched.
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

def _interpolate_position(prev_lat, prev_lon, prev_epoch, target_lat, target_lon, target_epoch, now_epoch):
    # Pure math, no DB/time.time() dependency (now_epoch is a parameter, not
    # read internally) so this is directly unit-testable. No live GPS exists
    # for PL - confirmed directly (mkuran.pl's own file listing has no
    # position feed alongside updates.pb for polish_trains specifically,
    # its README states live positions "may be available sometime in the
    # future", and PKP PLK's own full OpenAPI schema - the same one their
    # official passenger portal calls - has zero lat/lon/coordinate fields
    # anywhere, checked field-by-field). This estimates a marker between the
    # previous scheduled stop and the target one, proportional to elapsed vs
    # scheduled travel time - the same technique Deutsche Bahn's own
    # official "Zugradar" used for years (it also wasn't real GPS) before
    # being retired, not a fabricated position. Callers must label this
    # distinctly from HU/AT's real GPS - see position_source.
    if prev_lat is None or prev_lon is None or prev_epoch is None or target_epoch <= prev_epoch:
        return None, None
    fraction = max(0.0, min(1.0, (now_epoch - prev_epoch) / (target_epoch - prev_epoch)))
    lat = prev_lat + fraction * (target_lat - prev_lat)
    lon = prev_lon + fraction * (target_lon - prev_lon)
    return lat, lon

def resolve_delays(cur, rows, now_epoch=None):
    # DB-dependent step. Joins each raw (trip_id, stop_sequence) against our
    # stop_times table to get the scheduled stop and scheduled time, then
    # delay = actual - scheduled. Silently resolves to nothing if the trip
    # isn't in our PL trips (unimported/stale) or the stop_id it references
    # was one of the platform-level rows import.py's parent_station filter
    # excluded from stations - same graceful-skip behavior as HU/AT.
    if now_epoch is None:
        now_epoch = time.time()
    resolved = []
    for row in rows:
        kind = row.get("cancellation_kind")
        if kind == "trip":
            # No stop_sequence to match on - a cancelled trip publishes no
            # stop_time_updates. Attribute it to the trip's scheduled origin,
            # which is where a rider would have been standing.
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
        # stop_sequence comes back from OUR stop_times rather than being
        # echoed from the feed. For a stop-level row the two are equal by
        # construction (the query matched on it); for a trip cancellation the
        # feed publishes none at all and this is the only way to record which
        # stop the row was attributed to.
        (station_id, sched_arrival, sched_departure, station_name,
         target_lat, target_lon, route_name, headsign, trip_id,
         stop_sequence) = match

        if kind:
            # A train that is not calling here has no delay - not zero. Writing
            # 0 would count every cancellation as an on-time arrival in the
            # reliability aggregates, and no position is estimated either
            # (interpolating toward a stop the train will never make would
            # draw a marker for a journey that isn't happening).
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

        # A "delay" beyond 12 hours is a schedule-version mismatch, not a late
        # train. This feed's stop_sequence numbering usually matches our
        # imported static snapshot (measured: 399/400 sampled trips have every
        # feed sequence inside the static trip's own range), but a few trips -
        # international ones especially - get renumbered upstream, so the
        # sequence-to-sequence match above pairs the wrong stops and produces
        # ~20-hour figures. One real example, PLK_LEO_2026_516260411, is
        # numbered 15-31 in the feed and 0-16 in stop_times. These were being
        # written to history as real observations (they are why PL's worst_delay
        # reads 1182 minutes); skipped now, same graceful-skip convention as
        # every other row this function can't resolve.
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

        # Same plausibility guard as delay_minutes above - a split delay
        # derived from a mismatched stop_sequence is wrong in exactly the
        # same way, and must not slip into history through a new column.
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
            # The feed's own trip id, carried through unresolved alongside our
            # integer trips.id. api.py's /train/{country}/{id} needs it to find
            # this exact trip again in a freshly-fetched feed (the only place
            # the rest of the journey's stop_time_updates live) - our own
            # trip_id can't address the feed.
            "gtfs_trip_id": row["gtfs_trip_id"],
            "is_cancelled": False,
            "cancellation_kind": None,
            # PL is the one country with no platform data anywhere: the feed
            # sets no stop_id, stop_time_properties.assigned_stop_id is
            # 0/15231, and the static child rows carry no platform_code
            # either. Present and always None so every country's resolved row
            # has the same shape.
            "platform": None,
        })
    # mkuran.pl's live feed can legitimately carry two simultaneously-active
    # entities for the same recurring gtfs_trip_id at once (e.g. an overnight
    # service still running under yesterday's start_date, plus today's
    # edition of the same daily service already appearing) - confirmed
    # against the real feed: distinct entity.id, distinct (trip_id,
    # start_date) pairs, so this isn't a malformed/duplicated feed. But
    # trips/stop_times hold no per-service-day rows, only a single static
    # schedule per gtfs_trip_id, so both entities resolve to an identical
    # station/route/headsign here and would otherwise be written as
    # indistinguishable duplicate rows (confirmed: ~4-6% of a poll's rows).
    # Properly attributing each to its own service-day instance needs a
    # schema that records start_date, which delay_observations doesn't -
    # until then, collapse to one row per trip_id rather than double-count.
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

        # mkuran.pl's first stop_time_update for a trip stays the same predicted
        # stop for many consecutive polls until the train actually reaches it -
        # writing a fresh row every 5 min regardless was the dominant driver of
        # DB growth (confirmed: ~1.5-1.8M PL rows/day, ~193x more than the
        # distinct (trip_id, delay_minutes) pairs actually observed). Only write
        # a new row when the delay for a trip has actually changed since its
        # last recorded observation.
        # Scoped to TODAY, which the first version of this wasn't - and that was a
        # real data-integrity bug, not a nuance. 92.8% of PL trip_ids recur across
        # days (DE 43.2%, both measured), and ~71% of PL rows are exactly 0.0
        # minutes, so an unscoped "has this trip's delay changed since its last
        # observation" meant a train that runs punctually every day was recorded
        # once and then never again - every later day silently vanished. Comparing
        # within the service day gives each trip at least one row per day while
        # still collapsing the repeat-every-5-minutes noise this was written for.
        # Compared as a (delay, cancelled) pair, not delay alone: a trip that gets
        # cancelled goes from some delay to delay NULL, and a NULL never equals
        # anything in Python's `==` sense against a float - but a trip already
        # recorded as cancelled would otherwise re-write an identical row every
        # 5 minutes for the rest of the day, since NULL == NULL is True here.
        # Pairing them keeps both transitions (running -> cancelled and cancelled
        # -> running again) as real, recorded events while still collapsing the
        # repeat-every-poll noise this dedup was written for.
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
            # Keyed on (trip, STATION), not on the trip alone.
            #
            # Keying on the trip alone skipped the insert whenever the delay was
            # unchanged - including when the train had moved on to a completely
            # different stop. The row that would have recorded "this train reached
            # the next station, still 4 minutes down" was thrown away as a
            # duplicate, so Poland held 6.45M observations and exactly ZERO
            # journeys: 182,682 trip-days, every one of them at 1.0 distinct stops.
            # That is the whole trajectory signal, and no forecast can be built
            # without it.
            #
            # Germany never had this problem because its dedup already keyed on
            # (trip_id, stop_sequence), which is why DE is the only country with
            # usable trajectories today. Genuinely redundant repeats - same train,
            # same stop, same delay - are still skipped, so this adds at most one
            # row per stop a train actually calls at.
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
