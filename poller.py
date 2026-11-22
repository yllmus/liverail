import re
import requests
import psycopg2
from datetime import date

from hu_route_names import clean_route_name, extract_train_number
from trip_resolution import lookup_stop_identity, resolve_ambiguous_trip, local_now
from config import DB_PASSWORD

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": DB_PASSWORD,
    "host": "localhost"
}

COUNTRY_CODE = "HU"

GRAPHQL_URL = "https://mavplusz.hu/otp2-backend/otp/routers/default/index/graphql"

QUERY = """
{
  vehiclePositions(
    swLat: 45.7, swLon: 16.0,
    neLat: 48.6, neLon: 22.9,
    modes: [RAIL, RAIL_REPLACEMENT_BUS, SUBURBAN_RAILWAY, TRAMTRAIN]
  ) {
    lat
    lon
    trip { gtfsId tripShortName tripHeadsign route { longName } }
    stopRelationship { stop { gtfsId name platformCode } }
    prevOrCurrentStop { arrivalDelay departureDelay realtimeState }
  }
}
"""

def parse_vehicles(data):
    vehicles = data.get("data", {}).get("vehiclePositions", [])
    rows = []
    for v in vehicles:
        trip = v.get("trip")
        stop_rel = v.get("stopRelationship")
        prev = v.get("prevOrCurrentStop")
        if not trip or not stop_rel or not prev:
            continue
        stop = stop_rel.get("stop") or {}
        stop_name = stop.get("name")
        route_name = clean_route_name(trip.get("route", {}).get("longName"), trip.get("tripShortName"))
        # Split as well as collapsed. A transfer needs the feeder's ARRIVAL
        # and the connector's DEPARTURE and they differ wherever a train
        # waits at a stop; OTP publishes both and this was discarding one.
        # delay_seconds keeps its exact original meaning (including the `or`,
        # which also treats a literal 0 as absent) so delay_minutes and every
        # existing caller are untouched.
        arrival_delay = prev.get("arrivalDelay")
        departure_delay = prev.get("departureDelay")
        delay_seconds = arrival_delay or departure_delay
        # OTP's realtimeState enum includes CANCELED, and MAV's endpoint
        # accepts omitCanceled:false on its departure boards - but a
        # vehiclePositions query can only describe trains that are physically
        # running, so a cancelled service is absent rather than flagged, and a
        # 6,124-stoptime sample of MAV's own board returned zero CANCELED.
        # Read anyway (it costs one field on a call already being made) so we
        # record cancellations the day MAV starts publishing them - but HU is
        # not claimed as having real cancellation coverage. See
        # schema/005_cancellations_platform.sql.
        is_cancelled = prev.get("realtimeState") == "CANCELED"
        if not stop_name or (delay_seconds is None and not is_cancelled):
            continue
        rows.append({
            "stop_name": stop_name,
            "route_name": route_name,
            "delay_minutes": None if is_cancelled else round(delay_seconds / 60, 2),
            "delay_arrival_minutes": None if (is_cancelled or arrival_delay is None) else round(arrival_delay / 60, 2),
            "delay_departure_minutes": None if (is_cancelled or departure_delay is None) else round(departure_delay / 60, 2),
            "is_cancelled": is_cancelled,
            "cancellation_kind": "stop" if is_cancelled else None,
            # Best-filled platform data of the four countries (33/43 vehicles
            # in a real sample) - it was on the stop object all along and
            # simply never requested.
            "platform": stop.get("platformCode"),
            # Destination text and live coordinates — both come straight off
            # this same GraphQL response but were being discarded before;
            # confirmed present on the real endpoint (tripHeadsign, lat/lon)
            # before wiring them in. Either can legitimately be missing
            # (some entries don't carry a headsign) so these stay nullable,
            # unlike stop_name/route_name/delay_minutes which gate inclusion
            # above.
            "headsign": trip.get("tripHeadsign"),
            "lat": v.get("lat"),
            "lon": v.get("lon"),
            # OTP's live gtfsId, e.g. "1:32566556" - confirmed (2026-08-02)
            # this does NOT match trips.gtfs_trip_id for any sampled trip in
            # our imported static feed (checked directly against the real
            # endpoint + DB, 0/4 matched, exact and substring). Stored as an
            # opaque, unresolved identifier for now; it isn't joinable to
            # `trips`.
            "live_trip_id": trip.get("gtfsId"),
            # See resolve_trip_id() below - a second, independent identity
            # path that (unlike live_trip_id/gtfsId) can sometimes actually
            # join to our static trips table.
            "train_number": extract_train_number(trip.get("tripShortName")),
        })
    return rows

def resolve_trip_id(cur, number, station_name=None, delay_minutes=None, now=None):
    # MAV's live gtfsId (e.g. "1:33736070") is an OTP-internal identifier -
    # confirmed directly against the live endpoint + our imported static
    # feed that it shares no formatting or numbering scheme with
    # trips.gtfs_trip_id at all (not a staleness issue - the two id spaces
    # are structurally unrelated), so it can never be string-matched. This
    # is the alternative: match the train's real public number (from
    # tripShortName, see extract_train_number) against routes.name, which
    # the static feed embeds inconsistently - as a prefix ("Sz 5827"), a
    # suffix ("9609 HELIKON"), or completely bare ("9939") depending on
    # category - but always as a standalone digit token. Restricted to
    # route_type='2' (rail) so a train number doesn't collide with a
    # "Pótló" rail-replacement-BUS route reusing the same number.
    #
    # First pass (2026-08-20, pre-calendar) resolved ~25% (77/314) - a bare
    # number match, alone, is ambiguous for ~38% of vehicles because most HU
    # routes are imported one-per-LINE (e.g. "S76" covers every physical
    # train on the line all day) and MAV's own trip_short_name column is
    # 0/34,349 filled, so there was nothing to disambiguate by time of day.
    # Importing stop_times.txt didn't fix that alone: candidates for the same
    # number turned out to have *identical* scheduled times (they're one
    # physical service duplicated once per GTFS service_id/calendar
    # pattern). Importing calendar.txt (service_calendar table) and matching
    # against "which service runs today", then falling back to nearest
    # scheduled time at the observed station if still tied
    # (resolve_ambiguous_trip, trip_resolution.py), raised this to 60.5%.
    # The remaining ~40% is a real, confirmed structural gap: mostly HÉV
    # lines (H6/H7/H8/...), a different agency not in MAV's own GTFS feed at
    # all, plus genuine schedule drift against the one-time static snapshot.
    if not number:
        return None
    cur.execute("""
        SELECT DISTINCT t.id
        FROM routes r JOIN trips t ON t.route_id = r.id
        WHERE r.country_code = %s AND r.route_type = '2'
          AND r.name ~ %s
    """, (COUNTRY_CODE, r"(^|[^0-9])" + re.escape(number) + r"([^0-9]|$)"))
    matches = [m[0] for m in cur.fetchall()]
    # Ambiguous-match narrowing (same-day calendar, then nearest scheduled
    # time at the observed station) is shared with austria_poller.py - see
    # trip_resolution.py.
    station_names = [station_name] if station_name else None
    return resolve_ambiguous_trip(cur, COUNTRY_CODE, matches, station_names, delay_minutes, now)

def poll():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Polling MÁV...")
    resp = requests.post(GRAPHQL_URL, json={"query": QUERY}, timeout=30)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        return
    rows = parse_vehicles(resp.json())
    print(f"Got {len(rows)} vehicles")
    count = 0
    resolved_count = 0
    unresolved_station = 0
    for row in rows:
        state = "CANCELLED" if row["is_cancelled"] else row["delay_minutes"]
        print(f"  {row['route_name']} at {row['stop_name']}: {state}")
        # Real GPS straight off the live feed (not the schedule-interpolated
        # estimate PL/DE fall back to) - tagged explicitly so a consumer
        # never has to guess which kind of position a row holds.
        position_source = "live" if row["lat"] is not None else None
        trip_id = resolve_trip_id(cur, row["train_number"], row["stop_name"], row["delay_minutes"], local_now(COUNTRY_CODE))
        if trip_id is not None:
            resolved_count += 1
        # Which stop of the journey this is, and when it was due. Only
        # knowable once trip_id resolved - MAV's live feed identifies the stop
        # by name, not by position. (OTP does publish prevOrCurrentStop.
        # stopPosition, but that is an OTP pattern index and HU's static
        # stop_sequence values are sparse - up to 99,990, and only 56 of 2,000
        # sampled trips number consecutively - so the two are different
        # numbering schemes and must not be conflated. See
        # schema/008_stop_identity.sql.)
        stop_sequence, sched_arrival, sched_departure = lookup_stop_identity(
            cur, COUNTRY_CODE, trip_id, [row["stop_name"]])
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, headsign, lat, lon, position_source, live_trip_id, trip_id, cancellation_kind, platform,
                 stop_sequence, scheduled_arrival, scheduled_departure, delay_arrival_minutes, delay_departure_minutes)
            SELECT s.id, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            FROM stations s
            WHERE s.name = %s AND s.country_code = %s
        """, (
            date.today(), row["delay_minutes"], row["is_cancelled"], row["route_name"], COUNTRY_CODE,
            row["headsign"], row["lat"], row["lon"], position_source, row["live_trip_id"], trip_id,
            row["cancellation_kind"], row["platform"],
            stop_sequence, sched_arrival, sched_departure,
            row.get("delay_arrival_minutes"), row.get("delay_departure_minutes"),
            row["stop_name"], COUNTRY_CODE,
        ))
        # cur.rowcount, not an unconditional increment. The INSERT is a
        # SELECT against `stations`, so an unknown station name inserts NOTHING
        # and the old count still claimed it as saved. That is how 7.8% of
        # Hungarian observations - every HEV line, H5 through H9 - were parsed,
        # discarded and reported as written, for as long as HU has been
        # running. Counting what the database actually accepted is the only
        # version of this number worth printing.
        if cur.rowcount:
            count += 1
        else:
            unresolved_station += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations, {resolved_count} with a resolved trip_id, "
          f"{unresolved_station} dropped for an unknown station.")

if __name__ == "__main__":
    poll()
