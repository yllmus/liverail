import re
import requests
import psycopg2
from datetime import date

from hu_route_names import clean_route_name, extract_train_number
from trip_resolution import lookup_stop_identity, resolve_ambiguous_trip, local_now
from config import DB_CONFIG

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
        # Keep arrival and departure delays separately: a transfer needs the
        # feeder's arrival and the connector's departure, and they differ
        # wherever a train waits. The `or` deliberately treats 0 as missing.
        arrival_delay = prev.get("arrivalDelay")
        departure_delay = prev.get("departureDelay")
        delay_seconds = arrival_delay or departure_delay
        # vehiclePositions only lists trains that are running, so a cancelled
        # train is usually absent rather than flagged. The field is read anyway
        # in case MÁV starts publishing it.
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
            "platform": stop.get("platformCode"),
            # Either can be missing, so unlike the fields above they do not
            # gate inclusion.
            "headsign": trip.get("tripHeadsign"),
            "lat": v.get("lat"),
            "lon": v.get("lon"),
            # OTP's own trip id. It does not exist in the static GTFS, so it is
            # stored as an opaque id and only used to follow a train between
            # polls.
            "live_trip_id": trip.get("gtfsId"),
            "train_number": extract_train_number(trip.get("tripShortName")),
        })
    return rows

def resolve_trip_id(cur, number, station_name=None, delay_minutes=None, now=None):
    # OTP's live trip ids share nothing with the static feed's ids, so the join
    # uses the train's public number instead. The static feed embeds it in
    # routes.name as a prefix ("Sz 5827"), a suffix ("9609 HELIKON") or bare
    # ("9939"), always as a standalone run of digits. Rail routes only, so a
    # rail replacement bus with the same number cannot match.
    #
    # A number alone often matches several trips, so the candidates are
    # narrowed in trip_resolution.py. Trains that are not in MÁV's static feed
    # at all, such as the HÉV suburban lines, stay unresolved. See
    # docs/data-sources.md.
    if not number:
        return None
    cur.execute("""
        SELECT DISTINCT t.id
        FROM routes r JOIN trips t ON t.route_id = r.id
        WHERE r.country_code = %s AND r.route_type = '2'
          AND r.name ~ %s
    """, (COUNTRY_CODE, r"(^|[^0-9])" + re.escape(number) + r"([^0-9]|$)"))
    matches = [m[0] for m in cur.fetchall()]
    station_names = [station_name] if station_name else None
    return resolve_ambiguous_trip(cur, COUNTRY_CODE, matches, station_names, delay_minutes, now)

def poll():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
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
            # Real GPS position, unlike the interpolated estimate used for
            # Poland and Germany.
            position_source = "live" if row["lat"] is not None else None
            trip_id = resolve_trip_id(cur, row["train_number"], row["stop_name"], row["delay_minutes"], local_now(COUNTRY_CODE))
            if trip_id is not None:
                resolved_count += 1
            # Which stop of the journey this is and when it was due. MÁV names
            # the stop rather than numbering it, so this needs a resolved
            # trip_id. OTP's stopPosition is a different numbering from GTFS
            # stop_sequence and must not be used for it.
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
            # Count rows the database accepted. The INSERT selects from
            # stations, so an unknown station name writes nothing, and counting
            # attempts would report those rows as saved.
            if cur.rowcount:
                count += 1
            else:
                unresolved_station += 1
        conn.commit()
        print(f"Saved {count} observations, {resolved_count} with a resolved trip_id, "
              f"{unresolved_station} dropped for an unknown station.")
    finally:
        conn.close()

if __name__ == "__main__":
    poll()
