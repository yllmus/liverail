import re
import requests
import psycopg2
from datetime import date

from hu_route_names import clean_route_name, extract_train_number
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
    stopRelationship { stop { gtfsId name } }
    prevOrCurrentStop { arrivalDelay departureDelay }
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
        stop_name = stop_rel.get("stop", {}).get("name")
        route_name = clean_route_name(trip.get("route", {}).get("longName"), trip.get("tripShortName"))
        delay_seconds = prev.get("arrivalDelay") or prev.get("departureDelay")
        if not stop_name or delay_seconds is None:
            continue
        rows.append({
            "stop_name": stop_name,
            "route_name": route_name,
            "delay_minutes": round(delay_seconds / 60, 2),
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
            # opaque, unresolved identifier for now - see NOTES.md "Live
            # per-train identity" for why this isn't joined to `trips`.
            "live_trip_id": trip.get("gtfsId"),
            # See resolve_trip_id() below - a second, independent identity
            # path that (unlike live_trip_id/gtfsId) can sometimes actually
            # join to our static trips table.
            "train_number": extract_train_number(trip.get("tripShortName")),
        })
    return rows

def resolve_trip_id(cur, number):
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
    # Verified against a real 314-vehicle live sample (2026-08-20): this
    # resolves a single unambiguous trip for ~25% of vehicles (77/314).
    # The rest don't get a guess - left NULL, same graceful-skip convention
    # as PL/AT/DE's unresolved rows - because the other ~75% is a real,
    # confirmed structural gap this query can't close:
    #   - ~37% (116/314): no route in our static import contains the
    #     number at all. Partly HÉV lines (H6/H7/H8/...), which aren't in
    #     MAV's own GTFS feed - a different agency - and partly genuine
    #     schedule drift against our one-time static snapshot.
    #   - ~38% (121/314): the number matches more than one candidate trip.
    #     Most HU routes are imported one-per-LINE (e.g. "S76" covers every
    #     physical train on that line all day), not one-per-physical-train,
    #     and MAV's own trip_short_name column is 0/34,349 filled in the
    #     source feed itself - there's no calendar/stop_times data imported
    #     for HU to disambiguate further by time of day. Closing this gap
    #     for real would mean importing HU's stop_times.txt (already on
    #     disk, unused) and matching by scheduled time, mirroring PL's
    #     architecture - not attempted here, real follow-on scope.
    if not number:
        return None
    cur.execute("""
        SELECT DISTINCT t.id
        FROM routes r JOIN trips t ON t.route_id = r.id
        WHERE r.country_code = %s AND r.route_type = '2'
          AND r.name ~ %s
    """, (COUNTRY_CODE, r"(^|[^0-9])" + re.escape(number) + r"([^0-9]|$)"))
    matches = cur.fetchall()
    return matches[0][0] if len(matches) == 1 else None

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
    for row in rows:
        print(f"  {row['route_name']} at {row['stop_name']}: {row['delay_minutes']}")
        # Real GPS straight off the live feed (not the schedule-interpolated
        # estimate PL/DE fall back to) - tagged explicitly so a consumer
        # never has to guess which kind of position a row holds.
        position_source = "live" if row["lat"] is not None else None
        trip_id = resolve_trip_id(cur, row["train_number"])
        if trip_id is not None:
            resolved_count += 1
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, headsign, lat, lon, position_source, live_trip_id, trip_id)
            SELECT s.id, %s, NOW(), %s, false, %s, %s, %s, %s, %s, %s, %s, %s
            FROM stations s
            WHERE s.name = %s AND s.country_code = %s
        """, (
            date.today(), row["delay_minutes"], row["route_name"], COUNTRY_CODE,
            row["headsign"], row["lat"], row["lon"], position_source, row["live_trip_id"], trip_id,
            row["stop_name"], COUNTRY_CODE,
        ))
        count += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations, {resolved_count} with a resolved trip_id.")

if __name__ == "__main__":
    poll()
