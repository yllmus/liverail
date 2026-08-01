import requests
import psycopg2
from datetime import date

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": "changeme",
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
    trip { gtfsId tripHeadsign route { longName } }
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
        route_name = trip.get("route", {}).get("longName")
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
        })
    return rows

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
    for row in rows:
        print(f"  {row['route_name']} at {row['stop_name']}: {row['delay_minutes']}")
        # Real GPS straight off the live feed (not the schedule-interpolated
        # estimate PL/DE fall back to) - tagged explicitly so a consumer
        # never has to guess which kind of position a row holds.
        position_source = "live" if row["lat"] is not None else None
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, headsign, lat, lon, position_source)
            SELECT s.id, %s, NOW(), %s, false, %s, %s, %s, %s, %s, %s
            FROM stations s
            WHERE s.name = %s AND s.country_code = %s
        """, (
            date.today(), row["delay_minutes"], row["route_name"], COUNTRY_CODE,
            row["headsign"], row["lat"], row["lon"], position_source, row["stop_name"], COUNTRY_CODE,
        ))
        count += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations.")

if __name__ == "__main__":
    poll()
