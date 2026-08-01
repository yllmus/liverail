import requests
import psycopg2
from datetime import date
from google.transit import gtfs_realtime_pb2

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": "changeme",
    "host": "localhost"
}

COUNTRY_CODE = "PL"

GTFS_RT_URL = "https://mkuran.pl/gtfs/polish_trains/updates.pb"

def fetch_feed():
    resp = requests.get(GTFS_RT_URL, timeout=30)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed

def parse_pl_vehicles(feed):
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        if not trip_update.stop_time_update:
            continue
        stop_time_update = trip_update.stop_time_update[0]
        gtfs_stop_id = stop_time_update.stop_id
        gtfs_route_id = trip_update.trip.route_id
        if stop_time_update.HasField("arrival") and stop_time_update.arrival.HasField("delay"):
            delay_seconds = stop_time_update.arrival.delay
        elif stop_time_update.HasField("departure") and stop_time_update.departure.HasField("delay"):
            delay_seconds = stop_time_update.departure.delay
        else:
            continue
        if not gtfs_stop_id or not gtfs_route_id:
            continue
        rows.append({
            "gtfs_stop_id": gtfs_stop_id,
            "gtfs_route_id": gtfs_route_id,
            "delay_minutes": round(delay_seconds / 60, 2),
        })
    return rows

def poll():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Polling PKP...")
    feed = fetch_feed()
    rows = parse_pl_vehicles(feed)
    print(f"Got {len(rows)} vehicles")
    count = 0
    for row in rows:
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code)
            SELECT s.id, %s, NOW(), %s, false, r.name, %s
            FROM stations s
            JOIN routes r ON r.gtfs_route_id = %s AND r.country_code = %s
            WHERE s.gtfs_stop_id = %s AND s.country_code = %s
        """, (date.today(), row["delay_minutes"], COUNTRY_CODE,
              row["gtfs_route_id"], COUNTRY_CODE, row["gtfs_stop_id"], COUNTRY_CODE))
        count += cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations.")

if __name__ == "__main__":
    poll()
