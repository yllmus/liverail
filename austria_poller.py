import json
import subprocess
import psycopg2
from datetime import date

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": "changeme",
    "host": "localhost"
}

COUNTRY_CODE = "AT"

def call_radar():
    result = subprocess.run(
        ["node", "hafas/radar.js"],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        print(f"radar.js error: {result.stderr.strip()}")
        return []
    return json.loads(result.stdout)

def parse_at_vehicles(movements):
    rows = []
    for m in movements or []:
        line = m.get("line") or {}
        route_name = line.get("name")
        stopovers = m.get("nextStopovers") or []
        if not route_name or not stopovers:
            continue
        stop = stopovers[0]
        stop_name = (stop.get("stop") or {}).get("name")
        delay_seconds = stop.get("arrivalDelay")
        if delay_seconds is None:
            delay_seconds = stop.get("departureDelay")
        if not stop_name or delay_seconds is None:
            continue
        rows.append({
            "stop_name": stop_name,
            "route_name": route_name,
            "delay_minutes": round(delay_seconds / 60, 2),
        })
    return rows

def poll():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Polling ÖBB...")
    movements = call_radar()
    rows = parse_at_vehicles(movements)
    print(f"Got {len(rows)} vehicles")
    count = 0
    for row in rows:
        print(f"  {row['route_name']} at {row['stop_name']}: {row['delay_minutes']}")
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code)
            SELECT s.id, %s, NOW(), %s, false, %s, %s
            FROM stations s
            WHERE s.name = %s AND s.country_code = %s
        """, (date.today(), row["delay_minutes"], row["route_name"], COUNTRY_CODE, row["stop_name"], COUNTRY_CODE))
        count += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations.")

if __name__ == "__main__":
    poll()
