import json
import re
import subprocess
import psycopg2
from datetime import date

from config import DB_PASSWORD

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": DB_PASSWORD,
    "host": "localhost"
}

COUNTRY_CODE = "AT"

def station_name_candidates(name):
    # ÖBB's live HAFAS names and its own static GTFS station names disagree
    # on formatting for a meaningful chunk of stations: "Hbf" vs "Hauptbahnhof"
    # (affects ~20 major-city main stations), a trailing " Bahnhof"/" Bahnhst"
    # HAFAS sometimes adds that GTFS's station-level name doesn't have, and
    # parenthetical platform/entrance qualifiers HAFAS appends (e.g.
    # "Wien Hbf (Bahnsteige 3-12)"). Try the exact name first, then fall back
    # to normalized variants rather than dropping the observation.
    candidates = [name]
    stripped = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
    if stripped != name:
        candidates.append(stripped)
    for base in (name, stripped):
        if base.endswith(" Hbf"):
            candidates.append(base[: -len(" Hbf")] + " Hauptbahnhof")
        candidates.append(re.sub(r"\s+(Bahnhof|Bahnhst)$", "", base))
    # dedupe while preserving priority order (exact match first)
    seen = set()
    result = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            result.append(c)
    return result

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
        if line.get("mode") != "train":
            continue
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
        location = m.get("location") or {}
        rows.append({
            "stop_name": stop_name,
            "route_name": route_name,
            "delay_minutes": round(delay_seconds / 60, 2),
            # HAFAS's radar movements already carry a destination string
            # (direction) and a live vehicle coordinate (location) - both
            # confirmed 100% filled across a real sample of 122 train
            # movements - but were being discarded before this. Nullable
            # here regardless, same convention as HU's headsign/lat/lon.
            "headsign": m.get("direction"),
            "lat": location.get("latitude"),
            "lon": location.get("longitude"),
            # HAFAS's own composite tripId, e.g. "1|12345|0|86|20260801" -
            # this is a HAFAS-internal identifier, not a GTFS trip_id, and
            # structurally can never be joined against trips.gtfs_trip_id
            # (confirmed: AT's static trip ids look like
            # "1.TA.1-80-j26-1.1.H" - disjoint namespace, not a formatting
            # mismatch). Stored opaque/unresolved, same convention as HU -
            # see NOTES.md "Live per-train identity".
            "live_trip_id": m.get("tripId"),
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
        # Real GPS straight off HAFAS's radar (not the schedule-interpolated
        # estimate PL/DE fall back to) - tagged explicitly so a consumer
        # never has to guess which kind of position a row holds.
        position_source = "live" if row["lat"] is not None else None
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, headsign, lat, lon, position_source, live_trip_id)
            SELECT s.id, %s, NOW(), %s, false, %s, %s, %s, %s, %s, %s, %s
            FROM stations s
            WHERE s.name = ANY(%s) AND s.country_code = %s
            ORDER BY array_position(%s, s.name)
            LIMIT 1
        """, (
            date.today(), row["delay_minutes"], row["route_name"], COUNTRY_CODE,
            row["headsign"], row["lat"], row["lon"], position_source, row["live_trip_id"],
            station_name_candidates(row["stop_name"]), COUNTRY_CODE,
            station_name_candidates(row["stop_name"]),
        ))
        count += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations.")

if __name__ == "__main__":
    poll()
