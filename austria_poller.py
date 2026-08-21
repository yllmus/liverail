import json
import re
import subprocess
import psycopg2
from datetime import date

from at_route_names import extract_at_train_number
from trip_resolution import resolve_ambiguous_trip, local_now
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

def resolve_trip_id(cur, number, stop_name=None, delay_minutes=None, now=None):
    # AT's live radar() tripId is a HAFAS-internal composite key with no
    # relationship to trips.gtfs_trip_id (see NOTES.md "Live per-train
    # identity" - confirmed structural, not a staleness issue, same as HU's
    # gtfsId problem). NOTES.md had concluded AT was unfixable on the
    # strength of HAFAS's line.fahrtNr being 0/652 filled - but that's a
    # different field. The live line *name* itself ("RJX 639") already
    # carries the train's public number, in the same "CATEGORY NUMBER"
    # order as the static feed's own trip_short_name (confirmed: 14,690/
    # 15,165 AT trips have it filled, e.g. "RJ 658", "REX 7013") - so the
    # same number-then-calendar-then-schedule resolution HU uses (see
    # poller.py's resolve_trip_id, factored into trip_resolution.py) applies
    # here too, just matched against trip_short_name instead of routes.name.
    if not number:
        return None
    cur.execute("""
        SELECT id FROM trips
        WHERE country_code = %s AND trip_short_name ~ %s
    """, (COUNTRY_CODE, r"(^|[^0-9])" + re.escape(number) + r"([^0-9]|$)"))
    matches = [m[0] for m in cur.fetchall()]
    station_names = station_name_candidates(stop_name) if stop_name else None
    return resolve_ambiguous_trip(cur, COUNTRY_CODE, matches, station_names, delay_minutes, now)

def call_radar():
    result = subprocess.run(
        ["node", "hafas/radar.js"],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        print(f"radar.js error: {result.stderr.strip()}")
        return []
    return json.loads(result.stdout)

def call_trip(trip_id, line_name=None):
    # One train's full journey, for api.py's /train/AT/{id}. Kept here next to
    # call_radar() so both Node subprocess invocations live in one place and
    # share the same relative-path/timeout convention. radar() alone can't
    # serve a train tracker: its nextStopovers is bounded by the radar call's
    # own duration window (~4 stops in practice), while trip() returns every
    # stopover from origin to destination.
    result = subprocess.run(
        ["node", "hafas/trip.js", trip_id, line_name or ""],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        print(f"trip.js error: {result.stderr.strip()}")
        return None
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
        # hafas-client sets `cancelled` on a stopover from HAFAS's own
        # aCncl/dCncl flags, and only when true (absent means not cancelled).
        # 0/40 train movements in a real night sample - like HU, this is wired
        # so a real cancellation is recorded if OBB publishes one, not because
        # AT is known to have coverage. See
        # schema/005_cancellations_platform.sql.
        is_cancelled = bool(stop.get("cancelled"))
        if not stop_name or (delay_seconds is None and not is_cancelled):
            continue
        location = m.get("location") or {}
        rows.append({
            "stop_name": stop_name,
            "route_name": route_name,
            "delay_minutes": None if is_cancelled else round(delay_seconds / 60, 2),
            "is_cancelled": is_cancelled,
            "cancellation_kind": "stop" if is_cancelled else None,
            # Real-time platform where HAFAS knows it, falling back to the
            # planned one (20/40 train movements carried either in a real
            # sample). radar() already returned all four fields; they were
            # simply discarded.
            "platform": (stop.get("departurePlatform") or stop.get("arrivalPlatform")
                         or stop.get("plannedDeparturePlatform") or stop.get("plannedArrivalPlatform")),
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
            # See resolve_trip_id() below - a second, independent identity
            # path that (unlike live_trip_id/tripId) can sometimes actually
            # join to our static trips table.
            "train_number": extract_at_train_number(route_name),
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
    resolved_count = 0
    for row in rows:
        state = "CANCELLED" if row["is_cancelled"] else row["delay_minutes"]
        print(f"  {row['route_name']} at {row['stop_name']}: {state}")
        # Real GPS straight off HAFAS's radar (not the schedule-interpolated
        # estimate PL/DE fall back to) - tagged explicitly so a consumer
        # never has to guess which kind of position a row holds.
        position_source = "live" if row["lat"] is not None else None
        trip_id = resolve_trip_id(cur, row["train_number"], row["stop_name"], row["delay_minutes"], local_now(COUNTRY_CODE))
        if trip_id is not None:
            resolved_count += 1
        cur.execute("""
            INSERT INTO delay_observations
                (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, headsign, lat, lon, position_source, live_trip_id, trip_id, cancellation_kind, platform)
            SELECT s.id, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            FROM stations s
            WHERE s.name = ANY(%s) AND s.country_code = %s
            ORDER BY array_position(%s, s.name)
            LIMIT 1
        """, (
            date.today(), row["delay_minutes"], row["is_cancelled"], row["route_name"], COUNTRY_CODE,
            row["headsign"], row["lat"], row["lon"], position_source, row["live_trip_id"], trip_id,
            row["cancellation_kind"], row["platform"],
            station_name_candidates(row["stop_name"]), COUNTRY_CODE,
            station_name_candidates(row["stop_name"]),
        ))
        count += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {count} observations, {resolved_count} with a resolved trip_id.")

if __name__ == "__main__":
    poll()
