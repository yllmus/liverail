import json
import os
import re
import subprocess
import psycopg2
from datetime import date

from at_route_names import extract_at_train_number
from trip_resolution import lookup_stop_identity, resolve_ambiguous_trip, local_now
from config import DB_CONFIG

COUNTRY_CODE = "AT"
# The Node scripts sit next to this file, so they are found whatever the
# working directory.
HAFAS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hafas")
# Urban transit that HAFAS reports as mode "train". See parse_at_vehicles.
EXCLUDED_PRODUCTS = {"tram", "subway"}

def station_name_candidates(name):
    # ÖBB's live HAFAS names and its static GTFS names are formatted
    # differently: "Hbf" against "Hauptbahnhof", a trailing " Bahnhof" or "
    # Bahnhst" that only one side has, and platform qualifiers such as "Wien
    # Hbf (Bahnsteige 3-12)". Try the exact name first, then normalised
    # variants, rather than dropping the observation.
    candidates = [name]
    stripped = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
    if stripped != name:
        candidates.append(stripped)
    # HAFAS drops the space after an abbreviating dot that the static feed
    # keeps ("St.Pölten" against "St. Pölten"). The same shape appears in
    # "Kl.", "Gr." and "Bhf.", so it is handled generally.
    spaced = re.sub(r"\.(?=\S)", ". ", name).strip()
    bases = [name, stripped]
    if spaced not in bases:
        bases.append(spaced)
        stripped_spaced = re.sub(r"\s*\([^)]*\)\s*$", "", spaced).strip()
        if stripped_spaced not in bases:
            bases.append(stripped_spaced)
    for base in bases:
        # The base itself is a candidate too, not only the variants derived
        # from it: "St.Valentin Bahnhof" needs to become "St. Valentin
        # Bahnhof", which is what the static feed calls it.
        candidates.append(base)
        if base.endswith(" Hbf"):
            candidates.append(base[: -len(" Hbf")] + " Hauptbahnhof")
        # The two sources use Bahnhst (Bahnhaltestelle) and Bahnhof
        # interchangeably for the same stop, so try the suffix removed and each
        # spelling swapped for the other.
        candidates.append(re.sub(r"\s+(Bahnhof|Bahnhst)$", "", base))
        if base.endswith(" Bahnhst"):
            candidates.append(base[: -len(" Bahnhst")] + " Bahnhof")
        elif base.endswith(" Bahnhof"):
            candidates.append(base[: -len(" Bahnhof")] + " Bahnhst")
    # Deduplicate, keeping priority order (exact match first).
    seen = set()
    result = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            result.append(c)
    return result

def resolve_trip_id(cur, number, stop_name=None, delay_minutes=None, now=None):
    # The live tripId is a HAFAS-internal key unrelated to the static feed's
    # trip ids. The live line name ("RJX 639") carries the train's public
    # number in the same "CATEGORY NUMBER" form as the static trip_short_name
    # ("RJ 658"), so Austria uses the same number, calendar and schedule
    # resolution as Hungary, matched against trip_short_name instead of
    # routes.name.
    if not number:
        return None
    cur.execute("""
        SELECT id FROM trips
        WHERE country_code = %s AND trip_short_name ~ %s
    """, (COUNTRY_CODE, r"(^|[^0-9])" + re.escape(number) + r"([^0-9]|$)"))
    matches = [m[0] for m in cur.fetchall()]
    station_names = station_name_candidates(stop_name) if stop_name else None
    return resolve_ambiguous_trip(cur, COUNTRY_CODE, matches, station_names, delay_minutes, now)

def hafas_journey_id(trip_id):
    """The stable journey id inside a HAFAS tripId.

    A tripId looks like
        2|#VN#1#ST#1787300919#PI#0#ZI#82533#TA#0#DA#220826#...
    and the #ST# field is a server timestamp that changes between calls, so
    the whole string identifies a *sighting*, not a train. #ZI# is the journey
    and stays put. Split on '#': the value after 'ZI'.
    """
    if not trip_id:
        return None
    parts = trip_id.split("#")
    for i, part in enumerate(parts):
        if part == "ZI" and i + 1 < len(parts):
            return parts[i + 1]
    return None


def call_radar():
    result = subprocess.run(
        ["node", os.path.join(HAFAS_DIR, "radar.js")],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        print(f"radar.js error: {result.stderr.strip()}")
        return []
    return json.loads(result.stdout)

def call_trip(trip_id, line_name=None):
    # One train's full journey, for /train/AT/{id}. radar() only returns the
    # next few stops, while trip() returns every stop from origin to
    # destination.
    result = subprocess.run(
        ["node", os.path.join(HAFAS_DIR, "trip.js"), trip_id, line_name or ""],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        print(f"trip.js error: {result.stderr.strip()}")
        return None
    return json.loads(result.stdout)

def call_journeys(from_name, to_name, departure_iso=None, results=4):
    # Journey planning for /plan in Austria. Longer timeout, because journeys()
    # resolves both station names before it plans anything.
    result = subprocess.run(
        ["node", os.path.join(HAFAS_DIR, "journeys.js"), from_name, to_name,
         departure_iso or "", str(results)],
        capture_output=True, text=True, timeout=45
    )
    if result.returncode != 0:
        print(f"journeys.js error: {result.stderr.strip()}")
        return None
    try:
        return json.loads(result.stdout)
    except ValueError:
        return None

def parse_at_vehicles(movements):
    rows = []
    for m in movements or []:
        line = m.get("line") or {}
        if line.get("mode") != "train":
            continue
        # HAFAS labels Vienna's trams and U-Bahn as mode "train", separated
        # from rail only by `product`. ÖBB's rail GTFS has no tram stops, so
        # almost none of these rows could resolve to a station. Excluding them
        # also drops a few genuine tram-train services in Linz, which is an
        # accepted cost for a train delay tracker.
        if line.get("product") in EXCLUDED_PRODUCTS:
            continue
        route_name = line.get("name")
        stopovers = m.get("nextStopovers") or []
        if not route_name or not stopovers:
            continue
        stop = stopovers[0]
        stop_name = (stop.get("stop") or {}).get("name")
        # Keep arrival and departure delays separately, as in poller.py.
        #
        # radar() never sets an arrival delay on the next stopover, only a
        # departure delay, so for Austria only the departure side is filled.
        # trip() does carry both, but calling it for every vehicle on every
        # poll would cost hundreds of extra requests each time. The arrival
        # column stays NULL rather than borrowing the departure figure, which
        # would invent a number ÖBB never published.
        arrival_delay = stop.get("arrivalDelay")
        departure_delay = stop.get("departureDelay")
        delay_seconds = arrival_delay if arrival_delay is not None else departure_delay
        # hafas-client sets `cancelled` only when HAFAS flags it. Austria is
        # not known to publish cancellations this way; the field is read so one
        # is recorded if it does.
        is_cancelled = bool(stop.get("cancelled"))
        # A train without a published delay is still a train. About a third of
        # moving trains have no delay figure for their next stop yet. They are
        # kept with delay_minutes NULL and is_cancelled false, which the
        # frontend shows as "Unknown", not "On time".
        if not stop_name:
            continue
        location = m.get("location") or {}
        rows.append({
            "stop_name": stop_name,
            "route_name": route_name,
            "delay_minutes": None if (is_cancelled or delay_seconds is None)
                             else round(delay_seconds / 60, 2),
            "delay_arrival_minutes": None if (is_cancelled or arrival_delay is None) else round(arrival_delay / 60, 2),
            "delay_departure_minutes": None if (is_cancelled or departure_delay is None) else round(departure_delay / 60, 2),
            "is_cancelled": is_cancelled,
            "cancellation_kind": "stop" if is_cancelled else None,
            # Real-time platform where HAFAS knows it, otherwise the planned
            # one.
            "platform": (stop.get("departurePlatform") or stop.get("arrivalPlatform")
                         or stop.get("plannedDeparturePlatform") or stop.get("plannedArrivalPlatform")),
            # Either can be missing, same as Hungary's headsign and position.
            "headsign": m.get("direction"),
            "lat": location.get("latitude"),
            "lon": location.get("longitude"),
            # HAFAS's composite tripId. It cannot be joined to the static
            # feed's trip ids, so it is stored as an opaque id, as for Hungary.
            "live_trip_id": m.get("tripId"),
            "train_number": extract_at_train_number(route_name),
        })
    return rows

def poll():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        print("Polling ÖBB...")
        movements = call_radar()
        rows = parse_at_vehicles(movements)
        print(f"Got {len(rows)} vehicles")

        # Skip a train whose delay has not changed since the last poll, as the
        # Polish and German pollers do; writing every train every five minutes
        # stores many copies of the same fact.
        #
        # The key is the journey id inside the HAFAS tripId, not the whole
        # tripId: the full string embeds a server timestamp, so the same train
        # gets a different id on every poll. trip_id would not work either,
        # because it resolves for only about half of Austria's trains.
        for r in rows:
            r["_zi"] = hafas_journey_id(r.get("live_trip_id"))
        keys = [(r["_zi"], r.get("stop_name")) for r in rows if r["_zi"]]
        last_state = {}
        if keys:
            cur.execute("""
                SELECT DISTINCT ON (split_part(o.live_trip_id, '#', 9), s.name)
                       split_part(o.live_trip_id, '#', 9), s.name,
                       o.delay_minutes, o.is_cancelled
                FROM delay_observations o
                JOIN stations s ON s.id = o.station_id
                WHERE o.country_code = %s AND o.observed_date = %s
                  AND split_part(o.live_trip_id, '#', 9) = ANY(%s)
                ORDER BY split_part(o.live_trip_id, '#', 9), s.name, o.id DESC
            """, (COUNTRY_CODE, date.today(), [k[0] for k in keys]))
            last_state = {(r[0], r[1]): (r[2], bool(r[3])) for r in cur.fetchall()}

        count = 0
        resolved_count = 0
        skipped = 0
        unresolved_station = 0
        unknown = 0
        for row in rows:
            # Trains without a published delay stay in the live view (which
            # reads parse_at_vehicles directly) but are not stored: a NULL
            # delay adds nothing to a delay distribution. A cancellation is
            # always stored, because not running is the information.
            if row["delay_minutes"] is None and not row["is_cancelled"]:
                unknown += 1
                continue
            prev = last_state.get((row.get("_zi"), row.get("stop_name")))
            if prev is not None and prev == (row["delay_minutes"], row["is_cancelled"]):
                skipped += 1
                continue
            state = "CANCELLED" if row["is_cancelled"] else row["delay_minutes"]
            print(f"  {row['route_name']} at {row['stop_name']}: {state}")
            # Real GPS from HAFAS, unlike the interpolated estimate used for
            # Poland and Germany.
            position_source = "live" if row["lat"] is not None else None
            trip_id = resolve_trip_id(cur, row["train_number"], row["stop_name"], row["delay_minutes"], local_now(COUNTRY_CODE))
            if trip_id is not None:
                resolved_count += 1
            # Uses the same candidate names as the INSERT below, so the stop
            # identity always describes the station the row is written for.
            candidates = station_name_candidates(row["stop_name"])
            stop_sequence, sched_arrival, sched_departure = lookup_stop_identity(
                cur, COUNTRY_CODE, trip_id, candidates)
            cur.execute("""
                INSERT INTO delay_observations
                    (station_id, observed_date, actual_arrival, delay_minutes, is_cancelled, route_name, country_code, headsign, lat, lon, position_source, live_trip_id, trip_id, cancellation_kind, platform,
                     stop_sequence, scheduled_arrival, scheduled_departure, delay_arrival_minutes, delay_departure_minutes)
                SELECT s.id, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                FROM stations s
                WHERE s.name = ANY(%s) AND s.country_code = %s
                ORDER BY array_position(%s, s.name)
                LIMIT 1
            """, (
                date.today(), row["delay_minutes"], row["is_cancelled"], row["route_name"], COUNTRY_CODE,
                row["headsign"], row["lat"], row["lon"], position_source, row["live_trip_id"], trip_id,
                row["cancellation_kind"], row["platform"],
                stop_sequence, sched_arrival, sched_departure,
                row.get("delay_arrival_minutes"), row.get("delay_departure_minutes"),
                candidates, COUNTRY_CODE, candidates,
            ))
            # Count rows the database accepted, as in poller.py: an unmatched
            # station name writes nothing.
            if cur.rowcount:
                count += 1
            else:
                unresolved_station += 1
        conn.commit()
        print(f"Saved {count} observations, {resolved_count} with a resolved trip_id, "
              f"skipped {skipped} unchanged, {unknown} with no delay published, "
              f"{unresolved_station} dropped for an unknown station.")
    finally:
        conn.close()

if __name__ == "__main__":
    poll()
