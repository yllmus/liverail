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
# The Node scripts sit next to this file, so they are found from any
# working directory (the API, the poller and the tests all start elsewhere).
HAFAS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hafas")
# Urban transit that HAFAS reports as mode "train". Excluded from a rail
# tracker - see parse_at_vehicles for the measurement behind this.
EXCLUDED_PRODUCTS = {"tram", "subway"}

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
    # HAFAS omits the space after an abbreviating dot that the static feed
    # keeps: "St.Pölten Hbf" against "St. Pölten Hauptbahnhof". It was worth 7
    # dropped observations in a single poll. Generalised rather than
    # special-cased on "St." - the same shape appears in "Kl.", "Gr.", "Bhf."
    # and others.
    spaced = re.sub(r"\.(?=\S)", ". ", name).strip()
    bases = [name, stripped]
    if spaced not in bases:
        bases.append(spaced)
        stripped_spaced = re.sub(r"\s*\([^)]*\)\s*$", "", spaced).strip()
        if stripped_spaced not in bases:
            bases.append(stripped_spaced)
    for base in bases:
        # The base itself is a candidate, not only the variants derived from
        # it. Without this the respaced form was generated and then never
        # tried: "St.Valentin Bahnhof" produced "St. Valentin" but not
        # "St. Valentin Bahnhof", which is exactly what the static feed calls
        # it, so the observation was dropped anyway.
        candidates.append(base)
        if base.endswith(" Hbf"):
            candidates.append(base[: -len(" Hbf")] + " Hauptbahnhof")
        # Bahnhst (Bahnhaltestelle) and Bahnhof are used interchangeably
        # between the live feed and the static export for the same stop -
        # HAFAS says "Schrambach Bahnhst", the feed says "Schrambach Bahnhof".
        # Try the suffix removed, and each spelling swapped for the other.
        candidates.append(re.sub(r"\s+(Bahnhof|Bahnhst)$", "", base))
        if base.endswith(" Bahnhst"):
            candidates.append(base[: -len(" Bahnhst")] + " Bahnhof")
        elif base.endswith(" Bahnhof"):
            candidates.append(base[: -len(" Bahnhof")] + " Bahnhst")
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
    # relationship to trips.gtfs_trip_id (structural, not a staleness issue,
    # same as HU's gtfsId problem). HAFAS's line.fahrtNr is 0/652 filled, so
    # the number can't come from that field. The live line *name* itself
    # ("RJX 639") already
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
    # One train's full journey, for api.py's /train/AT/{id}. Kept here next to
    # call_radar() so both Node subprocess invocations live in one place and
    # share the same path and timeout convention. radar() alone can't
    # serve a train tracker: its nextStopovers is bounded by the radar call's
    # own duration window (~4 stops in practice), while trip() returns every
    # stopover from origin to destination.
    result = subprocess.run(
        ["node", os.path.join(HAFAS_DIR, "trip.js"), trip_id, line_name or ""],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        print(f"trip.js error: {result.stderr.strip()}")
        return None
    return json.loads(result.stdout)

def call_journeys(from_name, to_name, departure_iso=None, results=4):
    # Journey planning for /plan's AT branch. Third and last of the oneshot
    # HAFAS subprocesses, same convention as call_radar/call_trip so all three
    # invocations live together. Longer timeout than the others because
    # journeys() resolves two station names before it plans anything.
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
        # HAFAS labels Vienna's street trams and the U-Bahn as mode "train",
        # separated from actual rail only by `product`. This project used to
        # keep them deliberately, on the grounds that genuine tram-trains are
        # labelled identically and could not be separated.
        # Measured on 2026-08-22, that trade was a bad one: 264 tram rows per
        # poll, of which only 29 resolved to any station at all, because
        # OBB's rail GTFS contains no tram stops - 0 of its 1,173 stations
        # have "Schleife" or "Strassenbahn" in the name. The rest were parsed,
        # queried against `stations`, and thrown away every five minutes.
        #
        # The cost of excluding them is those 29 - some of which will be real
        # Linz tram-train services. That is a scope decision, made explicitly:
        # this is a train delay tracker, and a rider looking up a delayed
        # train is not looking for the 43 to Schottentor.
        if line.get("product") in EXCLUDED_PRODUCTS:
            continue
        route_name = line.get("name")
        stopovers = m.get("nextStopovers") or []
        if not route_name or not stopovers:
            continue
        stop = stopovers[0]
        stop_name = (stop.get("stop") or {}).get("name")
        # Split as well as collapsed - a transfer needs the feeder's ARRIVAL
        # and the connector's DEPARTURE. delay_seconds keeps its original
        # arrival-preferred meaning so delay_minutes is unchanged.
        #
        # AT will in practice only ever fill the departure side from here, and
        # that is a radar() limitation rather than an OBB one - measured, not
        # assumed: across 314 live movements, nextStopovers[0] carried
        # arrivalDelay 0 times (181 departure-only, 133 neither). The same
        # trains DO publish arrival delays through HAFAS's trip() call, and
        # they genuinely differ from the departure figure (RJ 263: 420s
        # arrival vs 480s departure at the same stop) - hafas/trip.js, which
        # /train/AT/{id} already uses, gets both. Filling the arrival column
        # from the poller would therefore mean one trip() call per vehicle per
        # poll, ~300 extra HAFAS round-trips every 5 minutes on a single-vCPU
        # box shared with three other pollers and the API. Left NULL rather
        # than paid for, and rather than quietly copying the departure figure
        # into it, which would fabricate an arrival delay OBB did not publish.
        arrival_delay = stop.get("arrivalDelay")
        departure_delay = stop.get("departureDelay")
        delay_seconds = arrival_delay if arrival_delay is not None else departure_delay
        # hafas-client sets `cancelled` on a stopover from HAFAS's own
        # aCncl/dCncl flags, and only when true (absent means not cancelled).
        # 0/40 train movements in a real night sample - like HU, this is wired
        # so a real cancellation is recorded if OBB publishes one, not because
        # AT is known to have coverage. See
        # schema/005_cancellations_platform.sql.
        is_cancelled = bool(stop.get("cancelled"))
        # A train with no delay figure is still a train. Measured on a live
        # radar call: 380 of 1,033 moving trains - 37% - had neither an
        # arrivalDelay nor a departureDelay on their next stopover, and every
        # one of them was dropped here. They are not cancelled, not buses and
        # not unnamed; OBB simply has not published a figure for that stopover
        # yet. Dropping them made more than a third of Austria's live map
        # invisible and left their existence unrecorded.
        #
        # They are written with delay_minutes NULL and is_cancelled false,
        # which is a different state from a cancellation (NULL and true) and
        # renders as "Unknown" rather than "On time" - sevInfo(null) in
        # index.html already does the right thing. percentile_cont ignores
        # NULLs, so they cannot skew a delay distribution either.
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
            # mismatch). Stored opaque/unresolved, same convention as HU.
            "live_trip_id": m.get("tripId"),
            # See resolve_trip_id() below - a second, independent identity
            # path that (unlike live_trip_id/tripId) can sometimes actually
            # join to our static trips table.
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

        # Dedup, which AT has never had. PL and DE skip an INSERT when a trip's
        # delay has not changed since the last poll (the fix that took
        # delay_observations from 54M rows to 15.3M); Austria wrote every train
        # every five minutes regardless, ~75,600 rows/day. That was affordable
        # only while 37% of its trains were being silently dropped - now that they
        # are recorded, unconditional writing would push AT past 185,000/day for
        # no extra information.
        #
        # Keyed on the journey id INSIDE the HAFAS tripId, not the tripId itself.
        # The full identifier is not stable across polls - it embeds a server
        # timestamp (#ST#), so the same physical train arrives wearing a different
        # id every five minutes. Measured: 1,440 rows in fifteen minutes carried
        # 438 distinct tripIds, one train alone accounting for five of them, and a
        # dedup keyed on the whole string skipped almost nothing. The #ZI# field
        # is the journey id and does hold still, which is why hafas_journey_id()
        # pulls it out. trip_id is no good either: it resolves for barely half of
        # AT's vehicles, so keying on it
        # would treat every unresolved train as brand new.
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
            # A train OBB has published no delay for is kept in the live feed -
            # parse_at_vehicles emits it, and /live?country=AT reads that parser
            # directly, so the map now shows all 1,064 moving trains instead of
            # 653. It is not written to history, though: delay_minutes NULL
            # contributes nothing to a delay distribution (percentile_cont ignores
            # NULLs) and there are ~380 of them per poll, which would have tripled
            # Austria's stored rows to buy no answerable question. A cancellation
            # is different and always stored - that IS the information.
            if row["delay_minutes"] is None and not row["is_cancelled"]:
                unknown += 1
                continue
            prev = last_state.get((row.get("_zi"), row.get("stop_name")))
            if prev is not None and prev == (row["delay_minutes"], row["is_cancelled"]):
                skipped += 1
                continue
            state = "CANCELLED" if row["is_cancelled"] else row["delay_minutes"]
            print(f"  {row['route_name']} at {row['stop_name']}: {state}")
            # Real GPS straight off HAFAS's radar (not the schedule-interpolated
            # estimate PL/DE fall back to) - tagged explicitly so a consumer
            # never has to guess which kind of position a row holds.
            position_source = "live" if row["lat"] is not None else None
            trip_id = resolve_trip_id(cur, row["train_number"], row["stop_name"], row["delay_minutes"], local_now(COUNTRY_CODE))
            if trip_id is not None:
                resolved_count += 1
            # Same candidate list the INSERT below resolves station_id with, so
            # the stop identity always describes the station the row is written
            # on - see lookup_stop_identity().
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
            # Same reasoning as poller.py: this INSERT is a SELECT against
            # `stations`, so an unmatched name writes nothing while an
            # unconditional counter still reports it as saved. AT resolves names
            # through station_name_candidates(), which covers a lot, but the
            # remaining misses are real and were invisible.
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
