"""Connection-scan journey planning for Poland.

Poland is the one country with deep per-train history and no planner to borrow:
MÁV publishes an OTP endpoint, ÖBB's HAFAS implements journeys(), DB's HAFAS is
shut down, and mkuran.pl is a data mirror with no planning API at all. So this
searches the static timetable we already import.

The algorithm is the Connection Scan Algorithm (CSA), which is the right shape
here for a reason worth writing down: a connection is one hop of one trip
between two consecutive stops, CSA sorts every connection by departure time and
sweeps forward once, and the whole thing is a single linear pass with no
priority queue. Given the data is already sorted-ish by time and lives one query
away, that beats a graph search that would need the timetable reshaped first.

Kept out of api.py, and DB access confined to _load_connections(), so the search
itself is a pure function over plain tuples and can be tested without a database
- the same split poland_poller.py uses for parse/resolve.

WHAT THIS DELIBERATELY DOES NOT DO
  * No real-time. It plans the printed timetable. Delay history is applied on
    top by the transfer-risk layer, which is a separate concern and already
    built.
  * No minimum interchange time of its own. Interchange is the risk layer's
    business (and no Polish feed publishes one anyway - transfers.txt exists in
    PL's feed but was skipped at import). CSA here allows a change whenever the
    connector departs at or after the feeder arrives; the risk figure is what
    tells a rider whether that is realistic.
  * No footpaths between nearby stations. Every change happens at one station.
"""

# A journey is not allowed to run forever: without a horizon the scan happily
# returns a 20-hour itinerary with nine changes, which is not a journey anyone
# would take and costs real time to find.
MAX_JOURNEY_SECONDS = 14 * 3600
DEFAULT_WINDOW_SECONDS = 8 * 3600
MAX_LEGS = 6

_DOW = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def hms_to_seconds(hms):
    """GTFS wall-clock to seconds past midnight of the service day.

    Legitimately exceeds 24:00:00 for services continuing overnight, which is
    why this returns raw seconds rather than a time object - 25:10:00 is a real
    value and a valid one, and clamping it would silently drop night trains.
    """
    if not hms:
        return None
    try:
        h, m, s = (int(x) for x in hms.split(":"))
    except (ValueError, AttributeError):
        return None
    return h * 3600 + m * 60 + s


def seconds_to_hm(sec):
    if sec is None:
        return None
    return f"{(sec // 3600) % 24:02d}:{(sec % 3600) // 60:02d}"


def load_connections(cur, country, service_date, from_sec, window_sec=DEFAULT_WINDOW_SECONDS):
    """Every timetabled hop that departs inside the window, on a running service.

    One query. The LEAD window function pairs each stop with the next stop of
    the same trip, which is exactly what a connection is - doing it in SQL
    avoids shipping 400k rows into Python to pair them up here.
    """
    dow = _DOW[service_date.weekday()]
    end_sec = from_sec + window_sec
    # Text comparison is correct for zero-padded GTFS times and keeps the
    # filter index-friendly: "25:10:00" > "23:00:00" lexicographically, which
    # is also true numerically, so overnight services are not excluded.
    lo = f"{from_sec // 3600:02d}:{(from_sec % 3600) // 60:02d}:00"
    hi = f"{end_sec // 3600:02d}:{(end_sec % 3600) // 60:02d}:00"
    cur.execute(f"""
        WITH running AS (
            -- GTFS-correct: a weekly pattern inside its validity window, OR an
            -- explicit added date, and never a removed one. Poland reaches
            -- this only through the second branch - its feed ships no
            -- calendar.txt at all (see schema/013).
            SELECT t.id, t.trip_short_name, t.headsign, r.name AS route_name
            FROM trips t
            LEFT JOIN routes r ON r.id = t.route_id
            WHERE t.country_code = %s
              AND NOT EXISTS (
                    SELECT 1 FROM service_date sd
                    WHERE sd.country_code = t.country_code
                      AND sd.service_id = t.service_id
                      AND sd.service_date = %s AND NOT sd.added)
              AND (
                EXISTS (
                    SELECT 1 FROM service_calendar sc
                    WHERE sc.country_code = t.country_code
                      AND sc.service_id = t.service_id AND sc.{dow}
                      AND (sc.start_date IS NULL OR sc.start_date <= %s)
                      AND (sc.end_date   IS NULL OR sc.end_date   >= %s))
                OR EXISTS (
                    SELECT 1 FROM service_date sd
                    WHERE sd.country_code = t.country_code
                      AND sd.service_id = t.service_id
                      AND sd.service_date = %s AND sd.added)
              )
        ), hops AS (
            SELECT st.trip_id, st.station_id, st.stop_sequence,
                   st.scheduled_departure AS dep,
                   lead(st.station_id)        OVER w AS to_station,
                   lead(st.scheduled_arrival) OVER w AS to_arr,
                   lead(st.stop_sequence)     OVER w AS to_seq
            FROM stop_times st
            JOIN running rt ON rt.id = st.trip_id
            WINDOW w AS (PARTITION BY st.trip_id ORDER BY st.stop_sequence)
        )
        SELECT h.trip_id, h.station_id, h.to_station, h.dep, h.to_arr,
               h.stop_sequence, h.to_seq, rt.route_name, rt.trip_short_name, rt.headsign
        FROM hops h
        JOIN running rt ON rt.id = h.trip_id
        WHERE h.to_station IS NOT NULL AND h.dep IS NOT NULL AND h.to_arr IS NOT NULL
          AND h.dep >= %s AND h.dep <= %s
    """, (country, service_date, service_date, service_date, service_date, lo, hi))
    conns = []
    for (trip_id, frm, to, dep, arr, seq, to_seq, route_name, short, headsign) in cur.fetchall():
        d, a = hms_to_seconds(dep), hms_to_seconds(arr)
        if d is None or a is None or a < d:
            continue
        conns.append((d, a, frm, to, trip_id, seq, to_seq, route_name, short, headsign))
    conns.sort(key=lambda c: c[0])
    return conns


def scan(connections, origin_id, destination_id, depart_after_sec):
    """One CSA sweep. Returns the connection chain to the destination, or None.

    Pure: takes tuples, returns tuples, touches nothing else. `connections`
    must be sorted by departure time, which load_connections guarantees.
    """
    # Arrival time AND change count, not arrival alone. Plain CSA minimises
    # arrival only, so on a tie it keeps whatever it found first - which
    # produced a real Warszawa->Krakow result that rode one stop for three
    # minutes, waited 42, then boarded the express that had been callable at
    # the origin all along. Same arrival, one pointless change. Ties now go to
    # the itinerary with fewer changes.
    earliest = {origin_id: (depart_after_sec, 0)}
    came_from = {}
    reached_trips = {}
    best_arrival = None
    for conn in connections:
        dep, arr, frm, to, trip_id = conn[0], conn[1], conn[2], conn[3], conn[4]
        if best_arrival is not None and dep > best_arrival:
            # Sorted by departure, so nothing later can improve on a journey
            # that already arrives before this one even starts.
            break
        if dep - depart_after_sec > MAX_JOURNEY_SECONDS:
            break
        # Staying on a trip costs no change; boarding it from a station does,
        # unless this is the very first leg out of the origin.
        onboard = reached_trips.get(trip_id)
        from_stop = earliest.get(frm)
        board_changes = None
        if from_stop is not None and from_stop[0] <= dep:
            board_changes = from_stop[1] + (0 if frm == origin_id else 1)
        if onboard is None and board_changes is None:
            continue
        changes = min(x for x in (onboard, board_changes) if x is not None)
        prev = reached_trips.get(trip_id)
        if prev is None or changes < prev:
            reached_trips[trip_id] = changes
        cur_best = earliest.get(to)
        better = cur_best is None or arr < cur_best[0] or (arr == cur_best[0] and changes < cur_best[1])
        if better:
            earliest[to] = (arr, changes)
            came_from[to] = conn
            if to == destination_id and (best_arrival is None or arr < best_arrival):
                best_arrival = arr
    if destination_id not in came_from:
        return None
    # Reconstruction has to walk back along a TRIP, not station by station.
    # came_from holds one connection per station, and a station the train
    # merely passed through may have been reached earlier by something else -
    # which produced a real Warszawa->Krakow chain containing a three-minute
    # hop on a different train, because the express's intermediate stop had
    # been reached sooner by that hop even though we never got off there.
    # So: take the connection into the destination, then slide back along its
    # own trip to the earliest stop where boarding was actually possible.
    by_trip = {}
    for c in connections:
        by_trip.setdefault(c[4], []).append(c)
    for hops in by_trip.values():
        hops.sort(key=lambda c: c[5])

    chain = []
    node = destination_id
    guard = 0
    while node != origin_id and node in came_from and guard < 200:
        guard += 1
        conn = came_from[node]
        trip_id = conn[4]
        run = [conn]
        # Walk backwards along this trip while the earlier stop was reachable
        # in time - that is where a rider would really have got on.
        hops = by_trip.get(trip_id, [])
        idx = next((i for i, c in enumerate(hops) if c[5] == conn[5]), None)
        if idx is not None:
            i = idx - 1
            while i >= 0:
                prev = hops[i]
                if prev[3] != run[0][2]:
                    break
                reached = earliest.get(prev[2])
                if reached is None or reached[0] > prev[0]:
                    break
                run.insert(0, prev)
                if prev[2] == origin_id:
                    break
                i -= 1
        chain = run + chain
        node = run[0][2]
    if node != origin_id:
        return None
    return chain

def chain_to_legs(chain):
    """Collapse consecutive hops of the same trip into one leg."""
    legs = []
    for conn in chain:
        dep, arr, frm, to, trip_id, seq, to_seq, route_name, short, headsign = conn
        if legs and legs[-1]["trip_id"] == trip_id:
            legs[-1]["to_station"] = to
            legs[-1]["arrive_sec"] = arr
            legs[-1]["to_seq"] = to_seq
        else:
            legs.append({
                "trip_id": trip_id, "route_name": route_name,
                "trip_short_name": short, "headsign": headsign,
                "from_station": frm, "to_station": to,
                "depart_sec": dep, "arrive_sec": arr,
                "from_seq": seq, "to_seq": to_seq,
            })
    return legs[:MAX_LEGS] if len(legs) <= MAX_LEGS else None
