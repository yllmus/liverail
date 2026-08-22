from datetime import datetime
from zoneinfo import ZoneInfo

# GTFS scheduled times are local wall-clock strings with no timezone info,
# but this box's system clock runs in UTC - found while debugging why AT's
# schedule disambiguation was resolving almost nothing even for clearly
# unambiguous-looking cases: a naive datetime.now() call is 1-2h off from
# Central European (Summer) Time depending on DST, which silently defeated
# both the calendar day-of-week check (wrong near midnight) and the
# TRIP_MATCH_TOLERANCE_MIN schedule comparison (wrong all day, every day).
# Every caller must resolve "now" through local_now(country_code) rather
# than relying on the now=None default below, which stays naive UTC only
# because tests exercise it directly with an explicit `now`.
COUNTRY_TIMEZONES = {
    "HU": "Europe/Budapest",
    "AT": "Europe/Vienna",
}


def local_now(country_code):
    tz = COUNTRY_TIMEZONES.get(country_code)
    if not tz:
        return datetime.now()
    return datetime.now(ZoneInfo(tz)).replace(tzinfo=None)


# How close a candidate trip's scheduled time at the observed station has to
# be to the live observation's estimated scheduled time (now - delay) to be
# accepted. Looser would start guessing between e.g. the morning and evening
# runs of the same numbered service; see NOTES.md "Live per-train identity".
TRIP_MATCH_TOLERANCE_MIN = 25


def narrow_to_running_today(cur, country_code, trip_ids, now=None):
    """Given ambiguous candidate trip ids sharing a train number, keep only
    the ones whose service_calendar entry actually runs on `now`'s date.

    Falls back to the original (unfiltered) set if the calendar narrows it
    to nothing - a missing/incomplete calendar entry should degrade back to
    the old ambiguous behavior, not silently discard every candidate.
    """
    today = (now or datetime.now()).date()
    cur.execute("""
        SELECT t.id
        FROM trips t
        JOIN service_calendar c
          ON c.country_code = t.country_code AND c.service_id = t.service_id
        WHERE t.id = ANY(%s) AND t.country_code = %s
          AND (c.start_date IS NULL OR c.start_date <= %s)
          AND (c.end_date   IS NULL OR c.end_date   >= %s)
          AND CASE EXTRACT(ISODOW FROM %s::date)
                WHEN 1 THEN c.monday WHEN 2 THEN c.tuesday
                WHEN 3 THEN c.wednesday WHEN 4 THEN c.thursday
                WHEN 5 THEN c.friday WHEN 6 THEN c.saturday
                ELSE c.sunday END
    """, (trip_ids, country_code, today, today, today))
    running = [r[0] for r in cur.fetchall()]
    return running if running else trip_ids


def _minutes_of_day(hms):
    # GTFS wall-clock, which may exceed 24:00:00 for a service continuing
    # past midnight - wrapped so it compares against a real clock time.
    try:
        h, m, _ = (int(x) for x in hms.split(":"))
    except (ValueError, AttributeError):
        return None
    return (h * 60 + m) % (24 * 60)


def disambiguate_by_schedule(cur, country_code, trip_ids, station_names, delay_minutes, now=None):
    """Break a remaining tie by where the train actually is right now.

    station_names is a list of acceptable station-name matches (callers with
    a single exact name pass [name]; AT passes station_name_candidates() to
    cover HAFAS/GTFS naming drift). The train is at one of station_names now,
    running delay_minutes late, so its scheduled time there should be close
    to now - delay; the candidate whose own scheduled stop_times entry is
    closest to that wins, within TRIP_MATCH_TOLERANCE_MIN.
    """
    now = now or datetime.now()
    target = (now.hour * 60 + now.minute) - (delay_minutes or 0)
    target %= 24 * 60
    cur.execute("""
        SELECT st.trip_id, COALESCE(st.scheduled_departure, st.scheduled_arrival)
        FROM stop_times st
        JOIN stations s ON s.id = st.station_id
        WHERE st.trip_id = ANY(%s) AND s.name = ANY(%s) AND s.country_code = %s
    """, (trip_ids, station_names, country_code))
    found = cur.fetchall()
    # If every candidate that has schedule data at this station agrees on the
    # exact same time, they aren't distinct services to pick between - they
    # are the same physical trip duplicated across multiple static GTFS rows
    # (confirmed against real AT data: 20/22 of AT's remaining post-calendar
    # ties are exactly this - e.g. Nightjet 448 to Bruxelles Midi exists as
    # 14 separate trip rows, all departing Wien Hauptbahnhof at 18:13). Same
    # "genuinely duplicate, pick one" convention this project already uses
    # for duplicate *station* name rows (LIMIT 1) - any one of them
    # describes the same real train equally correctly.
    times = {hms for _, hms in found}
    if len(times) == 1:
        return min(trip_id for trip_id, _ in found)
    best = None
    for trip_id, hms in found:
        sched = _minutes_of_day(hms)
        if sched is None:
            continue
        diff = abs(sched - target)
        diff = min(diff, 24 * 60 - diff)  # clock wrap: 23:55 vs 00:05 is 10min
        if diff <= TRIP_MATCH_TOLERANCE_MIN and (best is None or diff < best[1]):
            best = (trip_id, diff)
    return best[0] if best else None


def resolve_ambiguous_trip(cur, country_code, matches, station_names, delay_minutes, now=None):
    """Shared tail end of number-based trip resolution, once a live train
    number has already produced a candidate trip-id list: narrow by calendar,
    then by schedule position if still ambiguous. Returns a single trip id or
    None.
    """
    if len(matches) > 1:
        matches = narrow_to_running_today(cur, country_code, matches, now)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1 and station_names:
        return disambiguate_by_schedule(cur, country_code, matches, station_names, delay_minutes, now)
    return None


def lookup_stop_identity(cur, country_code, trip_id, station_names):
    """Which stop of this trip is the train at, and when was it due there?

    HU and AT resolve a live vehicle to a STATION BY NAME, not to a position
    in a journey - so unlike PL and DE, whose feeds hand them a stop_sequence
    or a stop_id directly, they can only recover the stop identity by looking
    it up in the schedule after the trip has been resolved.

    Returns (stop_sequence, scheduled_arrival, scheduled_departure), all None
    when the trip is unresolved or the schedule has no such stop - the same
    graceful-skip convention used everywhere else in this codebase, rather
    than guessing a position.

    `station_names` is the candidate list station_name_candidates() already
    builds for AT's HAFAS-vs-GTFS name formatting differences; HU passes a
    single-item list. Ordered by array_position so the closest spelling wins,
    exactly as the INSERT that resolves station_id does - the two must agree
    or the stop identity would describe a different station than the row it
    is written on.

    A trip that calls at the same station twice (a loop or a reversal) has two
    candidate rows. LIMIT 1 takes the earlier, matching the convention already
    used for AT's genuinely-duplicated station rows; there is no signal in
    either live feed to tell the two visits apart.
    """
    if trip_id is None or not station_names:
        return None, None, None
    cur.execute("""
        SELECT st.stop_sequence, st.scheduled_arrival, st.scheduled_departure
        FROM stop_times st
        JOIN stations s ON s.id = st.station_id
        WHERE st.trip_id = %s
          AND s.country_code = %s
          AND s.name = ANY(%s)
        ORDER BY array_position(%s, s.name), st.stop_sequence
        LIMIT 1
    """, (trip_id, country_code, station_names, station_names))
    row = cur.fetchone()
    return row if row else (None, None, None)
