from datetime import datetime

from trip_resolution import lookup_stop_identity, resolve_ambiguous_trip


class _FakeCursor:
    """Returns responses in sequence, one list per execute()/fetchall() pair -
    unlike the simpler single-response fakes in test_hu_poller.py/
    test_at_poller.py, this lets a test express "2 candidates, then the
    calendar narrows to 1" as two distinct fetchall results.
    """
    def __init__(self, responses):
        self._responses = list(responses)

    def execute(self, query, args):
        self.last_query_args = args

    def fetchall(self):
        return self._responses.pop(0)


def test_resolve_ambiguous_trip_single_match_skips_narrowing():
    cur = _FakeCursor([[]])  # would blow up if narrowing ran - only 1 candidate
    assert resolve_ambiguous_trip(cur, "HU", [101], None, None) == 101

def test_resolve_ambiguous_trip_calendar_narrows_to_one():
    cur = _FakeCursor([[(101,)]])  # calendar keeps only 101 of [101, 202]
    assert resolve_ambiguous_trip(cur, "HU", [101, 202], None, None) == 101

def test_resolve_ambiguous_trip_calendar_empty_falls_back_to_original_set():
    # No calendar row matched (e.g. missing calendar.txt entry) - keep the
    # original ambiguous set rather than discarding every candidate.
    cur = _FakeCursor([[]])
    assert resolve_ambiguous_trip(cur, "HU", [101, 202], None, None) is None

def test_resolve_ambiguous_trip_falls_through_to_schedule_when_still_tied():
    now = datetime(2026, 8, 20, 21, 30)
    cur = _FakeCursor([
        [(101,), (202,)],                  # calendar: both still run today
        [(101, "21:12:00"), (202, "08:05:00")],  # schedule: 101 is closer
    ])
    result = resolve_ambiguous_trip(cur, "HU", [101, 202], ["Tapolca"], 18, now)
    assert result == 101

def test_resolve_ambiguous_trip_collapses_exact_schedule_tie_to_lowest_id():
    # Same scheduled time at the station for every remaining candidate -
    # duplicate GTFS rows for one physical trip, not real ambiguity.
    now = datetime(2026, 8, 20, 18, 0)
    cur = _FakeCursor([
        [(202,), (101,), (303,)],                                  # calendar: all still run
        [(202, "18:13:00"), (101, "18:13:00"), (303, "18:13:00")],  # schedule: exact tie
    ])
    result = resolve_ambiguous_trip(cur, "AT", [101, 202, 303], ["Wien Hauptbahnhof"], 0, now)
    assert result == 101

def test_resolve_ambiguous_trip_no_station_returns_none_when_still_tied():
    cur = _FakeCursor([[(101,), (202,)]])
    assert resolve_ambiguous_trip(cur, "HU", [101, 202], None, None) is None

def test_resolve_ambiguous_trip_returns_none_for_no_candidates():
    assert resolve_ambiguous_trip(_FakeCursor([]), "HU", [], None, None) is None


class _FetchOneCursor:
    """lookup_stop_identity issues a single query and reads one row."""
    def __init__(self, row):
        self._row = row
        self.executed = False
        self.last_query_args = None

    def execute(self, query, args):
        self.executed = True
        self.last_query_args = args

    def fetchone(self):
        return self._row


def test_lookup_stop_identity_returns_the_scheduled_stop():
    cur = _FetchOneCursor((7, "09:59:00", "10:01:00"))
    assert lookup_stop_identity(cur, "AT", 101, ["Wien Hbf"]) == (7, "09:59:00", "10:01:00")


def test_lookup_stop_identity_skips_the_query_for_an_unresolved_trip():
    # HU resolves ~60% of trips and AT ~41%; the rest must not cost a query
    # each, and must not resolve to some other train's stop.
    cur = _FetchOneCursor((7, "09:59:00", None))
    assert lookup_stop_identity(cur, "HU", None, ["Tapolca"]) == (None, None, None)
    assert cur.executed is False


def test_lookup_stop_identity_skips_the_query_with_no_station_candidates():
    cur = _FetchOneCursor((7, "09:59:00", None))
    assert lookup_stop_identity(cur, "AT", 101, []) == (None, None, None)
    assert cur.executed is False


def test_lookup_stop_identity_returns_nulls_when_the_schedule_has_no_such_stop():
    # A resolved trip whose static schedule does not list this station - the
    # same graceful skip used everywhere else, not a guessed position.
    cur = _FetchOneCursor(None)
    assert lookup_stop_identity(cur, "AT", 101, ["Wien Hbf"]) == (None, None, None)


def test_lookup_stop_identity_passes_candidates_in_preference_order():
    # AT's HAFAS and GTFS names disagree on formatting, so the candidate list
    # is ordered - and it must be the SAME list the INSERT resolves
    # station_id with, or the stop identity would describe a different
    # station than the row it is written on.
    cur = _FetchOneCursor((7, "09:59:00", None))
    candidates = ["Wien Hbf", "Wien Hauptbahnhof"]
    lookup_stop_identity(cur, "AT", 101, candidates)
    assert cur.last_query_args == (101, "AT", candidates, candidates)
