from datetime import datetime

from trip_resolution import resolve_ambiguous_trip


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
