import json
import os

from austria_poller import parse_at_vehicles, station_name_candidates, resolve_trip_id

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "oebb_radar.json")

def load_fixture():
    with open(FIXTURE) as f:
        return json.load(f)

def test_parse_at_vehicles_extracts_rows_with_delay():
    rows = parse_at_vehicles(load_fixture())
    assert len(rows) == 2
    assert rows[0] == {
        "stop_name": "Wien Meidling",
        "route_name": "RJ 61",
        "delay_minutes": 3.0,
        "headsign": "Wien Hbf",
        "lat": 48.185,
        "lon": 16.375,
        "live_trip_id": "1|12345|0|86|20260801",
        "train_number": "61",
    }

def test_parse_at_vehicles_extracts_train_number():
    rows = parse_at_vehicles(load_fixture())
    assert rows[0]["train_number"] == "61"

def test_parse_at_vehicles_ignores_unspaced_line_code():
    # "S1" is a line code, not a train number - the static feed's own
    # trip_short_name never omits the space ("S 28209"), so a match here
    # would resolve against the wrong kind of identifier.
    rows = parse_at_vehicles(load_fixture())
    s1 = next(r for r in rows if r["stop_name"] == "Wien Praterstern")
    assert s1["train_number"] is None

def test_parse_at_vehicles_falls_back_to_departure_delay():
    rows = parse_at_vehicles(load_fixture())
    praterstern = next(r for r in rows if r["stop_name"] == "Wien Praterstern")
    assert praterstern["delay_minutes"] == 1.0

def test_parse_at_vehicles_skips_entries_with_no_stopovers():
    rows = parse_at_vehicles(load_fixture())
    assert all(r["route_name"] != "REX 7" for r in rows)

def test_parse_at_vehicles_empty_input():
    assert parse_at_vehicles([]) == []
    assert parse_at_vehicles(None) == []

def test_parse_at_vehicles_excludes_non_train_modes():
    rows = parse_at_vehicles(load_fixture())
    assert all(r["route_name"] != "Bus E3" for r in rows)

def test_station_name_candidates_prefers_exact_match_first():
    assert station_name_candidates("Wien Meidling")[0] == "Wien Meidling"

def test_station_name_candidates_expands_hbf_abbreviation():
    assert "Wien Hauptbahnhof" in station_name_candidates("Wien Hbf")

def test_station_name_candidates_strips_parenthetical_qualifier():
    assert "Wien Hbf" in station_name_candidates("Wien Hbf (Bahnsteige 3-12)")
    assert "Wien Hauptbahnhof" in station_name_candidates("Wien Hbf (Bahnsteige 3-12)")

def test_station_name_candidates_strips_trailing_bahnhof_suffix():
    assert "Wien Aspern Nord" in station_name_candidates("Wien Aspern Nord Bahnhof")

def test_station_name_candidates_has_no_duplicates():
    candidates = station_name_candidates("Wien Hbf (Bahnsteige 3-12)")
    assert len(candidates) == len(set(candidates))


class _FakeCursor:
    def __init__(self, response):
        self._response = response

    def execute(self, query, args):
        self.last_query_args = args

    def fetchall(self):
        return self._response

def test_resolve_trip_id_returns_none_without_a_number():
    assert resolve_trip_id(_FakeCursor([]), None) is None

def test_resolve_trip_id_returns_none_when_ambiguous():
    cur = _FakeCursor([(101,), (202,)])
    assert resolve_trip_id(cur, "639") is None

def test_resolve_trip_id_returns_none_when_not_found():
    assert resolve_trip_id(_FakeCursor([]), "639") is None

def test_resolve_trip_id_returns_the_single_match():
    cur = _FakeCursor([(101,)])
    assert resolve_trip_id(cur, "639") == 101
