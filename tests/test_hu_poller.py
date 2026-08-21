import json
import os

from poller import parse_vehicles, resolve_trip_id

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "mav_vehicle_positions.json")

def load_fixture():
    with open(FIXTURE) as f:
        return json.load(f)

def test_parse_vehicles_extracts_rows_with_delay():
    rows = parse_vehicles(load_fixture())
    assert len(rows) == 3
    assert rows[0] == {
        "stop_name": "Kecskemet",
        "route_name": "Budapest - Szeged",
        "delay_minutes": 5.0,
        "headsign": "Szeged",
        "lat": 46.9061,
        "lon": 19.6906,
        "live_trip_id": "HU:1:1234",
        "train_number": None,
        "is_cancelled": False,
        "cancellation_kind": None,
        "platform": "3",
    }

def test_parse_vehicles_leaves_platform_none_when_mav_omits_it():
    # ~23% of live vehicles carry no platformCode; that must stay NULL rather
    # than being inferred from anything else.
    rows = parse_vehicles(load_fixture())
    szolnok = next(r for r in rows if r["stop_name"] == "Szolnok")
    assert szolnok["platform"] is None

def test_parse_vehicles_reads_a_cancelled_realtime_state():
    # MAV publishes no cancellations today (0 in a 6,124-stoptime sample) and
    # a vehiclePositions query structurally cannot show a train that isn't
    # running - this exists so a real CANCELED is recorded if that changes,
    # not because HU has cancellation coverage.
    data = load_fixture()
    data["data"]["vehiclePositions"][0]["prevOrCurrentStop"]["realtimeState"] = "CANCELED"
    rows = parse_vehicles(data)
    assert rows[0]["is_cancelled"] is True
    assert rows[0]["cancellation_kind"] == "stop"
    assert rows[0]["delay_minutes"] is None

def test_parse_vehicles_falls_back_to_departure_delay():
    rows = parse_vehicles(load_fixture())
    szolnok = next(r for r in rows if r["stop_name"] == "Szolnok")
    assert szolnok["delay_minutes"] == 1.0

def test_parse_vehicles_skips_entries_missing_stop_relationship():
    rows = parse_vehicles(load_fixture())
    assert all(r["route_name"] != "Missing stop data" for r in rows)

def test_parse_vehicles_empty_input():
    assert parse_vehicles({"data": {"vehiclePositions": []}}) == []
    assert parse_vehicles({}) == []

def test_parse_vehicles_cleans_grouped_route_name():
    rows = parse_vehicles(load_fixture())
    row = next(r for r in rows if r["stop_name"] == "Kiskunfelegyhaza")
    assert row["route_name"] == "Sz 36441"

def test_parse_vehicles_extracts_train_number():
    rows = parse_vehicles(load_fixture())
    row = next(r for r in rows if r["stop_name"] == "Kiskunfelegyhaza")
    assert row["train_number"] == "36441"


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
    assert resolve_trip_id(cur, "5827") is None

def test_resolve_trip_id_returns_none_when_not_found():
    assert resolve_trip_id(_FakeCursor([]), "5827") is None

def test_resolve_trip_id_returns_the_single_match():
    cur = _FakeCursor([(101,)])
    assert resolve_trip_id(cur, "5827") == 101
