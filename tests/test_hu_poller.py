import json
import os

from poller import parse_vehicles

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "mav_vehicle_positions.json")

def load_fixture():
    with open(FIXTURE) as f:
        return json.load(f)

def test_parse_vehicles_extracts_rows_with_delay():
    rows = parse_vehicles(load_fixture())
    assert len(rows) == 2
    assert rows[0] == {
        "stop_name": "Kecskemet",
        "route_name": "Budapest - Szeged",
        "delay_minutes": 5.0,
        "headsign": "Szeged",
        "lat": 46.9061,
        "lon": 19.6906,
        "live_trip_id": "HU:1:1234",
    }

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
