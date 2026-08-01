import json
import os

from austria_poller import parse_at_vehicles

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
    }

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
