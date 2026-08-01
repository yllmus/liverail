import json
import os

import pytest
from fastapi.testclient import TestClient
from google.transit import gtfs_realtime_pb2

import api

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

client = TestClient(api.app)

# These hit the real local Postgres (train_delay_db), consistent with how
# every other part of this project runs against the real DB rather than a
# separate test-DB harness. They will fail if no Postgres is reachable.

def test_countries_returns_hu_with_attribution_field():
    resp = client.get("/countries")
    assert resp.status_code == 200
    codes = {c["code"]: c for c in resp.json()}
    assert "HU" in codes
    assert "attribution" in codes["HU"]
    assert codes["HU"]["attribution"] is None

def test_stations_defaults_to_hu():
    resp = client.get("/stations")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)

def test_live_hu_mocks_outbound_graphql(monkeypatch):
    class FakeResp:
        def json(self):
            with open(os.path.join(FIXTURE_DIR, "mav_vehicle_positions.json")) as f:
                data = json.load(f)
            data["data"]["vehiclePositions"] = [
                {
                    "lat": 47.5, "lon": 19.1,
                    "trip": {"tripShortName": "IC 123", "route": {"longName": "Test Route"}, "stoptimes": []},
                    "stopRelationship": {"stop": {"name": "Test Stop"}, "status": "IN_TRANSIT_TO"},
                    "prevOrCurrentStop": {"arrivalDelay": 60, "departureDelay": 60},
                }
            ]
            return data

    def fake_post(url, json, timeout):
        return FakeResp()

    monkeypatch.setattr(api.req, "post", fake_post)
    resp = client.get("/live?country=HU")
    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["route_name"] == "Test Route"
    assert body[0]["delay_minutes"] == 1.0

def test_live_at_mocks_radar_subprocess(monkeypatch):
    with open(os.path.join(FIXTURE_DIR, "oebb_radar.json")) as f:
        movements = json.load(f)

    monkeypatch.setattr(api, "call_radar", lambda: movements)
    api._AT_LIVE_CACHE["data"] = None
    api._AT_LIVE_CACHE["expires"] = 0

    resp = client.get("/live?country=AT")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 2
    assert body[0]["route_name"] == "RJ 61"
    assert body[0]["current_stop"] == "Wien Meidling"

def test_live_pl_mocks_gtfs_rt_fetch(monkeypatch):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    e = feed.entity.add()
    e.id = "1"
    e.trip_update.trip.trip_id = "does-not-exist"
    e.trip_update.trip.start_date = "20260801"
    stu = e.trip_update.stop_time_update.add()
    stu.stop_sequence = 0
    stu.arrival.time = 1785551442

    monkeypatch.setattr(api, "fetch_feed", lambda: feed)
    api._PL_LIVE_CACHE["data"] = None
    api._PL_LIVE_CACHE["expires"] = 0

    # gtfs_trip_id "does-not-exist" won't resolve against real PL trips, so
    # resolve_delays() legitimately returns nothing - this only exercises
    # the fetch/parse/resolve/dispatch path end to end, real DB included
    # (real PL stop_times are imported as of this test running), not a
    # claim about matching a specific live vehicle.
    resp = client.get("/live?country=PL")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
