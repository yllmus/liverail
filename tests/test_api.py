import json
import os

import pytest
from fastapi.testclient import TestClient

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
