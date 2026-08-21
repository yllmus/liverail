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
    api._LIVE_CACHE.pop("AT", None)

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
    api._LIVE_CACHE.pop("PL", None)

    # gtfs_trip_id "does-not-exist" won't resolve against real PL trips, so
    # resolve_delays() legitimately returns nothing - this only exercises
    # the fetch/parse/resolve/dispatch path end to end, real DB included
    # (real PL stop_times are imported as of this test running), not a
    # claim about matching a specific live vehicle.
    resp = client.get("/live?country=PL")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)

def test_live_de_mocks_gtfs_rt_fetch(monkeypatch):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    e = feed.entity.add()
    e.id = "1"
    e.trip_update.trip.trip_id = "does-not-exist"
    stu = e.trip_update.stop_time_update.add()
    stu.stop_id = "does-not-exist-either"
    stu.arrival.delay = 60

    monkeypatch.setattr(api, "fetch_de_feed", lambda: feed)
    api._LIVE_CACHE.pop("DE", None)

    # neither id resolves against real DE trips/stations, so resolve_delays()
    # legitimately returns nothing - this only exercises the
    # fetch/parse/resolve/dispatch path end to end, real DB included (real
    # DE stops/routes/trips are imported as of this test running), same as
    # the PL smoke test above.
    resp = client.get("/live?country=DE")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


# --- Phase 2 of the 2026-08-21 competitive-gap plan -------------------
# These hit the real local Postgres, same as the rest of this file. They
# assert response *shape* and the honesty guarantees rather than specific
# delay figures, which change every five minutes.

def test_operators_summary_returns_a_ranking_or_says_why_not():
    resp = client.get("/operators/summary?country=DE")
    assert resp.status_code == 200
    body = resp.json()
    assert body["meta"]["country"] == "DE"
    assert isinstance(body["operators"], list)
    for row in body["operators"]:
        assert row["operator"]
        assert row["total"] >= 20  # the HAVING floor

def test_operators_summary_explains_an_empty_ranking():
    # PL's feed carries no agency_name at all. An empty table must say why -
    # silence would read as "no data yet" for a gap that is permanent.
    resp = client.get("/operators/summary?country=PL")
    body = resp.json()
    if not body["operators"]:
        assert body["meta"]["note"]
        assert "operator names" in body["meta"]["note"]

def test_operators_summary_csv_export():
    resp = client.get("/operators/summary?country=HU&format=csv")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "attachment" in resp.headers["content-disposition"]
    assert resp.text.splitlines()[0].startswith("operator,")

def test_routes_summary_csv_matches_the_json_rows():
    # The export must come from the same cached rows the page renders, not a
    # second query that could drift from it.
    rows = client.get("/routes/summary?country=HU").json()
    csv_text = client.get("/routes/summary?country=HU&format=csv").text
    assert csv_text.splitlines()[0].startswith("route_name,")
    assert len(csv_text.strip().splitlines()) == len(rows) + 1  # + header

def test_station_board_defaults_to_today():
    resp = client.get("/station/Budapest-Keleti/board?country=HU")
    assert resp.status_code == 200
    body = resp.json()
    assert body["meta"]["station"] == "Budapest-Keleti"
    assert body["meta"]["date"]
    assert isinstance(body["trains"], list)

def test_station_board_rejects_a_malformed_date():
    resp = client.get("/station/Budapest-Keleti/board?country=HU&date=not-a-date")
    assert resp.status_code == 400

def test_station_board_accepts_a_past_date():
    resp = client.get("/station/Budapest-Keleti/board?country=HU&date=2026-08-20")
    assert resp.status_code == 200
    assert resp.json()["meta"]["date"] == "2026-08-20"

def test_train_history_route_is_not_swallowed_by_the_catchall():
    # /train/{country}/{id:path} is greedy and matched /train/DE/x/history
    # until the more specific route was registered first. Guard against a
    # future reordering silently breaking it again.
    resp = client.get("/train/DE/definitely-not-a-real-trip/history")
    assert resp.status_code == 200
    body = resp.json()
    assert "identity_basis" in body
    assert body["has_history"] is False

def test_train_history_reports_its_identity_basis():
    resp = client.get("/train/PL/definitely-not-a-real-trip/history")
    body = resp.json()
    # No static match is possible for a made-up id, so it must fall back to
    # the opaque identifier and say so rather than implying a real join.
    assert body["identity_basis"] == "live_trip_id"
    assert body["mixed_identity"] is False

def test_train_history_rejects_an_unknown_country():
    resp = client.get("/train/ZZ/whatever/history")
    assert resp.status_code == 400

def test_train_history_clamps_the_day_range():
    resp = client.get("/train/DE/nope/history?days=99999")
    assert resp.json()["days"] == api._HISTORY_MAX_DAYS
    resp = client.get("/train/DE/nope/history?days=0")
    assert resp.json()["days"] == 1
