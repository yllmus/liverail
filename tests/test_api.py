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

@pytest.mark.slow  # whole-country aggregate; see pytest.ini / TESTING.md
def test_operators_summary_returns_a_ranking_or_says_why_not():
    resp = client.get("/operators/summary?country=DE")
    assert resp.status_code == 200
    body = resp.json()
    assert body["meta"]["country"] == "DE"
    assert isinstance(body["operators"], list)
    for row in body["operators"]:
        assert row["operator"]
        assert row["total"] >= 20  # the HAVING floor

@pytest.mark.slow  # whole-country aggregate; see pytest.ini / TESTING.md
def test_operators_summary_explains_an_empty_ranking():
    # PL's feed carries no agency_name at all. An empty table must say why -
    # silence would read as "no data yet" for a gap that is permanent.
    resp = client.get("/operators/summary?country=PL")
    body = resp.json()
    if not body["operators"]:
        assert body["meta"]["note"]
        assert "operator names" in body["meta"]["note"]

@pytest.mark.slow  # whole-country aggregate; see pytest.ini / TESTING.md
def test_operators_summary_csv_export():
    resp = client.get("/operators/summary?country=HU&format=csv")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "attachment" in resp.headers["content-disposition"]
    assert resp.text.splitlines()[0].startswith("operator,")

@pytest.mark.slow  # whole-country aggregate; see pytest.ini / TESTING.md
def test_routes_summary_csv_matches_the_json_rows():
    # The export must come from the same cached rows the page renders, not a
    # second query that could drift from it.
    rows = client.get("/routes/summary?country=HU").json()
    csv_text = client.get("/routes/summary?country=HU&format=csv").text
    assert csv_text.splitlines()[0].startswith("route_name,")
    assert len(csv_text.strip().splitlines()) == len(rows) + 1  # + header

@pytest.mark.slow  # same missing index as the past-date case above
def test_station_board_defaults_to_today():
    resp = client.get("/station/Budapest-Keleti/board?country=HU")
    assert resp.status_code == 200
    body = resp.json()
    assert body["meta"]["station"] == "Budapest-Keleti"
    assert body["meta"]["date"]
    assert isinstance(body["trains"], list)

@pytest.mark.slow  # same missing index as the past-date case above
def test_station_board_rejects_a_malformed_date():
    resp = client.get("/station/Budapest-Keleti/board?country=HU&date=not-a-date")
    assert resp.status_code == 400

@pytest.mark.slow  # NOT an aggregate like the others - this one is a real missing
                   # index. /station/{name}/board filters observed_date, but
                   # idx_delay_obs_station_country is (station_id, country_code)
                   # only, so EXPLAIN shows observed_date as a Filter rather than
                   # an Index Cond: every row for a busy station is heap-fetched
                   # and discarded. Today's date is fast only because those rows
                   # are still in page cache. Fix is an index - see TESTING.md.
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

# --- typo-tolerant search (/search) -----------------------------------
# Runs against the real local search_index, same as the rest of this suite
# runs against real local Postgres.

def test_search_requires_no_exact_spelling():
    # The whole point of the endpoint: this returned nothing at all under the
    # old client-side substring match.
    resp = client.get("/search?q=Budpest&country=HU&limit=5")
    assert resp.status_code == 200
    names = [r["name"] for r in resp.json()["results"]]
    assert any("Budapest" in n for n in names)

def test_search_folds_diacritics_the_rider_cannot_type():
    # `Kraków Główny` was unreachable from an ASCII keyboard before the
    # search_text folding existed - measured zero results.
    resp = client.get("/search?q=Krakow&country=PL&limit=5")
    names = [r["name"] for r in resp.json()["results"]]
    assert any("Kraków" in n for n in names)

def test_search_ranks_the_busy_station_above_the_lookalike():
    # "Munchen" scored Munchhausen (0 observations) above München Hbf on pure
    # trigram similarity. The popularity term is what fixed it.
    resp = client.get("/search?q=Munchen&country=DE&limit=5")
    results = resp.json()["results"]
    assert results and results[0]["observations"] > 0

def test_search_kind_filter_returns_only_that_kind():
    resp = client.get("/search?q=Budapest&country=HU&kind=route&limit=10")
    assert all(r["kind"] == "route" for r in resp.json()["results"])

def test_search_empty_query_is_not_an_error():
    resp = client.get("/search?q=%20%20&country=HU")
    assert resp.status_code == 200
    assert resp.json()["results"] == []

def test_search_limit_is_clamped():
    resp = client.get("/search?q=a&country=HU&limit=9999")
    assert len(resp.json()["results"]) <= 50

# --- SEO safety -------------------------------------------------------

@pytest.mark.slow  # /sitemap.xml warms routes_list for ALL FOUR countries;
                   # ~60-90s cold (nginx's timeout was raised to 120s for it)
def test_thin_pages_are_excluded_from_the_sitemap():
    resp = client.get("/sitemap.xml")
    assert resp.status_code == 200
    conn = api.get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM search_index
        WHERE kind = 'station' AND observations < %s
        LIMIT 1
    """, (api._SEO_MIN_OBSERVATIONS,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if row:
        from urllib.parse import quote
        assert f"/station/{quote(row[0])}" not in resp.text

def test_a_thin_page_still_renders_but_says_noindex():
    # The page is not removed - a rider following a link to a quiet halt
    # still gets a real answer. It is only kept out of the index.
    conn = api.get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT country_code, name FROM search_index
        WHERE kind = 'station' AND observations = 0 LIMIT 1
    """)
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        return
    resp = client.get(f"/t/{row[0]}/station/{row[1]}")
    assert resp.status_code == 200
    assert 'content="noindex,follow"' in resp.text

@pytest.mark.slow  # the SEO page for a BUSY station (Budapest-Keleti, 164k+ rows).
                   # The thin-page test above passes instantly - it is row volume
                   # on a cold page cache, same family as the board tests.
def test_a_busy_page_is_still_indexable():
    resp = client.get("/t/HU/station/Budapest-Keleti")
    assert resp.status_code == 200
    assert "noindex" not in resp.text

# --- Phase 2: per-stop delay profile ----------------------------------
# DE and PL are used throughout: their identity resolution is a direct
# gtfs_trip_id lookup. HU and AT would call get_live_*() and hit the network.

def test_train_stops_route_is_not_swallowed_by_the_catchall():
    # Same greedy-route hazard the /history test guards. /stops was registered
    # after /history and before /train/{country}/{id:path}; a future reorder
    # would silently turn this into the catch-all.
    resp = client.get("/train/DE/definitely-not-a-real-trip/stops")
    assert resp.status_code == 200
    assert "stops" in resp.json()

def test_train_stops_explains_an_unresolved_trip_instead_of_returning_nothing():
    # Phase 1 writes stop_sequence only when the trip resolved, so an
    # unresolved train has no stop rows at all. An empty list on its own would
    # read as "this train calls nowhere" - it must say why.
    body = client.get("/train/PL/definitely-not-a-real-trip/stops").json()
    assert body["stops"] == []
    assert body["reason"]
    assert "stop identity" in body["reason"]

def test_train_stops_flags_that_at_publishes_no_arrival_delays():
    # Structural, not a thin sample: OBB's radar() never sets arrivalDelay
    # (0/314 measured), so the arrival columns are permanently NULL for AT.
    # Rendering that as "never late arriving" would be exactly backwards.
    assert client.get("/train/DE/nope/stops").json()["arrival_available"] is True
    assert client.get("/train/PL/nope/stops").json()["arrival_available"] is True

def test_train_stops_rejects_an_unknown_country():
    assert client.get("/train/ZZ/whatever/stops").status_code == 400

def test_train_stops_clamps_the_day_range():
    assert client.get("/train/DE/nope/stops?days=99999").json()["days"] == api._HISTORY_MAX_DAYS
    assert client.get("/train/DE/nope/stops?days=0").json()["days"] == 1

# --- Phase 2: delay distributions -------------------------------------

def test_distribution_requires_a_station():
    assert client.get("/distribution?country=AT").status_code == 400

def test_distribution_404s_on_an_unknown_station():
    assert client.get("/distribution?country=AT&station=Nowhere-At-All").status_code == 404

def test_distribution_reports_which_ladder_rung_answered():
    # A figure whose provenance is invisible is worse than no figure: "based
    # on this exact train" and "based on the station generally" are different
    # claims. basis must always say which.
    r = client.get("/distribution?country=AT&station=Wien Quartier Belvedere")
    assert r.status_code == 200
    b = r.json()
    if b["found"]:
        assert b["basis"] in ("trip", "route", "station")
        assert b["sample_count"] >= b["min_sample"]

def test_distribution_refuses_rather_than_quoting_a_thin_sample():
    # An unreachable minimum must exhaust the ladder and say so, not return
    # the coarsest rung anyway. A probability from four observations is a lie
    # with a decimal point.
    b = client.get("/distribution?country=AT&station=Wien Quartier Belvedere"
                   "&min_sample=999999999").json()
    assert b["found"] is False
    assert b["basis"] is None
    assert b["reason"]

def test_distribution_falls_back_when_the_route_is_unknown():
    # A route with no cell must drop to the station rung, not 404 and not
    # silently report the route as its basis.
    b = client.get("/distribution?country=AT&station=Wien Quartier Belvedere"
                   "&route_name=NO-SUCH-ROUTE-ZZZ").json()
    if b["found"]:
        assert b["basis"] == "station"

# --- Phase 3: journey planning ----------------------------------------

def test_plan_is_501_for_countries_without_a_planner():
    # Germany alone now. HU borrows MÁV's OTP, AT borrows ÖBB's HAFAS, and PL
    # is planned from our own imported timetable by a connection scan. DE's
    # HAFAS is shut down AND its poller samples too few stops per trip to
    # support the search, so it is the one country still without one.
    # Not 404 and not an empty result: the capability genuinely does not
    # exist, and the response says why rather than implying no route exists.
    for c in ("DE",):
        r = client.get(f"/plan?country={c}&origin=A&destination=B")
        assert r.status_code == 501
        # Must explain what is actually missing, not just refuse. A bare 501
        # would read as a temporary outage rather than an unbuilt capability.
        assert "connection-scan" in r.json()["detail"].lower()

def test_plan_requires_both_endpoints():
    assert client.get("/plan?country=HU&origin=Budapest-Keleti").status_code == 400

def test_plan_rejects_a_malformed_date_or_time():
    assert client.get("/plan?country=HU&origin=Debrecen&destination=Sopron"
                      "&date=22-08-2026").status_code == 400
    assert client.get("/plan?country=HU&origin=Debrecen&destination=Sopron"
                      "&time_=9am").status_code == 400

def test_plan_404s_on_an_unknown_station():
    r = client.get("/plan?country=HU&origin=Nowhere-At-All-XYZ&destination=Sopron")
    assert r.status_code == 404

def test_plan_rejects_identical_endpoints():
    r = client.get("/plan?country=HU&origin=Debrecen&destination=Debrecen")
    assert r.status_code == 400

# --- Phase 4: transfer probability ------------------------------------

def test_transfer_risk_requires_a_station():
    assert client.get("/transfer/risk?country=AT").status_code == 400

def test_transfer_risk_refuses_when_no_rung_is_deep_enough():
    # Suppressed entirely rather than computed from the connector alone: the
    # feeder's delay is what decides a transfer, and a figure missing it would
    # be a guess wearing a number.
    b = client.get("/transfer/risk?country=AT&station=Wien Quartier Belvedere"
                   "&buffer_min=10&min_sample=999999999").json()
    assert b["assessed"] is False
    assert b["reason"]

def test_transfer_risk_always_flags_the_interchange_assumption():
    # No feed publishes a minimum interchange time for AT, so the figure used
    # is a default. It must never be presented as measured.
    b = client.get("/transfer/risk?country=AT&station=Wien Quartier Belvedere"
                   "&buffer_min=10").json()
    if b["assessed"]:
        assert b["interchange_is_assumption"] is True
        assert b["interchange_note"]

def test_transfer_risk_is_monotonic_in_the_buffer():
    # More slack can never make a connection less likely. This caught a real
    # modelling bug: `worst` was being treated as the 100th percentile, which
    # pinned every buffer from 8 to 40 minutes at the same probability.
    ps = []
    for buf in (2, 4, 6, 8, 20):
        b = client.get("/transfer/risk?country=AT&station=Wien Quartier Belvedere"
                       f"&buffer_min={buf}").json()
        if b["assessed"] and b["probability"] is not None:
            ps.append(b["probability"])
    assert ps == sorted(ps)
    assert ps and ps[0] < ps[-1]

def test_transfer_risk_never_claims_better_than_the_modelled_tail():
    b = client.get("/transfer/risk?country=AT&station=Wien Quartier Belvedere"
                   "&buffer_min=600").json()
    if b["assessed"]:
        assert b["probability"] <= 0.95
        assert b["tail_unmodelled_above_pct"] == 95

def test_plan_supports_every_registered_planner_country():
    # The registry is the single source of truth for which countries can plan;
    # a country in it must not return 501.
    # INT is in the registry too, but it is not a country - it selects the
    # cross-border planner rather than a national one.
    assert set(api._PLAN_PROVIDERS) == {"HU", "AT", "PL", "INT"}
    for c in api._PLAN_PROVIDERS:
        assert client.get(f"/plan?country={c}&origin=X").status_code != 501

# --- Safer-train suggestions ------------------------------------------

def test_safer_connectors_are_offered_only_for_a_tight_change():
    # Attaching alternatives to a comfortable change invites second-guessing a
    # decision that is already fine, and costs a query plus a risk assessment
    # each. The presence of the block is itself the signal.
    r = client.get("/plan?country=PL&origin=Warszawa Centralna"
                   "&destination=Kraków Główny&time_=08:00&itineraries=3")
    assert r.status_code == 200
    for it in r.json()["itineraries"]:
        for t in it["transfers"]:
            risk = t.get("risk") or {}
            if t.get("alternatives"):
                assert risk.get("label") in ("tight", "unlikely")

def test_safer_connectors_actually_give_more_buffer():
    # An "alternative" that leaves sooner than the one already listed is not an
    # alternative, it is a worse option wearing the same label.
    r = client.get("/plan?country=PL&origin=Warszawa Centralna"
                   "&destination=Kraków Główny&time_=08:00&itineraries=3")
    for it in r.json()["itineraries"]:
        for t in it["transfers"]:
            for alt in t.get("alternatives") or []:
                assert alt["buffer_min"] > t["buffer_min"]

def test_plan_never_leaks_its_internal_time_keys():
    r = client.get("/plan?country=PL&origin=Warszawa Centralna"
                   "&destination=Kraków Główny&time_=08:00&itineraries=2")
    for it in r.json()["itineraries"]:
        for t in it["transfers"]:
            assert "_arrive_hm" not in t and "_depart_hm" not in t

# --- Cross-border journeys --------------------------------------------

def test_int_is_a_registered_planner():
    assert api._PLAN_PROVIDERS.get("INT")

def test_resolve_station_anywhere_prefers_depth_over_an_exact_match():
    # The regression this exists for: DE's import holds a station literally
    # named "Bohumin" with zero observations, while PL holds the same station
    # as "Bohumín" with ~19,000. Returning the exact hit reported "no data"
    # for a station we have thousands of records for.
    conn = api.get_conn()
    cur = conn.cursor()
    try:
        row = api.resolve_station_anywhere(cur, "Bohumin")
    finally:
        cur.close(); conn.close()
    if row:
        assert row[0] == "PL", f"expected PL to win on depth, got {row}"
        assert row[4] > 0

def test_resolve_station_anywhere_strips_hafas_platform_groups():
    # HAFAS appends platform groups in two shapes, only one parenthesised.
    conn = api.get_conn()
    cur = conn.cursor()
    try:
        for name in ("Wien Hbf (Bahnsteige 3-12)", "München Hbf Gl.5-10"):
            row = api.resolve_station_anywhere(cur, name)
            assert row is not None, f"{name} resolved to nothing"
            assert row[4] > 0, f"{name} resolved to a station with no history: {row}"
    finally:
        cur.close(); conn.close()

def test_resolve_station_anywhere_returns_none_for_a_country_we_do_not_import():
    conn = api.get_conn()
    cur = conn.cursor()
    try:
        row = api.resolve_station_anywhere(cur, "Zzzz Not A Real Station Xyz")
    finally:
        cur.close(); conn.close()
    assert row is None

def test_search_scope_all_crosses_countries():
    # The point of a four-country site: someone reading Hungarian delays who
    # searches for Wien should find it.
    b = client.get("/search?q=Wien&country=HU&scope=all&limit=5").json()
    assert b["scope"] == "all"
    assert any(r["country_code"] == "AT" for r in b["results"])

def test_search_default_scope_is_unchanged():
    # Every existing caller passes no scope and must keep country-only
    # behaviour - a silent widening would change what the whole frontend shows.
    b = client.get("/search?q=Wien&country=HU&limit=5").json()
    assert b["scope"] == "country"
    assert all(r.get("country_code") == "HU" for r in b["results"])

def test_search_scope_all_dedupes_one_station_held_by_several_feeds():
    # Budapest-Keleti is in AT's, DE's and HU's imports; only HU's has history.
    b = client.get("/search?q=Budapest-Keleti&country=AT&scope=all&limit=8").json()
    keleti = [r for r in b["results"] if "keleti" in (r["label"] or "").lower()]
    assert len(keleti) <= 1, f"duplicated across feeds: {keleti}"
    if keleti:
        assert keleti[0]["country_code"] == "HU"
