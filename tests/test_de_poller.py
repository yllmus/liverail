import pytest
from google.transit import gtfs_realtime_pb2

from germany_poller import parse_de_vehicles, resolve_delays, _scheduled_epoch, _interpolate_position

def make_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"

    e1 = feed.entity.add()
    e1.id = "1"
    e1.trip_update.trip.trip_id = "T1"
    stu1 = e1.trip_update.stop_time_update.add()
    stu1.stop_id = "424107"
    stu1.arrival.delay = 180
    stu1.arrival.time = 1785551442

    e2 = feed.entity.add()
    e2.id = "2"
    e2.trip_update.trip.trip_id = "T2"
    stu2 = e2.trip_update.stop_time_update.add()
    stu2.stop_id = "555000"
    stu2.departure.delay = 60
    # no arrival at all on this one - should fall back to departure.delay

    e3 = feed.entity.add()
    e3.id = "3"
    e3.trip_update.trip.trip_id = "T3"
    # no stop_time_update entries at all - should be skipped

    e4 = feed.entity.add()
    e4.id = "4"
    e4.trip_update.trip.trip_id = "T4"
    stu4 = e4.trip_update.stop_time_update.add()
    stu4.stop_id = "999999"
    stu4.arrival.time = 1785551442
    # arrival.time set but no delay field at all - should be skipped

    e5 = feed.entity.add()
    e5.id = "5"
    # no trip_id at all - should be skipped
    stu5 = e5.trip_update.stop_time_update.add()
    stu5.stop_id = "111111"
    stu5.arrival.delay = 0

    return feed

def test_parse_de_vehicles_extracts_raw_rows():
    rows = parse_de_vehicles(make_feed())
    assert len(rows) == 2
    assert rows[0] == {
        "gtfs_trip_id": "T1",
        "gtfs_stop_id": "424107",
        "delay_minutes": 3.0,
        "start_date": None,
    }

def test_parse_de_vehicles_falls_back_to_departure_delay():
    rows = parse_de_vehicles(make_feed())
    row = next(r for r in rows if r["gtfs_trip_id"] == "T2")
    assert row["delay_minutes"] == 1.0

def test_parse_de_vehicles_skips_entries_with_no_stop_time_update():
    rows = parse_de_vehicles(make_feed())
    assert all(r["gtfs_trip_id"] != "T3" for r in rows)

def test_parse_de_vehicles_skips_entries_with_no_delay_field():
    rows = parse_de_vehicles(make_feed())
    assert all(r["gtfs_trip_id"] != "T4" for r in rows)

def test_parse_de_vehicles_skips_entries_with_no_trip_id():
    rows = parse_de_vehicles(make_feed())
    assert all(r["gtfs_stop_id"] != "111111" for r in rows)

def test_parse_de_vehicles_empty_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    assert parse_de_vehicles(feed) == []


class _FakeCursor:
    # Pass a single value to have every fetchone() call return it. Pass a
    # list to have successive execute()+fetchone() calls consume one entry
    # each, in order - needed once resolve_delays started issuing an
    # optional second query per row (the previous-stop lookup for position
    # interpolation).
    def __init__(self, response):
        self._response = response
        self._is_sequence = isinstance(response, list)
        self._call_idx = 0
        self.last_query_args = None

    def execute(self, query, args):
        self.last_query_args = args

    def fetchone(self):
        if not self._is_sequence:
            return self._response
        r = self._response[self._call_idx] if self._call_idx < len(self._response) else None
        self._call_idx += 1
        return r

def test_resolve_delays_translates_child_stop_id_via_parent_map():
    # gtfs.de's feed always gives a platform/track-level child stop_id
    # (confirmed against real data - see germany_poller.py's comment); the
    # parent_map is what import.py's stops.txt parent_station filter left
    # out of `stations`, so resolve_delays must translate through it. No
    # scheduled_arrival/departure here (None, None) so position estimation
    # is skipped - covered separately below.
    cur = _FakeCursor((7, "Aachen, Hbf", 50.7678, 6.0913, "RE1", 99, None, None))
    parent_map = {"424107": "437028"}
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "424107", "delay_minutes": 3.0, "start_date": "20260801"}]
    resolved = resolve_delays(cur, rows, parent_map=parent_map)
    assert len(resolved) == 1
    assert resolved[0] == {
        "station_id": 7,
        "station_name": "Aachen, Hbf",
        "route_name": "RE1",
        "delay_minutes": 3.0,
        "lat": None,
        "lon": None,
        "position_source": None,
        "trip_id": 99,
    }
    # the query must have been issued against the *parent* id, not the raw one
    assert "437028" in cur.last_query_args

def test_resolve_delays_skips_stop_id_with_no_parent_in_map():
    cur = _FakeCursor((7, "Aachen, Hbf", 50.7678, 6.0913, "RE1", 99, None, None))
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "unknown-stop", "delay_minutes": 3.0, "start_date": "20260801"}]
    assert resolve_delays(cur, rows, parent_map={}) == []

def test_resolve_delays_skips_unmatched_trips():
    cur = _FakeCursor(None)
    parent_map = {"424107": "437028"}
    rows = [{"gtfs_trip_id": "unknown", "gtfs_stop_id": "424107", "delay_minutes": 3.0, "start_date": "20260801"}]
    assert resolve_delays(cur, rows, parent_map=parent_map) == []

def test_resolve_delays_estimates_position_from_previous_stop():
    # Target (Aachen Hbf) scheduled arrival 10:00, 3 min delay -> actual
    # 10:03. Previous stop (Köln Hbf) scheduled departure 09:00. Poll
    # happens exactly halfway between 09:00 and 10:03, so the estimated
    # marker should sit halfway between the two stations.
    target_scheduled = _scheduled_epoch("20260801", "10:00:00")
    target_actual = target_scheduled + 3 * 60
    prev_departure = _scheduled_epoch("20260801", "09:00:00")
    now = prev_departure + (target_actual - prev_departure) * 0.5
    cur = _FakeCursor([
        (7, "Aachen, Hbf", 50.7678, 6.0913, "RE1", 99, "10:00:00", None),
        (50.9429, 6.9578, "09:00:00", None),  # previous stop: Köln Hbf
    ])
    parent_map = {"424107": "437028"}
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "424107", "delay_minutes": 3.0, "start_date": "20260801"}]
    resolved = resolve_delays(cur, rows, parent_map=parent_map, now_epoch=now)
    assert len(resolved) == 1
    assert resolved[0]["position_source"] == "estimated"
    expected_lat = 50.9429 + 0.5 * (50.7678 - 50.9429)
    expected_lon = 6.9578 + 0.5 * (6.0913 - 6.9578)
    assert resolved[0]["lat"] == pytest.approx(expected_lat)
    assert resolved[0]["lon"] == pytest.approx(expected_lon)

def test_interpolate_position_returns_none_without_a_previous_stop():
    lat, lon = _interpolate_position(
        prev_lat=None, prev_lon=None, prev_epoch=None,
        target_lat=50.0, target_lon=6.0, target_epoch=2000,
        now_epoch=1500,
    )
    assert lat is None
    assert lon is None
