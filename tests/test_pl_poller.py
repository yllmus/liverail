import pytest
from google.transit import gtfs_realtime_pb2

from poland_poller import parse_pl_vehicles, resolve_delays, _scheduled_epoch, _interpolate_position

def make_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"

    e1 = feed.entity.add()
    e1.id = "1"
    e1.trip_update.trip.trip_id = "T1"
    e1.trip_update.trip.start_date = "20260801"
    stu1 = e1.trip_update.stop_time_update.add()
    stu1.stop_sequence = 3
    stu1.arrival.time = 1785551442

    e2 = feed.entity.add()
    e2.id = "2"
    e2.trip_update.trip.trip_id = "T2"
    e2.trip_update.trip.start_date = "20260801"
    stu2 = e2.trip_update.stop_time_update.add()
    stu2.stop_sequence = 1
    stu2.departure.time = 1785551600
    # no arrival at all on this one - should fall back to departure.time

    e3 = feed.entity.add()
    e3.id = "3"
    e3.trip_update.trip.trip_id = "T3"
    e3.trip_update.trip.start_date = "20260801"
    # no stop_time_update entries at all - should be skipped

    return feed

def test_parse_pl_vehicles_extracts_raw_rows():
    rows = parse_pl_vehicles(make_feed())
    assert len(rows) == 2
    assert rows[0] == {
        "gtfs_trip_id": "T1",
        "start_date": "20260801",
        "stop_sequence": 3,
        "actual_epoch": 1785551442,
    }

def test_parse_pl_vehicles_falls_back_to_departure_time():
    rows = parse_pl_vehicles(make_feed())
    row = next(r for r in rows if r["gtfs_trip_id"] == "T2")
    assert row["actual_epoch"] == 1785551600

def test_parse_pl_vehicles_skips_entries_with_no_stop_time_update():
    rows = parse_pl_vehicles(make_feed())
    assert all(r["gtfs_trip_id"] != "T3" for r in rows)

def test_parse_pl_vehicles_empty_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    assert parse_pl_vehicles(feed) == []

def test_scheduled_epoch_same_day():
    # 2026-08-01 10:00:00 Europe/Warsaw (CEST, UTC+2) == 08:00:00 UTC
    epoch = _scheduled_epoch("20260801", "10:00:00")
    assert epoch == 1785571200.0

def test_scheduled_epoch_past_midnight_rolls_to_next_day():
    # "25:00:00" on 2026-08-01 means 01:00:00 on 2026-08-02 local time
    epoch = _scheduled_epoch("20260801", "25:00:00")
    epoch_next_day_1am = _scheduled_epoch("20260802", "01:00:00")
    assert epoch == epoch_next_day_1am

def test_scheduled_epoch_no_time_returns_none():
    assert _scheduled_epoch("20260801", None) is None
    assert _scheduled_epoch("20260801", "") is None


class _FakeCursor:
    # Pass a single value to have every fetchone() call return it (used by
    # the older tests below, and by anything only exercising the first
    # query). Pass a list to have successive execute()+fetchone() calls
    # consume one entry each, in order - needed once resolve_delays started
    # issuing a second query per row (the previous-stop lookup for position
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

def test_resolve_delays_computes_delay_from_scheduled_and_actual():
    # station_id=42, scheduled_arrival="09:59:00" (2026-08-01, Warsaw),
    # actual arrival 60s later than scheduled. No previous-stop row (second
    # fetchone() call returns None) so no position is estimated here -
    # covered separately below.
    scheduled = _scheduled_epoch("20260801", "09:59:00")
    cur = _FakeCursor([
        (42, "09:59:00", None, "Warszawa Centralna", 52.2297, 21.0122, "IC 1", "Kraków Główny"),
        None,
    ])
    rows = [{
        "gtfs_trip_id": "T1", "start_date": "20260801",
        "stop_sequence": 3, "actual_epoch": scheduled + 60,
    }]
    resolved = resolve_delays(cur, rows)
    assert len(resolved) == 1
    assert resolved[0]["station_id"] == 42
    assert resolved[0]["station_name"] == "Warszawa Centralna"
    assert resolved[0]["route_name"] == "IC 1"
    assert resolved[0]["headsign"] == "Kraków Główny"
    assert resolved[0]["delay_minutes"] == 1.0
    assert resolved[0]["lat"] is None
    assert resolved[0]["lon"] is None
    assert resolved[0]["position_source"] is None

def test_resolve_delays_skips_unmatched_trips():
    cur = _FakeCursor(None)
    rows = [{
        "gtfs_trip_id": "unknown", "start_date": "20260801",
        "stop_sequence": 0, "actual_epoch": 1785551442,
    }]
    assert resolve_delays(cur, rows) == []

def test_resolve_delays_estimates_position_from_previous_stop():
    # Previous stop (Warszawa Centralna) scheduled to depart 09:00, target
    # stop (Kraków Główny) scheduled to arrive 12:00 - both same day. Poll
    # happens exactly 1/4 of the way through that window (09:45), so the
    # estimated marker should sit 1/4 of the way between the two stations.
    prev_departure = _scheduled_epoch("20260801", "09:00:00")
    target_arrival = _scheduled_epoch("20260801", "12:00:00")
    now = prev_departure + (target_arrival - prev_departure) * 0.25
    cur = _FakeCursor([
        (42, "12:00:00", None, "Kraków Główny", 50.0680, 19.9476, "IC 1", None),
        (52.2297, 21.0122, "09:00:00", None),  # previous stop: Warszawa Centralna
    ])
    rows = [{
        "gtfs_trip_id": "T1", "start_date": "20260801",
        "stop_sequence": 5, "actual_epoch": target_arrival,
    }]
    resolved = resolve_delays(cur, rows, now_epoch=now)
    assert len(resolved) == 1
    assert resolved[0]["position_source"] == "estimated"
    expected_lat = 52.2297 + 0.25 * (50.0680 - 52.2297)
    expected_lon = 21.0122 + 0.25 * (19.9476 - 21.0122)
    assert resolved[0]["lat"] == pytest.approx(expected_lat)
    assert resolved[0]["lon"] == pytest.approx(expected_lon)

def test_interpolate_position_clamps_to_target_when_now_is_past_arrival():
    lat, lon = _interpolate_position(
        prev_lat=52.0, prev_lon=21.0, prev_epoch=1000,
        target_lat=50.0, target_lon=19.0, target_epoch=2000,
        now_epoch=5000,  # well past target_epoch
    )
    assert lat == pytest.approx(50.0)
    assert lon == pytest.approx(19.0)

def test_interpolate_position_returns_none_without_a_previous_stop():
    lat, lon = _interpolate_position(
        prev_lat=None, prev_lon=None, prev_epoch=None,
        target_lat=50.0, target_lon=19.0, target_epoch=2000,
        now_epoch=1500,
    )
    assert lat is None
    assert lon is None
