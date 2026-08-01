from google.transit import gtfs_realtime_pb2

from poland_poller import parse_pl_vehicles, resolve_delays, _scheduled_epoch

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
    def __init__(self, response):
        self._response = response
        self.last_query_args = None

    def execute(self, query, args):
        self.last_query_args = args

    def fetchone(self):
        return self._response

def test_resolve_delays_computes_delay_from_scheduled_and_actual():
    # station_id=42, scheduled_arrival="09:59:00" (2026-08-01, Warsaw),
    # actual arrival 60s later than scheduled
    scheduled = _scheduled_epoch("20260801", "09:59:00")
    cur = _FakeCursor((42, "09:59:00", None, "Warszawa Centralna", "IC 1", "Kraków Główny"))
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

def test_resolve_delays_skips_unmatched_trips():
    cur = _FakeCursor(None)
    rows = [{
        "gtfs_trip_id": "unknown", "start_date": "20260801",
        "stop_sequence": 0, "actual_epoch": 1785551442,
    }]
    assert resolve_delays(cur, rows) == []
