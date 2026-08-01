from google.transit import gtfs_realtime_pb2

from germany_poller import parse_de_vehicles, resolve_delays

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
    def __init__(self, response):
        self._response = response
        self.last_query_args = None

    def execute(self, query, args):
        self.last_query_args = args

    def fetchone(self):
        return self._response

def test_resolve_delays_translates_child_stop_id_via_parent_map():
    # gtfs.de's feed always gives a platform/track-level child stop_id
    # (confirmed against real data - see germany_poller.py's comment); the
    # parent_map is what import.py's stops.txt parent_station filter left
    # out of `stations`, so resolve_delays must translate through it.
    cur = _FakeCursor((7, "Aachen, Hbf", "RE1"))
    parent_map = {"424107": "437028"}
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "424107", "delay_minutes": 3.0}]
    resolved = resolve_delays(cur, rows, parent_map=parent_map)
    assert len(resolved) == 1
    assert resolved[0] == {
        "station_id": 7,
        "station_name": "Aachen, Hbf",
        "route_name": "RE1",
        "delay_minutes": 3.0,
    }
    # the query must have been issued against the *parent* id, not the raw one
    assert "437028" in cur.last_query_args

def test_resolve_delays_skips_stop_id_with_no_parent_in_map():
    cur = _FakeCursor((7, "Aachen, Hbf", "RE1"))
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "unknown-stop", "delay_minutes": 3.0}]
    assert resolve_delays(cur, rows, parent_map={}) == []

def test_resolve_delays_skips_unmatched_trips():
    cur = _FakeCursor(None)
    parent_map = {"424107": "437028"}
    rows = [{"gtfs_trip_id": "unknown", "gtfs_stop_id": "424107", "delay_minutes": 3.0}]
    assert resolve_delays(cur, rows, parent_map=parent_map) == []
