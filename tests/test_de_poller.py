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

def make_cancelled_feed():
    # gtfs.de never sets the trip-level CANCELED flag (confirmed 0/7229 on the
    # real feed), so a fully cancelled train is only recognizable as one whose
    # stop_time_updates are *all* SKIPPED. A train that merely drops one stop
    # skips that stop and keeps the rest - a different fact for a rider, so it
    # is recorded as a different cancellation_kind.
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    skipped = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED

    e1 = feed.entity.add()
    e1.id = "c1"
    e1.trip_update.trip.trip_id = "WHOLE_TRIP_GONE"
    for stop_id in ("424107", "424108"):
        stu = e1.trip_update.stop_time_update.add()
        stu.stop_id = stop_id
        stu.schedule_relationship = skipped

    e2 = feed.entity.add()
    e2.id = "c2"
    e2.trip_update.trip.trip_id = "DROPS_ONE_STOP"
    stu = e2.trip_update.stop_time_update.add()
    stu.stop_id = "424107"
    stu.schedule_relationship = skipped
    later = e2.trip_update.stop_time_update.add()
    later.stop_id = "424109"
    later.arrival.delay = 120
    later.arrival.time = 1785551442

    return feed

def test_parse_de_vehicles_extracts_raw_rows():
    rows = parse_de_vehicles(make_feed())
    assert len(rows) == 2
    assert rows[0] == {
        "gtfs_trip_id": "T1",
        "gtfs_stop_id": "424107",
        "delay_minutes": 3.0,
        "delay_arrival_minutes": 3.0,
        "delay_departure_minutes": None,
        "start_date": None,
        "cancellation_kind": None,
    }

def test_parse_de_vehicles_reads_all_stops_skipped_as_a_cancelled_trip():
    rows = parse_de_vehicles(make_cancelled_feed())
    row = next(r for r in rows if r["gtfs_trip_id"] == "WHOLE_TRIP_GONE")
    assert row["cancellation_kind"] == "trip"
    assert row["delay_minutes"] is None

def test_parse_de_vehicles_reads_one_skipped_stop_as_stop_scoped():
    # The train still runs, so this must not be reported as a cancelled trip.
    rows = parse_de_vehicles(make_cancelled_feed())
    row = next(r for r in rows if r["gtfs_trip_id"] == "DROPS_ONE_STOP")
    assert row["cancellation_kind"] == "stop"
    assert row["delay_minutes"] is None

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
    cur = _FakeCursor((7, "Aachen, Hbf", 50.7678, 6.0913, "RE1", 99, None, None, 4))
    parent_map = {"424107": "437028"}
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "424107", "delay_minutes": 3.0, "start_date": "20260801", "cancellation_kind": None}]
    resolved = resolve_delays(cur, rows, parent_map=parent_map, platform_map={})
    assert len(resolved) == 1
    assert resolved[0] == {
        "station_id": 7,
        "station_name": "Aachen, Hbf",
        "route_name": "RE1",
        "delay_minutes": 3.0,
        # stop_sequence comes from our own stop_times via the join, not from
        # the feed - see schema/008_stop_identity.sql. The split delays are
        # None because this row was built without them; parse_de_vehicles is
        # what fills them, covered separately above.
        "stop_sequence": 4,
        "scheduled_arrival": None,
        "scheduled_departure": None,
        "delay_arrival_minutes": None,
        "delay_departure_minutes": None,
        "lat": None,
        "lon": None,
        "position_source": None,
        "trip_id": 99,
        # carried through unresolved so /train/DE/{id} can find this trip
        # again in a freshly-fetched feed - our integer trip_id can't
        # address the feed itself.
        "gtfs_trip_id": "T1",
        "is_cancelled": False,
        "cancellation_kind": None,
        "platform": None,
    }
    # the query must have been issued against the *parent* id, not the raw one
    assert "437028" in cur.last_query_args

def test_resolve_delays_reads_platform_from_the_raw_child_stop_id():
    # The platform lives on the *child* row the feed points at, not the parent
    # station the delay is attributed to - so the lookup must use the raw id.
    cur = _FakeCursor((7, "Aachen, Hbf", 50.7678, 6.0913, "RE1", 99, None, None, 4))
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "424107", "delay_minutes": 3.0,
             "start_date": "20260801", "cancellation_kind": None}]
    resolved = resolve_delays(cur, rows, parent_map={"424107": "437028"},
                              platform_map={"424107": "5"})
    assert resolved[0]["platform"] == "5"

def test_resolve_delays_marks_a_cancelled_row_without_a_delay_or_position():
    cur = _FakeCursor((7, "Aachen, Hbf", 50.7678, 6.0913, "RE1", 99, "10:00:00", None, 4))
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "424107", "delay_minutes": None,
             "start_date": "20260801", "cancellation_kind": "trip"}]
    resolved = resolve_delays(cur, rows, parent_map={"424107": "437028"}, platform_map={})
    assert len(resolved) == 1
    assert resolved[0]["is_cancelled"] is True
    assert resolved[0]["cancellation_kind"] == "trip"
    assert resolved[0]["delay_minutes"] is None
    assert resolved[0]["lat"] is None
    assert resolved[0]["position_source"] is None

def test_resolve_delays_skips_stop_id_with_no_parent_in_map():
    cur = _FakeCursor((7, "Aachen, Hbf", 50.7678, 6.0913, "RE1", 99, None, None, 4))
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "unknown-stop", "delay_minutes": 3.0, "start_date": "20260801", "cancellation_kind": None}]
    assert resolve_delays(cur, rows, parent_map={}, platform_map={}) == []

def test_resolve_delays_skips_unmatched_trips():
    cur = _FakeCursor(None)
    parent_map = {"424107": "437028"}
    rows = [{"gtfs_trip_id": "unknown", "gtfs_stop_id": "424107", "delay_minutes": 3.0, "start_date": "20260801", "cancellation_kind": None}]
    assert resolve_delays(cur, rows, parent_map=parent_map, platform_map={}) == []

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
        (7, "Aachen, Hbf", 50.7678, 6.0913, "RE1", 99, "10:00:00", None, 4),
        (50.9429, 6.9578, "09:00:00", None),  # previous stop: Köln Hbf
    ])
    parent_map = {"424107": "437028"}
    rows = [{"gtfs_trip_id": "T1", "gtfs_stop_id": "424107", "delay_minutes": 3.0, "start_date": "20260801", "cancellation_kind": None}]
    resolved = resolve_delays(cur, rows, parent_map=parent_map, platform_map={}, now_epoch=now)
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

def test_parse_de_vehicles_rejects_an_implausible_delay():
    # gtfs.de occasionally publishes a nonsense figure - a real poll produced
    # "-1299.0 minutes" (21 hours early). Averaging that into a route's
    # reliability corrupts every statistic built on top of it, so it is
    # dropped the same way poland_poller.py drops its own implausible rows.
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    e = feed.entity.add()
    e.id = "bad"
    e.trip_update.trip.trip_id = "T_BAD"
    stu = e.trip_update.stop_time_update.add()
    stu.stop_id = "424107"
    stu.arrival.delay = -1299 * 60
    stu.arrival.time = 1785551442
    assert parse_de_vehicles(feed) == []
