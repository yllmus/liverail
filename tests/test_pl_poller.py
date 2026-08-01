from google.transit import gtfs_realtime_pb2

from poland_poller import parse_pl_vehicles

def make_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"

    e1 = feed.entity.add()
    e1.id = "1"
    e1.trip_update.trip.trip_id = "T1"
    e1.trip_update.trip.route_id = "R1"
    stu1 = e1.trip_update.stop_time_update.add()
    stu1.stop_id = "S1"
    stu1.arrival.delay = 300
    stu1.departure.delay = 240

    e2 = feed.entity.add()
    e2.id = "2"
    e2.trip_update.trip.trip_id = "T2"
    e2.trip_update.trip.route_id = "R2"
    stu2 = e2.trip_update.stop_time_update.add()
    stu2.stop_id = "S2"
    stu2.departure.delay = 60

    e3 = feed.entity.add()
    e3.id = "3"
    e3.trip_update.trip.trip_id = "T3"
    e3.trip_update.trip.route_id = "R3"
    # no stop_time_update entries at all

    return feed

def test_parse_pl_vehicles_extracts_rows_with_delay():
    rows = parse_pl_vehicles(make_feed())
    assert len(rows) == 2
    assert rows[0] == {
        "gtfs_stop_id": "S1",
        "gtfs_route_id": "R1",
        "delay_minutes": 5.0,
    }

def test_parse_pl_vehicles_falls_back_to_departure_delay():
    rows = parse_pl_vehicles(make_feed())
    row = next(r for r in rows if r["gtfs_stop_id"] == "S2")
    assert row["delay_minutes"] == 1.0

def test_parse_pl_vehicles_skips_entries_with_no_stop_time_update():
    rows = parse_pl_vehicles(make_feed())
    assert all(r["gtfs_route_id"] != "R3" for r in rows)

def test_parse_pl_vehicles_empty_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    assert parse_pl_vehicles(feed) == []
