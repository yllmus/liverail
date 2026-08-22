"""Unit tests for the Polish connection-scan planner.

The scan is a pure function over tuples, so all of this runs without a
database - the same split poland_poller.py uses.

Connection tuple layout (see load_connections):
  (dep_sec, arr_sec, from_station, to_station, trip_id, from_seq, to_seq,
   route_name, trip_short_name, headsign)
"""
import pl_planner as P


def conn(dep, arr, frm, to, trip, seq=0, route="R"):
    return (dep, arr, frm, to, trip, seq, seq + 1, route, None, None)


def test_hms_to_seconds_handles_past_midnight():
    # 25:10:00 is a real GTFS value for an overnight service, not corrupt data.
    assert P.hms_to_seconds("01:00:00") == 3600
    assert P.hms_to_seconds("25:10:00") == 25 * 3600 + 600
    assert P.hms_to_seconds(None) is None
    assert P.hms_to_seconds("nonsense") is None


def test_seconds_to_hm_wraps_past_midnight_for_display():
    assert P.seconds_to_hm(3600) == "01:00"
    assert P.seconds_to_hm(25 * 3600) == "01:00"


def test_scan_finds_a_direct_connection():
    conns = [conn(100, 200, 1, 2, 10)]
    chain = P.scan(conns, 1, 2, 0)
    assert chain and len(chain) == 1
    assert P.chain_to_legs(chain)[0]["trip_id"] == 10


def test_scan_returns_none_when_unreachable():
    assert P.scan([conn(100, 200, 1, 2, 10)], 1, 99, 0) is None


def test_scan_will_not_board_a_train_that_has_already_left():
    # Departure is before the requested time, so it cannot be taken.
    assert P.scan([conn(100, 200, 1, 2, 10)], 1, 2, 500) is None


def test_scan_changes_trains_when_it_must():
    conns = [conn(100, 200, 1, 2, 10), conn(300, 400, 2, 3, 20)]
    legs = P.chain_to_legs(P.scan(conns, 1, 3, 0))
    assert [l["trip_id"] for l in legs] == [10, 20]


def test_consecutive_hops_of_one_trip_collapse_into_a_single_leg():
    conns = [conn(100, 200, 1, 2, 10, seq=0), conn(210, 300, 2, 3, 10, seq=1)]
    legs = P.chain_to_legs(P.scan(conns, 1, 3, 0))
    assert len(legs) == 1
    assert legs[0]["from_station"] == 1 and legs[0]["to_station"] == 3
    assert legs[0]["depart_sec"] == 100 and legs[0]["arrive_sec"] == 300


def test_reconstruction_does_not_invent_a_change_at_a_passed_through_stop():
    # The regression this was written for. A fast train runs 1->2->3. A slower
    # one reaches 2 earlier, so station 2's best-known arrival belongs to it -
    # but a rider on the through train never gets off at 2. Reconstructing
    # station-by-station produced a chain containing a pointless hop on the
    # other train; walking back along the trip does not.
    # stop_sequence must be distinct within a trip - the real table enforces
    # it with UNIQUE (trip_id, stop_sequence), and the walk-back keys on it.
    conns = [
        conn(0,   50,  1, 2, 99, seq=0),   # slow hop, reaches 2 first
        conn(100, 200, 1, 2, 10, seq=0),   # through train, leg one
        conn(200, 300, 2, 3, 10, seq=1),   # through train, leg two
    ]
    legs = P.chain_to_legs(P.scan(conns, 1, 3, 0))
    assert len(legs) == 1, f"expected one through leg, got {legs}"
    assert legs[0]["trip_id"] == 10


def test_scan_prefers_fewer_changes_when_arrival_ties():
    # Both routes arrive at 300. One is direct; the other changes at 2.
    conns = [
        conn(100, 300, 1, 3, 10),          # direct
        conn(100, 150, 1, 2, 20),          # change route, leg one
        conn(160, 300, 2, 3, 30),          # change route, leg two
    ]
    legs = P.chain_to_legs(P.scan(conns, 1, 3, 0))
    assert len(legs) == 1 and legs[0]["trip_id"] == 10
