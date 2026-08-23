"""The alert decision logic, which is where the spam risk lives.

A watched train is checked 288 times in 24 hours. These tests exist to pin
down that a single delay produces ONE notification, not 288.
"""
import pytest

from push_alerts import decide, compose, DELAY_THRESHOLD, WORSEN_STEP

FOUND = lambda **kw: {"found": True, "cancelled": False, "delay": None, **kw}


def test_on_time_train_never_notifies():
    assert decide(FOUND(delay=0.0), None) is None
    assert decide(FOUND(delay=DELAY_THRESHOLD - 0.1), None) is None


def test_first_crossing_notifies_once():
    first = decide(FOUND(delay=12.0), None)
    assert first == ("delayed", 12.0)
    # The same fact, five minutes later. This is the spam case.
    assert decide(FOUND(delay=12.0), {"kind": "delayed", "delay": 12.0}) is None


def test_small_worsening_stays_quiet():
    last = {"kind": "delayed", "delay": 12.0}
    assert decide(FOUND(delay=12.0 + WORSEN_STEP - 1), last) is None


def test_material_worsening_notifies():
    last = {"kind": "delayed", "delay": 12.0}
    assert decide(FOUND(delay=12.0 + WORSEN_STEP), last) == ("worsened", 22.0)


def test_recovery_notifies_only_if_we_had_said_late():
    late = {"kind": "delayed", "delay": 20.0}
    assert decide(FOUND(delay=1.0), late) == ("recovered", 1.0)
    # Never told them it was late -> saying "it is fine" is unprompted noise.
    assert decide(FOUND(delay=1.0), None) is None
    # And it does not repeat.
    assert decide(FOUND(delay=1.0), {"kind": "recovered", "delay": 1.0}) is None


def test_delay_can_fire_again_after_a_recovery():
    """A train that recovers and then goes late again is a new fact."""
    assert decide(FOUND(delay=15.0), {"kind": "recovered", "delay": 1.0}) == ("delayed", 15.0)


def test_cancellation_outranks_delay_and_sends_once():
    cur = {"found": True, "cancelled": True, "delay": None}
    assert decide(cur, None) == ("cancelled", None)
    assert decide(cur, {"kind": "cancelled", "delay": None}) is None
    # Cancelled while we had previously reported a delay: still worth saying.
    assert decide(cur, {"kind": "delayed", "delay": 30.0}) == ("cancelled", None)


def test_null_delay_is_not_treated_as_on_time():
    """MÁV reports no delay at a trip's final stop even for a late train.
    Unknown must not become 'recovered' - that would tell a rider on a
    52-minute-late train that it is fine."""
    assert decide(FOUND(delay=None), {"kind": "delayed", "delay": 52.0}) is None


def test_train_not_in_feed_is_silent():
    assert decide({"found": False}, None) is None
    assert decide({"found": False}, {"kind": "delayed", "delay": 20.0}) is None


@pytest.mark.parametrize("kind,delay,expect_in_title", [
    ("delayed", 14.0, "running late"),
    ("worsened", 40.0, "running later"),
    ("recovered", 1.0, "back on time"),
    ("cancelled", None, "cancelled"),
])
def test_compose_says_something_a_person_can_act_on(kind, delay, expect_in_title):
    title, body = compose(kind, delay, "RE 5", "Hamburg Hbf", "DE")
    assert expect_in_title in title
    assert "RE 5" in title
    assert "Hamburg Hbf" in body
    if delay is not None and kind != "cancelled":
        assert str(int(delay)) in body


def test_compose_survives_a_missing_label():
    title, _ = compose("delayed", 9.0, None, None, "PL")
    assert title.startswith("Your train")
