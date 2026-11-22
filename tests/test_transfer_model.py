"""Serving the promoted transfer-risk model.

The failure this guards against is not a crash - it is a model that answers
confidently with features it never trained on. So most of these assert that it
REFUSES rather than that it predicts.
"""

import pytest

import model_loop
import transfer_model as tm


@pytest.fixture(autouse=True)
def _clear_cache():
    tm._CACHE.clear()
    yield
    tm._CACHE.clear()


def test_feature_stats_round_trip(tmp_path):
    """The sidecar must return exactly what was written, including tuple keys
    rebuilt from strings - a route name containing a space or a colon must not
    corrupt the (station_id, route) key."""
    model_path = str(tmp_path / "m.txt")
    st = {12: (1.0, 2.0, 3.0, 4.0)}
    rt = {(12, "RE 1"): (5.0, 6.0, 7.0, 8.0),
          (12, "S:7 via X"): (9.0, 10.0, 11.0, 12.0)}
    counts = {12: 340}
    glob = (0.5, 1.5, 2.5, 3.5)
    model_loop.save_feature_stats(model_path, st, rt, counts, glob)
    st2, rt2, counts2, glob2, feats = model_loop.load_feature_stats(model_path)
    assert st2 == st
    assert rt2 == rt
    assert counts2 == counts
    assert tuple(glob2) == glob
    assert feats == model_loop.FEATURES


def test_champion_without_sidecar_is_refused(tmp_path, monkeypatch):
    """A model promoted before the sidecar existed cannot be served: its
    feature statistics are gone, so serving it would silently feed the booster
    numbers from a different window."""
    lonely = tmp_path / "old_model.txt"
    lonely.write_text("not really a booster")
    monkeypatch.setattr(tm, "champion_path", lambda c: str(lonely))
    assert tm._load("DE") is None


def test_missing_model_file_degrades_quietly(monkeypatch):
    monkeypatch.setattr(tm, "champion_path", lambda c: "/nonexistent/model.txt")
    assert tm._load("DE") is None
    assert tm.predict("DE", 1, "A", "B", 10, __import__("datetime").datetime(2026, 8, 23)) is None


def test_no_champion_means_no_prediction(monkeypatch):
    monkeypatch.setattr(tm, "champion_path", lambda c: None)
    assert tm.get_champion("ZZ") is None
    assert tm.status("ZZ")["model_kind"] == "mixture"


def test_unknown_station_is_refused(monkeypatch):
    """Outside its training distribution the analytic mixture is better than a
    guess, so the model must decline rather than extrapolate."""
    import datetime
    fake = {"booster": object(), "st": {5: (1, 2, 3, 4)}, "rt": {},
            "counts": {5: 10}, "glob": (1, 2, 3, 4),
            "features": model_loop.FEATURES, "path": "/tmp/x"}
    monkeypatch.setattr(tm, "get_champion", lambda c: fake)
    assert tm.predict("DE", 99999, "A", "B", 10, datetime.datetime(2026, 8, 23)) is None


def test_feature_vector_matches_training_layout(monkeypatch):
    """predict() builds its row by hand; build_features builds the training
    matrix. If the two ever disagree on order or length the model is silently
    wrong, so the row is compared against build_features on identical inputs."""
    import datetime
    import numpy as np
    st_stats = {5: (1.0, 2.0, 3.0, 4.0)}
    rt_stats = {(5, "F"): (10.0, 11.0, 12.0, 13.0), (5, "C"): (20.0, 21.0, 22.0, 23.0)}
    counts = {5: 77}
    glob = (0.0, 0.0, 0.0, 0.0)
    when = datetime.datetime(2026, 8, 23, 14, 0)   # a Sunday
    expected = model_loop.build_features(
        [{"station_id": 5, "feeder_route": "F", "conn_route": "C",
          "buffer_min": 9.0, "hour": when.hour, "day": when.date()}],
        st_stats, rt_stats, glob, counts)[0]

    captured = {}

    class FakeBooster:
        def predict(self, X):
            captured["row"] = X[0]
            return np.asarray([0.5])

    monkeypatch.setattr(tm, "get_champion", lambda c: {
        "booster": FakeBooster(), "st": st_stats, "rt": rt_stats,
        "counts": counts, "glob": glob, "features": model_loop.FEATURES,
        "path": "/tmp/x"})
    p = tm.predict("DE", 5, "F", "C", 9.0, when)
    assert p == 0.5
    assert list(captured["row"]) == pytest.approx(list(expected))


def test_probability_is_capped_and_bounded(monkeypatch):
    import datetime
    import numpy as np

    class Extreme:
        def __init__(self, v): self.v = v
        def predict(self, X): return np.asarray([self.v])

    def champ(v):
        return {"booster": Extreme(v), "st": {5: (1.0, 2.0, 3.0, 4.0)}, "rt": {},
                "counts": {5: 10}, "glob": (0, 0, 0, 0),
                "features": model_loop.FEATURES, "path": "/tmp/x"}
    when = datetime.datetime(2026, 8, 23, 9, 0)
    monkeypatch.setattr(tm, "get_champion", lambda c: champ(1.0))
    assert tm.predict("DE", 5, "A", "B", 30, when) == 0.95   # never certainty
    monkeypatch.setattr(tm, "get_champion", lambda c: champ(-0.2))
    assert tm.predict("DE", 5, "A", "B", 1, when) == 0.0
    monkeypatch.setattr(tm, "get_champion", lambda c: champ(float("nan")))
    assert tm.predict("DE", 5, "A", "B", 10, when) is None
