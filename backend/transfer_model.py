"""Serve the promoted transfer-risk model.

model_loop.py trains a challenger, scores it against the incumbent on held-out
data, and promotes it only if it wins. Until this module existed, that was the
whole story: the winner was written to model_eval and to disk, and NOTHING READ
IT BACK. /transfer/risk kept answering from the analytic mixture, so a measured
improvement - DE's champion beat the mixture 0.1423 to 0.1527 on tight
transfers, the only case a rider actually asks about - reached nobody.

TRAIN/SERVE SKEW IS THE FAILURE MODE THIS IS BUILT AROUND. Nine of the twelve
features are delay statistics fitted on the training window. Recomputing them
at request time from a different window, or from delay_distribution (whose
percentiles are built over a different span and a different row filter), would
feed the booster numbers it never saw during training. Predictions would stay
plausible-looking while being wrong, which is the hardest kind of bug to
notice. So model_loop writes the exact statistics it trained with to a sidecar,
and this reads them back verbatim. The model and its features travel together
or not at all.

WHAT IT REFUSES TO DO. If there is no promoted champion for a country, if the
sidecar is missing, if lightgbm is not installed, or if the station was never
seen in training, this returns None and the caller falls back to the mixture.
A model that guesses outside its training distribution is worse than the
analytic fallback, which at least degrades honestly.
"""
import math
import os
import threading
import time

import psycopg2

from config import DB_CONFIG

TASK = "transfer_risk"
# How often to look for a newly promoted champion. The loop retrains weekly, so
# checking every 30 minutes picks a promotion up promptly without hammering
# model_eval on every request.
REFRESH_SECONDS = 1800

_CACHE = {}          # country -> dict(booster, stats, path, loaded_at) or None
_LOCK = threading.Lock()


def _connect():
    return psycopg2.connect(**DB_CONFIG)


def champion_path(country):
    """The most recently promoted model for this country, or None."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT model_path FROM model_eval
            WHERE country_code = %s AND task = %s AND promoted AND model_path IS NOT NULL
            ORDER BY trained_at DESC LIMIT 1
        """, (country, TASK))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()


def _load(country):
    """Returns the loaded champion, or None. Never raises - a missing or
    unreadable model must degrade to the mixture, not 500 a journey plan."""
    try:
        import lightgbm as lgb
    except Exception:
        return None
    path = champion_path(country)
    if not path or not os.path.exists(path):
        return None
    try:
        import model_loop
        if not os.path.exists(model_loop.stats_path(path)):
            # A champion promoted before the sidecar existed. Unservable by
            # design rather than servable-but-skewed.
            return None
        st, rt, counts, glob, features = model_loop.load_feature_stats(path)
        booster = lgb.Booster(model_file=path)
        return {"booster": booster, "st": st, "rt": rt, "counts": counts,
                "glob": glob, "features": features, "path": path}
    except Exception:
        return None


def get_champion(country):
    now = time.time()
    hit = _CACHE.get(country)
    if hit is not None and now - hit["checked_at"] < REFRESH_SECONDS:
        return hit["model"]
    with _LOCK:
        hit = _CACHE.get(country)
        if hit is not None and now - hit["checked_at"] < REFRESH_SECONDS:
            return hit["model"]
        model = _load(country)
        _CACHE[country] = {"model": model, "checked_at": now}
        return model


def predict(country, station_id, feeder_route, conn_route, buffer_min, when):
    """P(the connection is made), or None if the champion cannot answer.

    Feature order and construction mirror model_loop.build_features exactly.
    If the two ever drift the model is silently wrong, so they are asserted
    against the feature list stored in the sidecar.
    """
    m = get_champion(country)
    if m is None:
        return None
    st = m["st"].get(station_id)
    if st is None:
        # Never seen this station in training. The mixture handles it honestly.
        return None
    f = m["rt"].get((station_id, feeder_route), st)
    c = m["rt"].get((station_id, conn_route), st)
    row = [float(buffer_min), float(when.hour), float(when.weekday()),
           st[0], st[1], st[2], st[3], f[0], f[1], c[0], c[1],
           float(m["counts"].get(station_id, 0))]
    if len(row) != len(m["features"]):
        return None
    try:
        import numpy as np
        p = m["booster"].predict(np.asarray([row], dtype=np.float32))[0]
    except Exception:
        return None
    if math.isnan(p):
        return None
    # Same ceiling the mixture is held to. The curve is fitted to p95 and the
    # tail above it is unmodelled, so claiming more than 0.95 certainty would
    # overstate what three weeks of history can support.
    return max(0.0, min(0.95, float(p)))


def status(country):
    """What is serving this country, for the API to report honestly."""
    m = get_champion(country)
    if m is None:
        return {"model_kind": "mixture", "model_path": None}
    return {"model_kind": "lgbm", "model_path": os.path.basename(m["path"]),
            "stations_known": len(m["st"])}
