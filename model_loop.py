"""Retrain, score against the incumbent, promote only on a win.

This is the part that makes "gets better as data arrives" a fact rather than a
hope. It is deliberately NOT online learning. Rows arrive every five minutes;
the patterns underneath move on the scale of timetable periods. Updating
weights per row would be brittle, near-impossible to validate, and would buy
nothing that a weekly retrain does not.

What it is instead is champion/challenger:

    rebuild the labelled dataset from a rolling window
    train a challenger on the older part
    score challenger AND incumbent on the SAME held-out later part
    write both results to model_eval
    promote the challenger only if it wins by more than noise

The gate is the whole point. The transfer probability shipped unverified for
weeks and was systematically pessimistic in every reliability bin, and nothing
caught it because nothing was scoring it. A loop that retrains automatically
has that same failure mode with a shorter fuse - it can degrade continuously
and silently. Every promotion here has to be earned against a measurement.

Three rules the gate enforces, each from something already observed:

  1. Beat the incumbent overall by a real margin, not a rounding error.
  2. Do not get worse on TIGHT transfers. The overall score is dominated by
     20-60 minute buffers where nearly everything succeeds; a model can
     improve on average while getting worse at the only case a rider asks
     about.
  3. Beat the base rate. A model that cannot beat "always guess the average"
     is not a model, and the base rate here is 0.92 - a high bar that looks
     deceptively easy to clear.

The analytic mixture is never deleted. It is the permanent fallback for any
station too thin to fit on, and the incumbent a challenger must beat on day
one.
"""
import argparse
import os
import time
from collections import defaultdict

import numpy as np
import psycopg2

import transfer_backtest as tb
from config import DB_PASSWORD

MODEL_DIR = os.path.expanduser("~/models")
TASK = "transfer_risk"

# Promotion thresholds. A challenger must clear all of them.
MIN_IMPROVEMENT = 0.0015      # absolute Brier, ~0.5% of a typical 0.03 score
TIGHT_TOLERANCE = 0.002       # how much worse it may be on tight buffers
TIGHT_MAX_BUFFER = 10.0       # minutes; "tight" means a change under this
MIN_TEST_ROWS = 2000


def connect():
    return psycopg2.connect(dbname="train_delay_db", user="trainuser",
                            password=DB_PASSWORD, host="localhost")


def build_features(rows, st_stats, rt_stats, glob, st_counts):
    X = []
    for r in rows:
        s = st_stats.get(r["station_id"], glob)
        f = rt_stats.get((r["station_id"], r["feeder_route"]), s)
        c = rt_stats.get((r["station_id"], r["conn_route"]), s)
        X.append([r["buffer_min"], r["hour"], r["day"].weekday(),
                  s[0], s[1], s[2], s[3], f[0], f[1], c[0], c[1],
                  st_counts.get(r["station_id"], 0)])
    return np.asarray(X, dtype=np.float32)


FEATURES = ["buffer", "hour", "dow", "st_p50", "st_p90", "st_p99", "st_mean",
            "f_p50", "f_p90", "c_p50", "c_p90", "st_n"]


def _stats(vals):
    if not vals:
        return (np.nan,) * 4
    a = np.asarray(vals)
    return (float(np.median(a)), float(np.quantile(a, .9)),
            float(np.quantile(a, .99)), float(a.mean()))


def calibration_error(pred, label, bins=10):
    """Mean absolute gap between predicted and observed, weighted by bin size.

    Reported alongside Brier because they answer different questions: Brier can
    look fine while a model is confidently wrong in one band, and it was
    exactly a calibration gap - pessimism in every bin - that the shipped
    model was hiding.
    """
    p, y = np.asarray(pred), np.asarray(label)
    total, n = 0.0, 0
    for i in range(bins):
        lo, hi = i / bins, (i + 1) / bins
        m = (p >= lo) & (p < hi if i < bins - 1 else p <= hi)
        if m.sum() == 0:
            continue
        total += abs(p[m].mean() - y[m].mean()) * m.sum()
        n += m.sum()
    return float(total / n) if n else float("nan")


def run_cycle(country="DE", days=21, limit=400000, permille=60, mct=5.0,
              dry_run=False, seed=0):
    """One retrain-and-judge cycle. Returns the decision as a dict."""
    import lightgbm as lgb

    conn = connect()
    cur = conn.cursor()
    try:
        rows = tb.load_transfers(cur, country, days, limit, sample_permille=permille)
    finally:
        cur.close()
        conn.close()
    if len(rows) < MIN_TEST_ROWS * 2:
        return {"promoted": False, "decision": f"only {len(rows)} transfers, too few to judge"}

    day_list = sorted({r["day"] for r in rows})
    if len(day_list) < 4:
        return {"promoted": False, "decision": f"only {len(day_list)} distinct days"}
    cut = day_list[int(len(day_list) * 0.7)]

    # Split by DATE and hold out whole STATIONS. Both guards matter: a random
    # split lets a model see the day it is scored on, and a model that only
    # works where it trained is a lookup table.
    rng = np.random.default_rng(seed)
    stations = sorted({r["station_id"] for r in rows})
    unseen = set(rng.permutation(stations)[: max(1, len(stations) // 5)].tolist())
    train = [r for r in rows if r["day"] < cut and r["station_id"] not in unseen]
    test = [r for r in rows if r["day"] >= cut]
    if len(test) < MIN_TEST_ROWS:
        return {"promoted": False, "decision": f"only {len(test)} test rows"}

    # Everything below is fitted on TRAIN ONLY.
    st_d, rt_d = defaultdict(list), defaultdict(list)
    for r in train:
        st_d[r["station_id"]] += [r["feeder_delay"], r["conn_delay"]]
        rt_d[(r["station_id"], r["feeder_route"])].append(r["feeder_delay"])
        rt_d[(r["station_id"], r["conn_route"])].append(r["conn_delay"])
    q_st = {k: tb.empirical_quantiles(v) for k, v in st_d.items()}
    q_rt = {k: tb.empirical_quantiles(v) for k, v in rt_d.items()}
    m_st = {k: tb.fit_lognormal_mixture(v) for k, v in st_d.items()
            if len(v) >= tb.MIN_SAMPLES_FOR_FIT}
    m_rt = {k: tb.fit_lognormal_mixture(v) for k, v in rt_d.items()
            if len(v) >= tb.MIN_SAMPLES_FOR_FIT}
    st_stats = {k: _stats(v) for k, v in st_d.items()}
    rt_stats = {k: _stats(v) for k, v in rt_d.items() if len(v) >= 10}
    st_counts = {k: len(v) for k, v in st_d.items()}
    glob = _stats([d for v in st_d.values() for d in v])

    ytr = np.asarray([tb.label(r, mct) for r in train], dtype=np.int8)
    Xtr = build_features(train, st_stats, rt_stats, glob, st_counts)
    booster = lgb.train(
        {"objective": "binary", "learning_rate": 0.05, "num_leaves": 31,
         "min_data_in_leaf": 200, "feature_fraction": 0.9, "bagging_fraction": 0.8,
         "bagging_freq": 1, "num_threads": 2, "verbose": -1, "seed": seed},
        lgb.Dataset(Xtr, label=ytr, feature_name=FEATURES), num_boost_round=300)

    # Score both on identical rows - only those the incumbent can also answer,
    # or the comparison flatters whichever model answers more often.
    rng2 = np.random.default_rng(seed + 1)
    keep, inc, buffers, feats = [], [], [], []
    for r in test:
        fm = m_rt.get((r["station_id"], r["feeder_route"])) or m_st.get(r["station_id"])
        cm = m_rt.get((r["station_id"], r["conn_route"])) or m_st.get(r["station_id"])
        if fm is None:
            continue
        p = tb.predict_mixture(fm, cm, r["buffer_min"], mct, rng2)
        if p is None:
            continue
        inc.append(p)
        keep.append(tb.label(r, mct))
        buffers.append(r["buffer_min"])
        feats.append(r)
    if len(keep) < MIN_TEST_ROWS:
        return {"promoted": False, "decision": f"only {len(keep)} comparable rows"}

    y = np.asarray(keep)
    chal = booster.predict(build_features(feats, st_stats, rt_stats, glob, st_counts))
    buf = np.asarray(buffers)
    tight = buf < TIGHT_MAX_BUFFER
    base = float(ytr.mean())

    res = {
        "country": country, "task": TASK,
        "train_from": min(r["day"] for r in train), "train_to": max(r["day"] for r in train),
        "test_from": min(r["day"] for r in feats), "test_to": max(r["day"] for r in feats),
        "n_train": len(train), "n_test": len(y), "base_rate": base,
        "inc_brier": tb.brier(inc, y), "chal_brier": tb.brier(chal, y),
        "inc_tight": tb.brier(np.asarray(inc)[tight], y[tight]) if tight.sum() else None,
        "chal_tight": tb.brier(chal[tight], y[tight]) if tight.sum() else None,
        "inc_cal": calibration_error(inc, y), "chal_cal": calibration_error(chal, y),
        "base_brier": tb.brier(np.full(len(y), base), y),
    }

    # --- the gate ---------------------------------------------------------
    gain = res["inc_brier"] - res["chal_brier"]
    reasons = []
    if gain < MIN_IMPROVEMENT:
        reasons.append(f"gain {gain:+.4f} below {MIN_IMPROVEMENT} threshold")
    if res["chal_brier"] >= res["base_brier"]:
        reasons.append(f"loses to the base rate ({res['chal_brier']:.4f} vs {res['base_brier']:.4f})")
    if res["inc_tight"] is not None and res["chal_tight"] is not None:
        if res["chal_tight"] > res["inc_tight"] + TIGHT_TOLERANCE:
            reasons.append(f"worse on tight transfers ({res['chal_tight']:.4f} vs {res['inc_tight']:.4f})")
    promote = not reasons
    res["promoted"] = promote
    res["decision"] = ("promoted: Brier %+.4f, tight %+.4f"
                       % (gain, (res["inc_tight"] or 0) - (res["chal_tight"] or 0))
                       ) if promote else "rejected: " + "; ".join(reasons)

    path = None
    if promote and not dry_run:
        os.makedirs(MODEL_DIR, exist_ok=True)
        path = os.path.join(MODEL_DIR, f"{TASK}_{country}_{int(time.time())}.txt")
        booster.save_model(path)
    res["model_path"] = path

    if not dry_run:
        conn = connect()
        cur = conn.cursor()
        try:
            for kind, b, t, c in (("mixture", res["inc_brier"], res["inc_tight"], res["inc_cal"]),
                                  ("lgbm", res["chal_brier"], res["chal_tight"], res["chal_cal"])):
                cur.execute("""
                    INSERT INTO model_eval
                      (country_code, task, model_kind, train_from, train_to,
                       test_from, test_to, n_train, n_test, brier, brier_tight,
                       base_rate, calibration_err, promoted, decision, model_path)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (country, TASK, kind, res["train_from"], res["train_to"],
                      res["test_from"], res["test_to"], res["n_train"], res["n_test"],
                      b, t, base, c,
                      promote and kind == "lgbm", res["decision"],
                      path if kind == "lgbm" else None))
            conn.commit()
        finally:
            cur.close()
            conn.close()
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--country", default="DE")
    ap.add_argument("--days", type=int, default=21)
    ap.add_argument("--permille", type=int, default=60)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    r = run_cycle(a.country, a.days, permille=a.permille, dry_run=a.dry_run)
    if "inc_brier" not in r:
        print(f"{a.country}: {r['decision']}")
        return
    print(f"=== {r['country']} / {r['task']} ===")
    print(f"train {r['train_from']}..{r['train_to']} ({r['n_train']})   "
          f"test {r['test_from']}..{r['test_to']} ({r['n_test']})")
    print(f"{'':12s} {'Brier':>9s} {'tight':>9s} {'calib err':>10s}")
    print(f"{'base rate':12s} {r['base_brier']:9.4f}")
    print(f"{'incumbent':12s} {r['inc_brier']:9.4f} {r['inc_tight'] or float('nan'):9.4f} {r['inc_cal']:10.4f}")
    print(f"{'challenger':12s} {r['chal_brier']:9.4f} {r['chal_tight'] or float('nan'):9.4f} {r['chal_cal']:10.4f}")
    print(f"\n{r['decision']}")
    if r.get("model_path"):
        print(f"saved: {r['model_path']}")


if __name__ == "__main__":
    main()
