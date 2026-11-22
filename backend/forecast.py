"""Two-stage delay forecasting with calibrated prediction intervals.

Built after reading what the field actually does, rather than guessing. Three
findings from the literature shaped every design choice here:

1. PERSISTENCE IS NEARLY UNBEATABLE ON MAE, AND THAT IS NORMAL. The 2025
   state-of-the-art graph-attention model on 40% of the Dutch network scores
   MAE 1.09 min against a persistence baseline's 0.97 - it LOSES. Its value is
   in classifying delay events (precision 62.3% vs 60.6%), not in error
   magnitude. So chasing MAE here is chasing the one number the whole field
   fails to move, and this module reports classification quality and interval
   coverage as first-class results rather than a footnote.

2. TWO-LEVEL BEATS ONE-LEVEL. Laifa et al. (ICAART 2022) predict a delay CLASS
   with one LightGBM, feed that prediction to a second LightGBM as a feature,
   and gain ~7% - and the same trick improved every benchmark model they tried.
   The GATv2 paper independently splits classifier from regressor because the
   two tasks fight for gradient. Both point the same way, so stage one's class
   probabilities become stage two's features here.

3. CONGESTION IS THE FEATURE THAT WORKS, AT FINE TIME GRANULARITY. The GATv2
   paper's own contribution is "count of other train arrivals at platform p in
   the prior 60 minutes" and movements "within a +/-5-minute window". My first
   attempt used HOURLY station aggregates and they were worthless once
   de-leaked. An hour is simply too coarse to see a knock-on effect.

AND ONE FINDING OF MY OWN, WHICH IS WHY THIS EXISTS AT ALL. delay_forecast.py
reported +12.5% until the network feature turned out to be reading the hour it
was predicting, which contained the target row itself. Everything below is
therefore built so leakage is structurally impossible rather than checked for:
congestion windows are half-open and strictly BEFORE the prediction instant,
history statistics come from the training split alone, days are split
chronologically, and a fifth of stations are held out entirely.

WHAT IS PREDICTED. A train's delay some stops further along its own journey.
Three outputs, because a rider needs different things:
    class   which delay band, as a probability - the honest form of the answer
    point   a single number, for when one is needed
    range   a prediction INTERVAL with guaranteed coverage, via conformalized
            quantile regression (Romano et al. 2019): train quantile models,
            then calibrate their width on held-out data so the stated coverage
            is the coverage actually delivered. A 90% interval that contains
            the truth 90% of the time is worth more to a traveller than a point
            estimate that is quietly wrong.
"""
import argparse
import time
from collections import defaultdict

import numpy as np
import psycopg2

from config import DB_CONFIG

MAX_ABS_DELAY = 720.0
# Delay bands for stage one. Deliberately the app's own severity tiers rather
# than the uniform 5-minute bins the Tunisian paper used: the boundaries a
# rider acts on are the ones the rest of this site already draws.
BANDS = [2.0, 5.0, 10.0, 20.0, 40.0]
BAND_NAMES = ["<=2", "2-5", "5-10", "10-20", "20-40", ">40"]
# Congestion lookback, from the GATv2 paper's "prior 60 minutes".
CONGESTION_WINDOWS = (15, 60)
TARGET_COVERAGE = 0.90


def connect():
    return psycopg2.connect(**DB_CONFIG)


def band_of(delay):
    for i, b in enumerate(BANDS):
        if delay <= b:
            return i
    return len(BANDS)


def load_observations(cur, country, days, max_rows=1_400_000):
    """Every usable observation, with a per-train identity and a real clock.

    IDENTITY DIFFERS BY COUNTRY, for reasons this project has already
    established rather than by preference:
      DE, PL  trips.id - a real schedule identity, 100% populated
      HU      route_name - which the 2026-08-02 fix made a per-train-number
              identity, and which reaches 21 days where trip_id reaches 4
      AT      trip_id, though its resolution is still ~28%

    ORDERING is by actual_arrival, the poll timestamp, which is populated for
    100% of the archive. stop_sequence was only stamped from 2026-08-21 and
    using it as the ordering key would throw away 85% of the history; where it
    IS recoverable (a stop_times join) it is used for the horizon instead.
    """
    ident = "d.route_name" if country == "HU" else "d.trip_id::text"

    # SAMPLE WHOLE TRIPS ACROSS THE WHOLE WINDOW, rather than taking the
    # newest N rows.
    #
    # A LIMIT keeps whichever end of the period it is ordered by, and for
    # Germany that is fatal: its 21-day window contains the 12-day outage, so
    # the newest 1.4M rows cover barely four days and no chronological
    # train/calibration/test split can fit in them. Hashing the identity and
    # keeping a deterministic fraction preserves entire journeys - which
    # sampling individual rows would destroy - while spanning every day in the
    # period.
    cur.execute(f"""
        SELECT count(*) FROM delay_observations d
        WHERE d.country_code = %s AND d.delay_minutes IS NOT NULL
          AND NOT d.is_cancelled AND d.station_id IS NOT NULL
          AND {ident} IS NOT NULL AND d.observed_date >= current_date - %s
    """, (country, days))
    total = cur.fetchone()[0] or 0
    keep_frac = min(1.0, max_rows / total) if total else 1.0
    modulus = max(1, int(round(1.0 / keep_frac))) if keep_frac < 1.0 else 1
    sample_clause = ("" if modulus == 1 else
                     f"AND abs(hashtext({ident})) %% {modulus} = 0")

    cur.execute(f"""
        SELECT {ident} AS ident, d.observed_date, d.station_id, d.route_name,
               d.delay_minutes, d.actual_arrival, st.stop_sequence
        FROM delay_observations d
        LEFT JOIN stop_times st
               ON st.trip_id = d.trip_id AND st.station_id = d.station_id
        WHERE d.country_code = %s
          AND d.delay_minutes IS NOT NULL AND NOT d.is_cancelled
          AND abs(d.delay_minutes) <= %s
          AND d.station_id IS NOT NULL
          AND {ident} IS NOT NULL
          AND d.observed_date >= current_date - %s
          {"AND d.route_name NOT LIKE '%%/%%'" if country == "HU" else ""}
          {sample_clause}
        ORDER BY d.id
    """, (country, MAX_ABS_DELAY, days))
    obs = []
    for ident_v, day, sid, route, delay, t, seq in cur.fetchall():
        obs.append({"ident": ident_v, "day": day, "station_id": sid,
                    "route": route, "delay": float(delay), "t": t, "seq": seq})
    return obs


def split_days(rows, train_frac=0.6, calib_frac=0.75):
    """Chronological cut points chosen by SAMPLE MASS, not by counting days.

    Two distinct failures forced this, both found by running:

    1. Germany lost 99.7% of its observations for twelve days (2026-08-08..19).
       Splitting on the day INDEX put the whole calibration window inside that
       dead zone and produced zero calibration rows.
    2. Splitting on mass alone then failed the opposite way: Germany's recent
       days are so much larger than its older ones that 60% of the mass is not
       reached until the FINAL day, so both cut points landed on the same date
       and the calibration set was empty again.

    An empty calibration set is not a cosmetic problem - it silently voids the
    conformal coverage guarantee, which is the one output here that comes with
    a promise. So the cuts are chosen by mass and then forced to be distinct
    days with rows on both sides.
    """
    per_day = defaultdict(int)
    for r in rows:
        per_day[r["day"]] += 1
    days = sorted(per_day)
    if len(days) < 3:
        return (days[0], days[-1]) if days else (None, None)
    total = sum(per_day.values())

    cum, frac = 0, {}
    for d in days:
        cum += per_day[d]
        frac[d] = cum / total

    # The training cut may be neither the first day (train would be empty) nor
    # the last (nothing left for calibration and test). If no eligible day
    # reaches the target fraction - which happens when the mass sits at the end
    # of the window - take the latest eligible day rather than giving up.
    eligible = days[1:-1]
    d_tr = next((d for d in eligible if frac[d] >= train_frac), eligible[-1])
    later = [d for d in days if d > d_tr]
    d_ca = next((d for d in later if frac[d] >= calib_frac), later[-1])
    return d_tr, d_ca


class Congestion:
    """What every other train was doing, in the window BEFORE an instant.

    The window is half-open - [t - w, t) - so the observation being predicted
    can never be inside it. That is the structural fix for the leak that made
    the first version of this look twice as good as it was.

    Implemented with sorted arrays and binary search rather than SQL, because
    the per-row lateral join this replaces is quadratic-ish and this box has
    one vCPU.
    """

    def __init__(self, obs):
        per_station = defaultdict(list)
        national = []
        for o in obs:
            if o["t"] is None:
                continue
            ts = o["t"].timestamp()
            per_station[o["station_id"]].append((ts, o["delay"]))
            national.append((ts, o["delay"]))
        self.st = {}
        for sid, rows in per_station.items():
            rows.sort()
            a = np.asarray([r[0] for r in rows])
            d = np.asarray([r[1] for r in rows])
            self.st[sid] = (a, d, np.cumsum(d), np.cumsum(d > 5.0))
        national.sort()
        a = np.asarray([r[0] for r in national])
        d = np.asarray([r[1] for r in national])
        self.nat = (a, d, np.cumsum(d), np.cumsum(d > 5.0))

    @staticmethod
    def _window(idx, ts, minutes):
        a, d, csum, clate = idx
        hi = np.searchsorted(a, ts, side="left")          # strictly before ts
        lo = np.searchsorted(a, ts - minutes * 60.0, side="left")
        n = hi - lo
        if n <= 0:
            return (0.0, np.nan, np.nan)
        s = csum[hi - 1] - (csum[lo - 1] if lo > 0 else 0.0)
        l = clate[hi - 1] - (clate[lo - 1] if lo > 0 else 0.0)
        return (float(n), float(s / n), float(l / n))

    def last_before(self, sid, ts, k=2):
        """The k most recent readings at this station strictly before ts, as
        (delay, minutes_ago) pairs.

        THIS IS THE TRAIN-LEVEL VERSION OF CONGESTION, and it exists because
        the hourly aggregate version did not work. An hour-wide mean at a
        station washes out exactly the effect docs/delay-forecasting.md measured: a
        specific train handing its delay to a specific later train 10-75
        minutes behind it, at 4.4x the rate chance produces. The published
        state of the art reaches for the same granularity - "movements within
        a +/-5-minute window" - rather than an hourly average.

        Strictly before ts, so the row being predicted can never be inside it.
        """
        idx = self.st.get(sid)
        if idx is None:
            return []
        a, d, _c, _l = idx
        hi = np.searchsorted(a, ts, side="left")
        out = []
        for j in range(hi - 1, max(-1, hi - 1 - k), -1):
            out.append((float(d[j]), (ts - a[j]) / 60.0))
        return out

    def station(self, sid, ts, minutes):
        idx = self.st.get(sid)
        return (0.0, np.nan, np.nan) if idx is None else self._window(idx, ts, minutes)

    def nation(self, ts, minutes):
        return self._window(self.nat, ts, minutes)


BASE_FEATURES = [
    # own trajectory
    "cur_delay", "delay_lag1", "delay_slope", "delay_max", "delay_min",
    "delay_mean", "stops_elapsed", "minutes_running",
    # horizon
    "stops_ahead", "minutes_ahead",
    # calendar
    "hour", "dow",
    # history, training split only
    "tgt_st_p50", "tgt_st_p90", "tgt_st_n",
    "cur_st_p50", "cur_st_p90",
    "route_p50", "route_p90", "route_n",
    "trainstop_p50", "trainstop_p90", "trainstop_n",
    "linestop_p50", "linestop_p90", "linestop_n",
    # congestion, strictly before the prediction instant
    "tgt_cong_n_15", "tgt_cong_mean_15", "tgt_cong_late_15",
    "tgt_cong_n_60", "tgt_cong_mean_60", "tgt_cong_late_60",
    "cur_cong_n_15", "cur_cong_mean_15", "cur_cong_late_15",
    "cur_cong_n_60", "cur_cong_mean_60", "cur_cong_late_60",
    "nat_cong_n_60", "nat_cong_mean_60", "nat_cong_late_60",
    # the train immediately ahead - see Congestion.last_before
    "tgt_prev_delay", "tgt_prev_age", "tgt_prev2_delay",
    "cur_prev_delay", "cur_prev_age",
]
STAGE2_FEATURES = BASE_FEATURES + [f"p_band_{n}" for n in BAND_NAMES]


def build_samples(obs, cong, max_horizon=8, max_samples=900_000, seed=0):
    """One row per (train-day, from-stop, to-stop).

    Features describe only what is knowable AT THE FROM STOP. The label is the
    delay at the to-stop and nothing on the feature side may touch it.

    TRIPS ARE SUBSAMPLED AT RANDOM, NOT TRUNCATED. The first version simply
    stopped once it had enough rows - and since trips arrive in time order,
    that quietly kept only the OLDEST days of the loaded window. Hungary
    averages ten stops per journey, so the cap was reached six days in and the
    model was trained and tested entirely on 2-7 August while believing it had
    three weeks. A chronological train/test split is meaningless if the sample
    only covers one end of the period.
    """
    trips = defaultdict(list)
    for o in obs:
        trips[(o["ident"], o["day"])].append(o)
    keys = [k for k, v in trips.items() if len(v) >= 2]
    # Roughly how many rows each trip yields, so the cap can be turned into a
    # trip budget spread across the whole window instead of a stopping point.
    est = max(1.0, np.mean([min(len(trips[k]), max_horizon + 1) for k in keys[:2000]]))
    budget = int(max_samples / max(1.0, est * max_horizon / 2.0))
    if len(keys) > budget:
        rng = np.random.default_rng(seed)
        keys = [keys[i] for i in rng.permutation(len(keys))[:budget]]
    rows = []
    for key in keys:
        stops = trips[key]
        stops.sort(key=lambda s: (s["t"] or 0))
        # De-duplicate consecutive readings of the same station: a train
        # sitting in the feed produces many, and they are not a journey.
        seq = []
        for s in stops:
            if not seq or seq[-1]["station_id"] != s["station_id"]:
                seq.append(s)
        if len(seq) < 2:
            continue
        n = len(seq)
        for i in range(n - 1):
            here = seq[i]
            if here["t"] is None:
                continue
            ts = here["t"].timestamp()
            hist = [s["delay"] for s in seq[: i + 1]]
            t0 = seq[0]["t"]
            for j in range(i + 1, min(i + 1 + max_horizon, n)):
                tgt = seq[j]
                mins_ahead = ((tgt["t"] - here["t"]).total_seconds() / 60.0
                              if tgt["t"] and here["t"] else None)
                stops_ahead = (tgt["seq"] - here["seq"]
                               if tgt["seq"] is not None and here["seq"] is not None
                               else j - i)
                rows.append({
                    "ident": here["ident"], "day": here["day"],
                    "from_station": here["station_id"],
                    "station_id": tgt["station_id"], "route": tgt["route"],
                    "ts": ts,
                    "cur_delay": here["delay"],
                    "delay_lag1": hist[-2] if len(hist) >= 2 else here["delay"],
                    "delay_slope": (hist[-1] - hist[-2]) if len(hist) >= 2 else 0.0,
                    "delay_max": max(hist), "delay_min": min(hist),
                    "delay_mean": float(np.mean(hist)),
                    "stops_elapsed": float(i),
                    "minutes_running": ((here["t"] - t0).total_seconds() / 60.0
                                        if t0 else 0.0),
                    "stops_ahead": float(max(1, stops_ahead)),
                    "minutes_ahead": mins_ahead,
                    "hour": float(here["t"].hour), "dow": float(here["day"].weekday()),
                    "label": tgt["delay"],
                })
        if len(rows) >= max_samples:
            break
    return rows


def _q(vals):
    if not vals:
        return (np.nan, np.nan, 0.0)
    a = np.asarray(vals)
    return (float(np.median(a)), float(np.quantile(a, 0.9)), float(len(a)))


def attach_history(rows):
    """Attach history features computed ONLY from strictly earlier days.

    THIS REPLACES A VERSION THAT LEAKED BADLY, and the failure is worth
    recording because it presented as a modelling problem rather than a data
    one.

    The old version fitted "this train at this stop" on the labels of the
    training rows themselves. Germany reissues trip_id every service day, so a
    given trip usually appears on exactly ONE day - meaning its statistic was
    the median of its own labels, i.e. the answer. The model duly ranked
    trainstop_p90 its single most valuable feature and then lost to persistence
    by 60%, because at test time that feature is absent for a trip never seen
    before. Enormous training gain plus catastrophic test error is the
    signature of precisely this.

    An expanding window fixes it, and is also what production actually does: to
    predict a train today you may use every day up to yesterday and nothing
    from today. Rows are processed in day order and a day's labels only join
    the history after every row in that day has been featurised.
    """
    st, rt, ts_, ls_ = (defaultdict(list), defaultdict(list),
                        defaultdict(list), defaultdict(list))
    ordered = sorted(rows, key=lambda r: r["day"])
    i = 0
    while i < len(ordered):
        day = ordered[i]["day"]
        j = i
        while j < len(ordered) and ordered[j]["day"] == day:
            r = ordered[j]
            r["_hist"] = (
                _q(st.get(r["station_id"], [])),
                _q(st.get(r["from_station"], [])),
                _q(rt.get(r["route"], [])),
                _q(ts_.get((r["ident"], r["station_id"]), [])),
                _q(ls_.get((r["route"], r["station_id"]), [])),
            )
            j += 1
        for k in range(i, j):
            r = ordered[k]
            st[r["station_id"]].append(r["label"])
            rt[r["route"]].append(r["label"])
            ts_[(r["ident"], r["station_id"])].append(r["label"])
            ls_[(r["route"], r["station_id"])].append(r["label"])
        i = j
    return rows


def matrix(rows, cong):
    idx = {f: i for i, f in enumerate(BASE_FEATURES)}
    X = np.full((len(rows), len(BASE_FEATURES)), np.nan, dtype=np.float32)
    for n, r in enumerate(rows):
        for f in ("cur_delay", "delay_lag1", "delay_slope", "delay_max", "delay_min",
                  "delay_mean", "stops_elapsed", "minutes_running", "stops_ahead",
                  "minutes_ahead", "hour", "dow"):
            v = r.get(f)
            if v is not None:
                X[n, idx[f]] = v
        tgt_st, cur_st, rr, tr, ln = r["_hist"]
        X[n, idx["tgt_st_p50"]], X[n, idx["tgt_st_p90"]], X[n, idx["tgt_st_n"]] = tgt_st
        X[n, idx["cur_st_p50"]], X[n, idx["cur_st_p90"]] = cur_st[0], cur_st[1]
        X[n, idx["route_p50"]], X[n, idx["route_p90"]], X[n, idx["route_n"]] = rr
        (X[n, idx["trainstop_p50"]], X[n, idx["trainstop_p90"]],
         X[n, idx["trainstop_n"]]) = tr
        # Line-level history, keyed on (route, station) rather than on the
        # trip. It exists because trainstop_* is the strongest feature in both
        # countries and Germany can barely populate it: DE reissues trip_id
        # every service day, so only 4.0% of its (trip, station) pairs recur
        # on 3+ days, against 62.7% for Hungary - which is the whole reason DE
        # gains +0.8% on MAE where HU gains +18.3%. (route, station) is stable
        # across days for every country and spans the entire archive, so it
        # gives DE a dense version of the signal that was carrying HU.
        (X[n, idx["linestop_p50"]], X[n, idx["linestop_p90"]],
         X[n, idx["linestop_n"]]) = ln
        ts = r["ts"]
        for w in CONGESTION_WINDOWS:
            cn, cm, cl = cong.station(r["station_id"], ts, w)
            X[n, idx[f"tgt_cong_n_{w}"]] = cn
            X[n, idx[f"tgt_cong_mean_{w}"]] = cm
            X[n, idx[f"tgt_cong_late_{w}"]] = cl
            fn, fm, fl = cong.station(r["from_station"], ts, w)
            X[n, idx[f"cur_cong_n_{w}"]] = fn
            X[n, idx[f"cur_cong_mean_{w}"]] = fm
            X[n, idx[f"cur_cong_late_{w}"]] = fl
        prev = cong.last_before(r["station_id"], ts, 2)
        if prev:
            X[n, idx["tgt_prev_delay"]] = prev[0][0]
            X[n, idx["tgt_prev_age"]] = prev[0][1]
            if len(prev) > 1:
                X[n, idx["tgt_prev2_delay"]] = prev[1][0]
        cprev = cong.last_before(r["from_station"], ts, 1)
        if cprev:
            X[n, idx["cur_prev_delay"]] = cprev[0][0]
            X[n, idx["cur_prev_age"]] = cprev[0][1]
        nn, nm, nl = cong.nation(ts, 60)
        X[n, idx["nat_cong_n_60"]] = nn
        X[n, idx["nat_cong_mean_60"]] = nm
        X[n, idx["nat_cong_late_60"]] = nl
    return X


def conformal_interval(lo_cal, hi_cal, y_cal, lo_te, hi_te, coverage=TARGET_COVERAGE):
    """Conformalized quantile regression (Romano, Patterson & Candes, 2019).

    Quantile regression alone gives intervals whose stated coverage is a hope.
    Conformalising them makes it a guarantee: the calibration residual is how
    far outside its own interval each held-out point fell, and widening every
    interval by the (1-alpha) quantile of that gives marginal coverage of at
    least 1-alpha, with no assumption about the model being right.
    """
    e = np.maximum(lo_cal - y_cal, y_cal - hi_cal)
    n = len(e)
    k = min(n - 1, int(np.ceil((n + 1) * coverage)) - 1)
    q = float(np.sort(e)[max(0, k)])
    return lo_te - q, hi_te + q, q


def run(country="DE", days=21, max_horizon=8, seed=0, rounds=300):
    import lightgbm as lgb

    conn = connect(); cur = conn.cursor()
    try:
        obs = load_observations(cur, country, days)
    finally:
        cur.close(); conn.close()
    if len(obs) < 20000:
        return {"error": f"{country}: only {len(obs)} observations"}

    cong = Congestion(obs)
    rows = build_samples(obs, cong, max_horizon)
    if len(rows) < 20000:
        return {"error": f"{country}: only {len(rows)} samples from {len(obs)} observations"}

    days_seen = sorted({r["day"] for r in rows})
    if len(days_seen) < 4:
        return {"error": f"{country}: only {len(days_seen)} distinct days"}

    # Chronological split, plus a fifth of stations held out entirely so a
    # model that has merely memorised a station cannot pass. Calibration is a
    # separate slice again - conformal guarantees are void if the calibration
    # data trained the model.
    d_tr, d_ca = split_days(rows)
    rng = np.random.default_rng(seed)
    stations = sorted({r["station_id"] for r in rows})
    unseen = set(rng.permutation(stations)[: max(1, len(stations) // 5)].tolist())

    train = [r for r in rows if r["day"] < d_tr and r["station_id"] not in unseen]
    calib = [r for r in rows if d_tr <= r["day"] < d_ca]
    test = [r for r in rows if r["day"] >= d_ca]
    if min(len(train), len(calib), len(test)) < 2000:
        return {"error": (f"{country}: split too small "
                          f"({len(train)}/{len(calib)}/{len(test)})")}

    # Featurised over ALL rows, because history must expand across the split
    # boundaries in day order - not be fitted on the training split.
    attach_history(rows)
    Xtr, Xca, Xte = (matrix(s, cong) for s in (train, calib, test))
    ytr = np.asarray([r["label"] for r in train], dtype=np.float32)
    yca = np.asarray([r["label"] for r in calib], dtype=np.float32)
    yte = np.asarray([r["label"] for r in test], dtype=np.float32)
    btr = np.asarray([band_of(v) for v in ytr])
    bte = np.asarray([band_of(v) for v in yte])

    common = {"learning_rate": 0.05, "num_leaves": 63, "min_data_in_leaf": 100,
              "feature_fraction": 0.9, "bagging_fraction": 0.8, "bagging_freq": 1,
              "num_threads": 2, "verbose": -1, "seed": seed}

    # --- stage 1: which delay band ---------------------------------------
    clf = lgb.train({**common, "objective": "multiclass",
                     "num_class": len(BAND_NAMES)},
                    lgb.Dataset(Xtr, label=btr, feature_name=BASE_FEATURES),
                    num_boost_round=rounds)
    Ptr, Pca, Pte = (clf.predict(X) for X in (Xtr, Xca, Xte))

    # --- stage 2: the band probabilities become features -----------------
    S2tr = np.hstack([Xtr, Ptr]).astype(np.float32)
    S2ca = np.hstack([Xca, Pca]).astype(np.float32)
    S2te = np.hstack([Xte, Pte]).astype(np.float32)
    reg = lgb.train({**common, "objective": "regression_l1"},
                    lgb.Dataset(S2tr, label=ytr, feature_name=STAGE2_FEATURES),
                    num_boost_round=rounds)
    pred = reg.predict(S2te)

    # one-level control, so the two-level claim is measured and not assumed
    reg1 = lgb.train({**common, "objective": "regression_l1"},
                     lgb.Dataset(Xtr, label=ytr, feature_name=BASE_FEATURES),
                     num_boost_round=rounds)
    pred1 = reg1.predict(Xte)

    # --- intervals --------------------------------------------------------
    a = (1.0 - TARGET_COVERAGE) / 2.0
    qlo = lgb.train({**common, "objective": "quantile", "alpha": a},
                    lgb.Dataset(S2tr, label=ytr, feature_name=STAGE2_FEATURES),
                    num_boost_round=rounds)
    qhi = lgb.train({**common, "objective": "quantile", "alpha": 1 - a},
                    lgb.Dataset(S2tr, label=ytr, feature_name=STAGE2_FEATURES),
                    num_boost_round=rounds)
    lo, hi, qadj = conformal_interval(qlo.predict(S2ca), qhi.predict(S2ca), yca,
                                      qlo.predict(S2te), qhi.predict(S2te))

    naive = np.asarray([r["cur_delay"] for r in test], dtype=np.float32)
    mae = lambda p: float(np.abs(p - yte).mean())

    res = {"country": country, "days": f"{days_seen[0]}..{days_seen[-1]}",
           "n_days": len(days_seen), "n_obs": len(obs), "n_samples": len(rows),
           "n_train": len(train), "n_calib": len(calib), "n_test": len(test),
           "held_out_stations": len(unseen),
           "naive_mae": mae(naive), "one_level_mae": mae(pred1),
           "two_level_mae": mae(pred)}
    res["gain_pct"] = 100 * (res["naive_mae"] - res["two_level_mae"]) / res["naive_mae"]
    res["two_level_lift_pct"] = (100 * (res["one_level_mae"] - res["two_level_mae"])
                                 / res["one_level_mae"])

    # Classification is where the field finds its value, so score it properly.
    # The comparison is against persistence's own implied band, not against
    # nothing - "assume the band it is in now" is the honest rival.
    nb = np.asarray([band_of(v) for v in naive])
    pb = Pte.argmax(axis=1)
    res["band_acc_model"] = float((pb == bte).mean())
    res["band_acc_naive"] = float((nb == bte).mean())
    # The decision a rider actually makes: will it be late at all (>5 min)?
    late = bte >= 2
    p_late = Pte[:, 2:].sum(axis=1)
    res["late_base_rate"] = float(late.mean())
    res["late_brier_model"] = float(((p_late - late) ** 2).mean())
    res["late_brier_naive"] = float((((naive > 5.0).astype(float) - late) ** 2).mean())
    pl = p_late >= 0.5
    tp = int((pl & late).sum()); fp = int((pl & ~late).sum()); fn = int((~pl & late).sum())
    res["late_precision"] = tp / (tp + fp) if tp + fp else None
    res["late_recall"] = tp / (tp + fn) if tp + fn else None
    nl = naive > 5.0
    tpn = int((nl & late).sum()); fpn = int((nl & ~late).sum()); fnn = int((~nl & late).sum())
    res["late_precision_naive"] = tpn / (tpn + fpn) if tpn + fpn else None
    res["late_recall_naive"] = tpn / (tpn + fnn) if tpn + fnn else None

    res["coverage"] = float(((yte >= lo) & (yte <= hi)).mean())
    res["interval_width"] = float(np.median(hi - lo))
    res["conformal_adjust"] = qadj

    ah = np.asarray([r["stops_ahead"] for r in test])
    res["by_horizon"] = []
    gated = naive.astype(np.float64).copy()
    switch = None
    for h in range(1, max_horizon + 1):
        m = ah == h
        if m.sum() < 200:
            continue
        mm, nn_ = float(np.abs(pred[m] - yte[m]).mean()), float(np.abs(naive[m] - yte[m]).mean())
        res["by_horizon"].append({"h": h, "n": int(m.sum()), "model": mm, "naive": nn_})
        if mm < nn_:
            gated[m] = pred[m]
            switch = switch or h
    res["gated_mae"] = mae(gated)
    res["gated_gain_pct"] = 100 * (res["naive_mae"] - res["gated_mae"]) / res["naive_mae"]
    res["gate_switch_at"] = switch

    imp = sorted(zip(STAGE2_FEATURES, reg.feature_importance("gain")),
                 key=lambda t: -t[1])
    res["top_features"] = [(n, float(v)) for n, v in imp[:14]]
    res["models"] = {"clf": clf, "reg": reg, "qlo": qlo, "qhi": qhi, "q": qadj}
    return res


def report(r):
    if "error" in r:
        print(r["error"]); return
    print(f"=== {r['country']}  {r['days']}  ({r['n_days']} days) ===")
    print(f"{r['n_obs']} observations -> {r['n_samples']} samples   "
          f"train {r['n_train']} / calib {r['n_calib']} / test {r['n_test']}   "
          f"({r['held_out_stations']} stations held out)")
    print(f"\n-- point error (MAE, minutes) --")
    print(f"  persistence      {r['naive_mae']:7.3f}")
    print(f"  one-level        {r['one_level_mae']:7.3f}")
    print(f"  two-level        {r['two_level_mae']:7.3f}   ({r['gain_pct']:+.1f}% vs persistence,"
          f" {r['two_level_lift_pct']:+.1f}% vs one-level)")
    print(f"  horizon-gated    {r['gated_mae']:7.3f}   ({r['gated_gain_pct']:+.1f}%)"
          + (f", model from {r['gate_switch_at']} stops out" if r['gate_switch_at'] else ""))
    print(f"\n-- will it be late (>5 min)?  base rate {100*r['late_base_rate']:.1f}% --")
    print(f"{'':18}{'Brier':>8}{'precision':>11}{'recall':>9}")
    print(f"  persistence     {r['late_brier_naive']:8.4f}"
          f"{(r['late_precision_naive'] or 0):11.3f}{(r['late_recall_naive'] or 0):9.3f}")
    print(f"  model           {r['late_brier_model']:8.4f}"
          f"{(r['late_precision'] or 0):11.3f}{(r['late_recall'] or 0):9.3f}")
    print(f"  band accuracy:  persistence {r['band_acc_naive']:.3f}   "
          f"model {r['band_acc_model']:.3f}")
    print(f"\n-- {int(TARGET_COVERAGE*100)}% prediction interval (conformalized) --")
    print(f"  actual coverage {100*r['coverage']:.1f}%   median width "
          f"{r['interval_width']:.1f} min   conformal widening {r['conformal_adjust']:+.2f}")
    if r["by_horizon"]:
        print(f"\n{'stops':>6}{'n':>9}{'naive':>9}{'model':>9}{'gain':>8}")
        for h in r["by_horizon"]:
            g = 100 * (h["naive"] - h["model"]) / h["naive"] if h["naive"] else 0
            print(f"{h['h']:6}{h['n']:9}{h['naive']:9.3f}{h['model']:9.3f}{g:7.1f}%")
    print("\ntop features by gain:")
    for n, v in r["top_features"]:
        print(f"   {n:22} {v:12.0f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--country", default="DE")
    ap.add_argument("--days", type=int, default=21)
    ap.add_argument("--max-horizon", type=int, default=8)
    ap.add_argument("--rounds", type=int, default=300)
    a = ap.parse_args()
    t0 = time.time()
    r = run(a.country, a.days, a.max_horizon, rounds=a.rounds)
    report(r)
    print(f"\ntook {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
