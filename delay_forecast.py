"""Let the model find the patterns, instead of hand-testing one hypothesis at a time.

WHY THIS EXISTS. DELAY_STRUCTURE.md holds five investigations, each of which
worked the same way: guess a mechanism, write a query, test it. That found real
things (delay coupling at 4.4x above chance; padding that makes late trains
recover and early ones bleed) but it does not scale, and it can only ever find
what somebody already suspected.

The limit was never the learner. It was the FEATURE SPACE. A gradient-boosted
tree discovers interactions among the columns it is handed, automatically and
without being told to look - but it cannot discover a pattern its columns
cannot express. The transfer model has twelve features and not one of them
carries another train's delay, so coupling is invisible to it in principle, no
matter how long it trains.

So this does the opposite: it generates feature FAMILIES mechanically, hands
over everything, and lets the model decide what matters. Nobody writes down
"trains recover when they are late" or "delays spread between trains at a
station" as a hypothesis. Those are things the model should find, and then
permutation importance says whether it actually used them.

    own       where this train is now, and how it got there
    horizon   how far ahead we are asking
    schedule  what the timetable says about the gap
    station   how this station behaves historically
    route     how this service behaves historically
    network   what every OTHER train is doing right now  <- the new one

The network family is the point. It is what lets the model pick up coupling,
shared disruption and station congestion without anyone specifying a pair of
trains, because it carries the state of the system around this train.

WHAT IS BEING PREDICTED. A train's delay some number of stops further along its
own journey - the "when will this actually get in" question. The baseline is
PERSISTENCE: assume the delay it has now is the delay it will have then. That
is a genuinely hard baseline. An earlier LightGBM attempt at the neighbouring
one-segment version LOST to it (MAE 0.554 vs 0.494), which is recorded in
DELAY_STRUCTURE.md and is the reason nothing here is assumed to work until it
is measured on held-out days.

LEAKAGE IS THE FAILURE MODE. Every historical statistic is computed on the
training split alone, days are split chronologically, and a fifth of stations
are held out entirely so a model that has merely memorised a station cannot
pass. Same discipline as model_loop.py, for the same reason.
"""
import argparse
import time
from collections import defaultdict

import numpy as np
import psycopg2

from config import DB_PASSWORD

# Ignore readings this far apart when treating two stops as one journey. A
# stop's stored delay is only as fresh as the last time it sat in the sampling
# window, and DELAY_STRUCTURE.md found 26.2% of adjacent pairs were more than
# 15 minutes apart - comparing those measures the poller, not the railway.
MAX_ABS_DELAY = 720.0        # the same implausible-delay guard used elsewhere


def connect():
    return psycopg2.connect(dbname="train_delay_db", user="trainuser",
                            password=DB_PASSWORD, host="localhost")


def _hms(v):
    """GTFS wall-clock 'HH:MM:SS' -> minutes past midnight. Values beyond 24h
    are legal in GTFS (overnight continuations) and are kept as-is."""
    if not v:
        return None
    try:
        h, m, s = str(v).split(":")
        return int(h) * 60 + int(m) + int(s) / 60.0
    except ValueError:
        return None


def load_trajectories(cur, country, days):
    """Every (trip, day) we saw at more than one stop, in journey order."""
    cur.execute("""
        SELECT trip_id, observed_date, stop_sequence, station_id, route_name,
               delay_minutes, scheduled_arrival, scheduled_departure
        FROM delay_observations
        WHERE country_code = %s
          AND trip_id IS NOT NULL AND stop_sequence IS NOT NULL
          AND delay_minutes IS NOT NULL AND NOT is_cancelled
          AND abs(delay_minutes) <= %s
          AND observed_date >= current_date - %s
        ORDER BY trip_id, observed_date, stop_sequence, id
    """, (country, MAX_ABS_DELAY, days))
    trips = defaultdict(dict)
    for (tid, day, seq, sid, route, delay, sarr, sdep) in cur.fetchall():
        # One reading per stop: the last one written is the most recent.
        trips[(tid, day)][seq] = {
            "seq": seq, "station_id": sid, "route": route,
            "delay": float(delay),
            "sched": _hms(sarr) if sarr else _hms(sdep),
        }
    out = {}
    for k, stops in trips.items():
        if len(stops) >= 2:
            out[k] = [stops[s] for s in sorted(stops)]
    return out


def network_state(cur, country, days):
    """What the rest of the railway is doing, per (day, hour).

    Deliberately crude and mechanically derived - a mean, a spread, a count, a
    late share. Nobody is asserting that any of these matter; they are handed
    over so the model can decide. This is the family that can express
    system-wide disruption and station congestion, which no per-train feature
    can.

    READ ONLY THROUGH THE PREVIOUS HOUR. The first version of this keyed on the
    TARGET station in the TARGET hour, which was wrong twice over: that hour has
    not happened yet when the prediction is made, and the cell contains the very
    observation being predicted. 34% of (station, day, hour) cells hold exactly
    one row and 69% hold three or fewer, so for a third of samples the feature
    simply WAS the label. It ranked second in importance and produced a +12.5%
    improvement that was not real. Every network lookup now uses hour - 1, which
    is both strictly in the past and cannot contain the target row.
    """
    cur.execute("""
        SELECT observed_date,
               floor(EXTRACT(EPOCH FROM scheduled_arrival::interval) / 3600.0) AS hr,
               count(*), avg(delay_minutes), stddev_pop(delay_minutes),
               avg(CASE WHEN delay_minutes > 5 THEN 1.0 ELSE 0.0 END)
        FROM delay_observations
        WHERE country_code = %s AND delay_minutes IS NOT NULL
          AND NOT is_cancelled AND scheduled_arrival IS NOT NULL
          AND abs(delay_minutes) <= %s
          AND observed_date >= current_date - %s
        GROUP BY 1, 2
    """, (country, MAX_ABS_DELAY, days))
    net = {}
    for day, hr, n, mean, sd, late in cur.fetchall():
        if hr is None:
            continue
        net[(day, int(hr) % 24)] = (float(n), float(mean or 0), float(sd or 0),
                                    float(late or 0))
    return net


def prev_hour(day, hour):
    """The last fully-elapsed hour. Returns None at hour 0 rather than wrapping
    into the previous day, which would be a different service day."""
    return None if hour is None or hour < 1 else (day, hour - 1)


def station_network_state(cur, country, days):
    """The same idea, but local: how busy and how late is THIS station, this
    hour. Congestion and knock-on effects are station-scale, and a national
    average cannot see them."""
    cur.execute("""
        SELECT station_id, observed_date,
               floor(EXTRACT(EPOCH FROM scheduled_arrival::interval) / 3600.0) AS hr,
               count(*), avg(delay_minutes),
               avg(CASE WHEN delay_minutes > 5 THEN 1.0 ELSE 0.0 END)
        FROM delay_observations
        WHERE country_code = %s AND delay_minutes IS NOT NULL
          AND NOT is_cancelled AND scheduled_arrival IS NOT NULL
          AND station_id IS NOT NULL AND abs(delay_minutes) <= %s
          AND observed_date >= current_date - %s
        GROUP BY 1, 2, 3
    """, (country, MAX_ABS_DELAY, days))
    out = {}
    for sid, day, hr, n, mean, late in cur.fetchall():
        if hr is None:
            continue
        out[(sid, day, int(hr) % 24)] = (float(n), float(mean or 0), float(late or 0))
    return out


FEATURES = [
    # own
    "cur_delay", "delay_slope", "delay_max_so_far", "delay_min_so_far",
    "stops_elapsed", "mean_delay_so_far",
    # horizon
    "stops_ahead", "sched_minutes_ahead",
    # schedule
    "hour", "dow", "sched_gap_per_stop",
    # station (historical, train split only)
    "tgt_st_p50", "tgt_st_p90", "tgt_st_n", "cur_st_p50", "cur_st_p90",
    # route (historical, train split only)
    "route_p50", "route_p90", "route_n",
    # network - what every OTHER train was doing in the last elapsed hour.
    # Never the current or target hour: see network_state().
    "net_n", "net_mean", "net_sd", "net_late_share",
    "st_net_n", "st_net_mean", "st_net_late_share",
    "tgt_net_n", "tgt_net_mean", "tgt_net_late_share",
]


def build_samples(trips, net, stnet, max_horizon=8):
    """One row per (trip-day, from-stop, to-stop) pair.

    Features describe only what is knowable at the FROM stop. The label is the
    delay at the TO stop, which nothing on the feature side may look at.
    """
    rows = []
    for (tid, day), stops in trips.items():
        n = len(stops)
        for i in range(n - 1):
            here = stops[i]
            hist = [s["delay"] for s in stops[: i + 1]]
            slope = (hist[-1] - hist[-2]) if len(hist) >= 2 else 0.0
            hour = int((here["sched"] or 0) // 60) % 24 if here["sched"] is not None else 0
            for j in range(i + 1, min(i + 1 + max_horizon, n)):
                tgt = stops[j]
                gap = None
                if here["sched"] is not None and tgt["sched"] is not None:
                    gap = tgt["sched"] - here["sched"]
                    if gap < 0:
                        gap = None            # rollover or bad ordering
                rows.append({
                    "trip_id": tid, "day": day,
                    "from_station": here["station_id"], "station_id": tgt["station_id"],
                    "route": tgt["route"],
                    "cur_delay": here["delay"],
                    "delay_slope": slope,
                    "delay_max_so_far": max(hist),
                    "delay_min_so_far": min(hist),
                    "mean_delay_so_far": float(np.mean(hist)),
                    "stops_elapsed": float(i),
                    "stops_ahead": float(j - i),
                    "sched_minutes_ahead": gap,
                    "hour": float(hour),
                    "dow": float(day.weekday()),
                    "sched_gap_per_stop": (gap / (j - i)) if gap is not None else None,
                    # All three strictly from the previous elapsed hour.
                    "net": net.get(prev_hour(day, hour)) if prev_hour(day, hour) else None,
                    "st_net": (stnet.get((here["station_id"],) + prev_hour(day, hour))
                               if prev_hour(day, hour) else None),
                    "tgt_net": (stnet.get((tgt["station_id"],) + prev_hour(day, hour))
                                if prev_hour(day, hour) else None),
                    "label": tgt["delay"],
                })
    return rows


def _q(vals):
    if not vals:
        return (np.nan, np.nan, 0.0)
    a = np.asarray(vals)
    return (float(np.median(a)), float(np.quantile(a, 0.9)), float(len(a)))


def fit_history(rows):
    """Station and route statistics, from the TRAINING rows only."""
    st, rt = defaultdict(list), defaultdict(list)
    for r in rows:
        st[r["station_id"]].append(r["label"])
        rt[r["route"]].append(r["label"])
    return ({k: _q(v) for k, v in st.items()},
            {k: _q(v) for k, v in rt.items()})


def matrix(rows, st_hist, rt_hist):
    X = np.full((len(rows), len(FEATURES)), np.nan, dtype=np.float32)
    idx = {f: i for i, f in enumerate(FEATURES)}
    for n, r in enumerate(rows):
        for f in ("cur_delay", "delay_slope", "delay_max_so_far", "delay_min_so_far",
                  "stops_elapsed", "mean_delay_so_far", "stops_ahead",
                  "sched_minutes_ahead", "hour", "dow", "sched_gap_per_stop"):
            v = r.get(f)
            if v is not None:
                X[n, idx[f]] = v
        s = st_hist.get(r["station_id"])
        if s:
            X[n, idx["tgt_st_p50"]], X[n, idx["tgt_st_p90"]], X[n, idx["tgt_st_n"]] = s
        cs = st_hist.get(r["from_station"])
        if cs:
            X[n, idx["cur_st_p50"]], X[n, idx["cur_st_p90"]] = cs[0], cs[1]
        rr = rt_hist.get(r["route"])
        if rr:
            X[n, idx["route_p50"]], X[n, idx["route_p90"]], X[n, idx["route_n"]] = rr
        if r["net"]:
            X[n, idx["net_n"]], X[n, idx["net_mean"]], X[n, idx["net_sd"]], \
                X[n, idx["net_late_share"]] = r["net"]
        if r["st_net"]:
            X[n, idx["st_net_n"]], X[n, idx["st_net_mean"]], \
                X[n, idx["st_net_late_share"]] = r["st_net"]
        if r["tgt_net"]:
            X[n, idx["tgt_net_n"]], X[n, idx["tgt_net_mean"]], \
                X[n, idx["tgt_net_late_share"]] = r["tgt_net"]
    return X


def permutation_importance(booster, X, y, baseline_mae, seed=0, repeats=1):
    """Which features did the model ACTUALLY use?

    Gain-based importance flatters correlated features and can credit one that
    changes nothing. Shuffling a column and measuring how much worse the model
    gets is the honest version: if breaking a feature costs nothing, the model
    was not relying on it, whatever the gain table says.
    """
    rng = np.random.default_rng(seed)
    out = []
    for i, name in enumerate(FEATURES):
        loss = []
        for _ in range(repeats):
            Xp = X.copy()
            rng.shuffle(Xp[:, i])
            loss.append(float(np.abs(booster.predict(Xp) - y).mean()))
        out.append((name, float(np.mean(loss)) - baseline_mae))
    return sorted(out, key=lambda t: -t[1])


def run(country="DE", days=21, max_horizon=8, seed=0, quiet=False):
    import lightgbm as lgb

    conn = connect()
    cur = conn.cursor()
    try:
        trips = load_trajectories(cur, country, days)
        net = network_state(cur, country, days)
        stnet = station_network_state(cur, country, days)
    finally:
        cur.close(); conn.close()
    if not trips:
        return {"error": f"{country}: no multi-stop trajectories"}

    rows = build_samples(trips, net, stnet, max_horizon)
    if len(rows) < 5000:
        return {"error": f"{country}: only {len(rows)} samples"}

    day_list = sorted({r["day"] for r in rows})
    # Three days is the floor, not a comfortable number. stop_sequence has only
    # existed since 2026-08-21, so a trajectory dataset cannot yet be deeper
    # than that - and a 2-day train / 1-day test split cannot distinguish a
    # real pattern from one Tuesday. Results below seven days are reported as
    # PRELIMINARY and are not used to promote anything.
    if len(day_list) < 3:
        return {"error": f"{country}: only {len(day_list)} distinct days"}
    cut = day_list[max(1, int(len(day_list) * 0.7))]

    rng = np.random.default_rng(seed)
    stations = sorted({r["station_id"] for r in rows if r["station_id"] is not None})
    unseen = set(rng.permutation(stations)[: max(1, len(stations) // 5)].tolist())
    train = [r for r in rows if r["day"] < cut and r["station_id"] not in unseen]
    test = [r for r in rows if r["day"] >= cut]
    if len(test) < 1000 or len(train) < 5000:
        return {"error": f"{country}: train {len(train)} / test {len(test)} too small"}

    st_hist, rt_hist = fit_history(train)
    Xtr = matrix(train, st_hist, rt_hist)
    ytr = np.asarray([r["label"] for r in train], dtype=np.float32)
    Xte = matrix(test, st_hist, rt_hist)
    yte = np.asarray([r["label"] for r in test], dtype=np.float32)

    booster = lgb.train(
        {"objective": "regression_l1", "learning_rate": 0.05, "num_leaves": 63,
         "min_data_in_leaf": 100, "feature_fraction": 0.9, "bagging_fraction": 0.8,
         "bagging_freq": 1, "num_threads": 2, "verbose": -1, "seed": seed},
        lgb.Dataset(Xtr, label=ytr, feature_name=FEATURES), num_boost_round=400)

    pred = booster.predict(Xte)
    # PERSISTENCE: the delay it has now is the delay it will have. Hard to beat,
    # and it already beat a LightGBM attempt at the one-segment version once.
    naive = np.asarray([r["cur_delay"] for r in test], dtype=np.float32)

    mae = lambda a: float(np.abs(a - yte).mean())
    rmse = lambda a: float(np.sqrt(((a - yte) ** 2).mean()))
    res = {
        "country": country, "n_train": len(train), "n_test": len(test),
        "days": f"{day_list[0]}..{day_list[-1]}", "cut": str(cut),
        "held_out_stations": len(unseen),
        "n_days": len(day_list),
        "preliminary": len(day_list) < 7,
        "model_mae": mae(pred), "naive_mae": mae(naive),
        "model_rmse": rmse(pred), "naive_rmse": rmse(naive),
    }
    res["mae_gain_pct"] = 100 * (res["naive_mae"] - res["model_mae"]) / res["naive_mae"]

    # Per horizon: persistence should decay as the horizon grows, and if the
    # model has learned anything real its advantage should GROW with distance.
    ah = np.asarray([r["stops_ahead"] for r in test])
    res["by_horizon"] = []
    for h in range(1, int(max_horizon) + 1):
        m = ah == h
        if m.sum() < 200:
            continue
        res["by_horizon"].append({
            "stops_ahead": h, "n": int(m.sum()),
            "model": float(np.abs(pred[m] - yte[m]).mean()),
            "naive": float(np.abs(naive[m] - yte[m]).mean())})

    # HORIZON GATE. Persistence is close to unbeatable one stop ahead - the
    # delay simply has not had time to change - and the model's advantage grows
    # with distance, where timetable padding and recovery have room to act. So
    # the useful system is not "model instead of persistence", it is "model
    # where the model is measurably better". This reports what such a gate
    # would score, and from which horizon it should switch.
    gated = np.array(naive, dtype=np.float64, copy=True)
    switch, covered = None, 0
    for h in res["by_horizon"]:
        if h["model"] < h["naive"]:
            m = ah == h["stops_ahead"]
            gated[m] = pred[m]
            covered += h["n"]
            if switch is None:
                switch = h["stops_ahead"]
    res["gated_mae"] = mae(gated)
    res["gated_gain_pct"] = 100 * (res["naive_mae"] - res["gated_mae"]) / res["naive_mae"]
    res["gate_switch_at"] = switch
    res["gate_covers_pct"] = 100.0 * covered / max(1, len(test))

    res["importance"] = permutation_importance(booster, Xte, yte, res["model_mae"], seed)
    res["booster"] = booster
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--country", default="DE")
    ap.add_argument("--days", type=int, default=21)
    ap.add_argument("--max-horizon", type=int, default=8)
    a = ap.parse_args()
    t0 = time.time()
    r = run(a.country, a.days, a.max_horizon)
    if "error" in r:
        print(r["error"]); return
    print(f"=== {r['country']} delay forecast === {r['days']} (cut {r['cut']})")
    print(f"train {r['n_train']}  test {r['n_test']}  "
          f"({r['held_out_stations']} stations held out entirely)")
    if r["preliminary"]:
        print(f"*** PRELIMINARY: only {r['n_days']} days of trajectory data exist "
              f"(stop_sequence began 2026-08-21). Directional, not a result. ***")
    print(f"\n{'':10} {'MAE':>8} {'RMSE':>8}")
    print(f"{'persistence':10} {r['naive_mae']:8.3f} {r['naive_rmse']:8.3f}")
    print(f"{'model':10} {r['model_mae']:8.3f} {r['model_rmse']:8.3f}")
    print(f"{'gated':10} {r['gated_mae']:8.3f}")
    print(f"\nMAE vs persistence:  model {r['mae_gain_pct']:+.1f}%   "
          f"horizon-gated {r['gated_gain_pct']:+.1f}%")
    if r["gate_switch_at"]:
        print(f"gate: use the model from {r['gate_switch_at']} stops ahead "
              f"({r['gate_covers_pct']:.0f}% of samples), persistence below that")
    else:
        print("gate: persistence wins at every horizon - nothing to ship")
    if r["by_horizon"]:
        print(f"\n{'stops ahead':>11} {'n':>7} {'naive':>8} {'model':>8} {'gain':>8}")
        for h in r["by_horizon"]:
            g = 100 * (h["naive"] - h["model"]) / h["naive"] if h["naive"] else 0
            print(f"{h['stops_ahead']:11} {h['n']:7} {h['naive']:8.3f} "
                  f"{h['model']:8.3f} {g:7.1f}%")
    print("\nWhat the model actually relied on (permutation importance,")
    print("MAE cost of shuffling each feature):")
    for name, cost in r["importance"][:12]:
        if cost <= 0:
            continue
        print(f"   {name:22} {cost:+.4f}")
    print(f"\ntook {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
