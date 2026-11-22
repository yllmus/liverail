"""Does our transfer probability mean anything? Measure it, then improve it.

A probability nobody checks against outcomes is decoration. This builds real
labelled transfer outcomes out of the archive, scores the model we ship today
against them, and scores two alternatives, so any change to the risk figure is
justified by a number rather than by a story.

Numpy only, deliberately. sklearn and scipy are not installed and this box has
~880 MB of RAM free and two cores shared with four pollers; adding a heavy
dependency to fit a model is not worth it when the part of the literature that
actually matters here can be written in fifty lines.

--------------------------------------------------------------------------
WHAT THE LITERATURE SAYS, AND WHAT WE TOOK FROM IT
--------------------------------------------------------------------------
Two directly relevant precedents:

* Bahnvorhersage (bahnvorhersage.de, open source) predicts exactly this for
  Germany, from ~700 GB and a billion stops, using features amounting to
  delay history by train type, month and weekday. It is a production system,
  and it is proof the question is answerable.

* "Probabilistic modeling of delays for train journeys with transfers"
  (arXiv 2504.17479) pairs a gradient-boosted transfer-reliability model with
  a delay model whose key idea is a TWO-COMPONENT LOGNORMAL MIXTURE, and they
  validate on stations the model never trained on.

The mixture is the part worth stealing. A single distribution cannot describe
train delay because two different processes generate it: normal operation,
where nearly everything is within a few minutes, and disruption, where the
tail runs to hours. One lognormal fits the body and its tail decays far too
fast to represent the disrupted cases - which are precisely the cases that
break a connection.

Our shipped model does something cruder still: it interpolates stored
quantiles and holds flat above p95, so everything beyond the 95th percentile
is treated as exactly p95. That is a deliberate, documented choice made
because `worst` (a single maximum) was worse. A fitted mixture should beat
both, and this harness is how we find out rather than assume.

Their gradient booster we cannot copy without sklearn. The honest substitute
is to fit the one parameter that matters most and is currently invented -
interchange time - directly from outcomes, per station.

--------------------------------------------------------------------------
NO LOOKAHEAD
--------------------------------------------------------------------------
The single thing that would make every number here a lie: distributions are
fitted ONLY on days strictly before the test day. A model that has seen the
test day's delays will score beautifully and predict nothing. The split is by
date, not random, for the same reason.

Validation also holds out whole STATIONS, following the paper: a model that
only works where it was fitted is not a model, it is a lookup table.

--------------------------------------------------------------------------
WHAT THIS CAN AND CANNOT SEE
--------------------------------------------------------------------------
We observe trains, not passengers. Every "made" here means the connection was
physically possible given an assumed interchange time - never that anyone
actually made it. Say so wherever a figure derived from this is shown.
"""
import argparse
import math
from collections import defaultdict

import numpy as np
import psycopg2

from config import DB_CONFIG

# Interchange time assumed when scoring, in minutes. This is the parameter the
# whole model is most sensitive to and the one no feed publishes; fit_mct()
# below estimates it per station from outcomes instead.
DEFAULT_MCT_MIN = 5.0
MIN_SAMPLES_FOR_FIT = 30


def connect():
    return psycopg2.connect(**DB_CONFIG)


def hms_seconds(v):
    if v is None:
        return None
    try:
        h, m, s = (int(x) for x in str(v).split(":"))
    except ValueError:
        return None
    return h * 3600 + m * 60 + s


def load_transfers(cur, country, days, limit, min_buffer=60, max_buffer=3600,
                   sample_permille=1000):
    """Real transfer opportunities with both trains' observed delays.

    One row per (feeder trip, connector trip, station, date). DISTINCT ON keeps
    the latest reading of each train at each stop, because the poller samples
    the same train many times as it approaches.
    """
    cur.execute("""
        WITH obs AS (
          SELECT DISTINCT ON (o.trip_id, o.station_id, o.observed_date)
                 o.trip_id, o.station_id, o.observed_date, o.delay_minutes,
                 o.route_name
          FROM delay_observations o
          WHERE o.country_code = %s AND o.trip_id IS NOT NULL
            AND o.delay_minutes IS NOT NULL AND NOT o.is_cancelled
            AND o.observed_date >= CURRENT_DATE - %s
          ORDER BY o.trip_id, o.station_id, o.observed_date, o.id DESC
        )
        SELECT a.station_id, a.observed_date,
               a.trip_id, a.delay_minutes, a.route_name,
               b.trip_id, b.delay_minutes, b.route_name,
               sa.scheduled_arrival, sb.scheduled_departure
        FROM obs a
        JOIN stop_times sa ON sa.trip_id = a.trip_id AND sa.station_id = a.station_id
        JOIN obs b ON b.station_id = a.station_id AND b.observed_date = a.observed_date
                  AND b.trip_id <> a.trip_id
        JOIN stop_times sb ON sb.trip_id = b.trip_id AND sb.station_id = b.station_id
        WHERE sa.scheduled_arrival IS NOT NULL AND sb.scheduled_departure IS NOT NULL
          AND (EXTRACT(EPOCH FROM sb.scheduled_departure::interval)
             - EXTRACT(EPOCH FROM sa.scheduled_arrival::interval)) BETWEEN %s AND %s
        -- Sampled pseudo-randomly rather than LIMITed straight off the join.
        -- A plain LIMIT returned whatever order the join produced, which was
        -- so skewed by date that a 150k sample held 116k training rows and
        -- 420 test rows.
        AND mod(abs(hashtext(a.trip_id::text || b.trip_id::text || a.observed_date::text)), 1000) < %s
        LIMIT %s
    """, (country, days, min_buffer, max_buffer, sample_permille, limit))
    out = []
    for (station_id, day, a_trip, a_delay, a_route,
         b_trip, b_delay, b_route, sched_arr, sched_dep) in cur.fetchall():
        arr, dep = hms_seconds(sched_arr), hms_seconds(sched_dep)
        if arr is None or dep is None:
            continue
        out.append({
            "station_id": station_id, "day": day,
            "feeder_trip": a_trip, "feeder_delay": float(a_delay), "feeder_route": a_route,
            "conn_trip": b_trip, "conn_delay": float(b_delay), "conn_route": b_route,
            "buffer_min": (dep - arr) / 60.0,
            "hour": arr // 3600,
        })
    return out


def label(row, mct_min):
    """Was the connection physically possible?

    Feeder arrives late by feeder_delay; connector leaves late by conn_delay,
    which HELPS - a delayed connector waits for you. This is the outcome, and
    nothing that computes a prediction may look at these two delays.
    """
    slack = row["buffer_min"] + row["conn_delay"] - row["feeder_delay"]
    return 1 if slack >= mct_min else 0


# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------

def fit_lognormal_mixture(x, iters=60):
    """Two-component lognormal mixture by EM, numpy only.

    The literature's core idea: train delay is generated by two processes -
    normal operation and disruption - and one lognormal cannot describe both,
    because its tail decays far too fast to represent the disrupted cases that
    actually break connections.

    Delays go negative (early trains), and log() does not, so the series is
    shifted by its minimum before fitting and shifted back when sampling.
    Returns None when there is not enough to fit, and the caller falls back.
    """
    x = np.asarray(x, dtype=float)
    if x.size < MIN_SAMPLES_FOR_FIT:
        return None
    shift = float(min(0.0, x.min())) - 1.0
    y = np.log(x - shift)
    # Start the two components deliberately apart - body and tail - rather
    # than at random, which on a heavily skewed series often collapses both
    # onto the body and wastes the mixture.
    mu = np.array([np.quantile(y, 0.35), np.quantile(y, 0.9)])
    sigma = np.array([y.std() or 1.0, (y.std() or 1.0) * 1.5])
    weight = np.array([0.8, 0.2])
    for _ in range(iters):
        with np.errstate(divide="ignore", under="ignore"):
            comp = np.stack([
                weight[k] * np.exp(-0.5 * ((y - mu[k]) / sigma[k]) ** 2)
                / (sigma[k] * math.sqrt(2 * math.pi))
                for k in range(2)])
        total = comp.sum(axis=0)
        total[total <= 0] = 1e-300
        resp = comp / total
        nk = resp.sum(axis=1)
        if np.any(nk < 1e-6):
            break
        weight = nk / y.size
        mu = (resp * y).sum(axis=1) / nk
        sigma = np.sqrt((resp * (y - mu[:, None]) ** 2).sum(axis=1) / nk)
        sigma = np.maximum(sigma, 1e-3)
    return {"mu": mu, "sigma": sigma, "weight": weight, "shift": shift}


def sample_mixture(fit, n, rng):
    k = rng.random(n) < fit["weight"][0]
    z = np.where(k,
                 rng.normal(fit["mu"][0], fit["sigma"][0], n),
                 rng.normal(fit["mu"][1], fit["sigma"][1], n))
    return np.exp(z) + fit["shift"]


def empirical_quantiles(x):
    x = np.asarray(x, dtype=float)
    if x.size == 0:
        return None
    return {q: float(np.quantile(x, q)) for q in (0.5, 0.75, 0.9, 0.95)}


# ---------------------------------------------------------------------------
# The three predictors under test
# ---------------------------------------------------------------------------

def predict_shipped(feeder_q, conn_q, buffer_min, mct_min, grid=24):
    """What api.py does today: interpolate stored quantiles, flat above p95."""
    if not feeder_q:
        return None
    pts_f = sorted(feeder_q.items())
    pts_c = sorted(conn_q.items()) if conn_q else None

    def at(pts, u):
        if u <= pts[0][0]:
            return pts[0][1]
        for (q0, v0), (q1, v1) in zip(pts, pts[1:]):
            if u <= q1:
                return v0 + (v1 - v0) * (u - q0) / (q1 - q0)
        return pts[-1][1]

    slack = buffer_min - mct_min
    made = total = 0
    for i in range(grid):
        fd = at(pts_f, (i + 0.5) / grid)
        if pts_c:
            for j in range(grid):
                cd = at(pts_c, (j + 0.5) / grid)
                total += 1
                made += (fd - cd <= slack)
        else:
            total += 1
            made += (fd <= slack)
    return min(made / total, 0.95) if total else None


def predict_mixture(feeder_fit, conn_fit, buffer_min, mct_min, rng, n=2000):
    """Monte Carlo over two fitted lognormal mixtures."""
    if feeder_fit is None:
        return None
    fd = sample_mixture(feeder_fit, n, rng)
    cd = sample_mixture(conn_fit, n, rng) if conn_fit is not None else np.zeros(n)
    return float(np.mean((fd - cd) <= (buffer_min - mct_min)))


def predict_base_rate(rate, *_args, **_kw):
    return rate


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def brier(preds, labels):
    p = np.asarray(preds, dtype=float)
    y = np.asarray(labels, dtype=float)
    return float(np.mean((p - y) ** 2))


def reliability(preds, labels, bins=10):
    p = np.asarray(preds, dtype=float)
    y = np.asarray(labels, dtype=float)
    out = []
    for i in range(bins):
        lo, hi = i / bins, (i + 1) / bins
        m = (p >= lo) & (p < hi if i < bins - 1 else p <= hi)
        if m.sum() == 0:
            continue
        out.append({"bin": f"{lo:.1f}-{hi:.1f}", "n": int(m.sum()),
                    "predicted": float(p[m].mean()), "observed": float(y[m].mean())})
    return out


def mct_sensitivity(rows, grid=None):
    """How the observed success rate moves with the assumed interchange time.

    NOT a fit. An earlier version of this tried to estimate interchange time
    from outcomes and it is circular by construction: the label is DEFINED
    using the interchange time, so any search drives it to zero, where every
    transfer counts as made. There is no way around that with this data.
    Interchange time is identifiable only against evidence of what passengers
    actually did, and we observe trains, not passengers.

    What can honestly be said is how sensitive the answer is to the
    assumption, which is what this reports.
    """
    grid = grid or [0, 2, 5, 8, 10, 15, 20]
    return [(m, float(np.mean([label(r, m) for r in rows]))) for m in grid]


# ---------------------------------------------------------------------------
# The experiment
# ---------------------------------------------------------------------------

def run(country="DE", days=21, limit=400000, holdout_frac=0.3, seed=0,
        mct=DEFAULT_MCT_MIN, sample_permille=40):
    rng = np.random.default_rng(seed)
    conn = connect()
    cur = conn.cursor()
    try:
        rows = load_transfers(cur, country, days, limit,
                              sample_permille=sample_permille)
    finally:
        cur.close()
        conn.close()
    if not rows:
        print(f"{country}: no transfer pairs found")
        return

    days_sorted = sorted({r["day"] for r in rows})
    if len(days_sorted) < 3:
        print(f"{country}: only {len(days_sorted)} days, not enough to split")
        return
    cut = days_sorted[int(len(days_sorted) * (1 - holdout_frac))]
    train = [r for r in rows if r["day"] < cut]
    test = [r for r in rows if r["day"] >= cut]
    stations = sorted({r["station_id"] for r in rows})
    unseen = set(np.random.default_rng(seed).permutation(stations)[: max(1, len(stations) // 5)].tolist())
    train = [r for r in train if r["station_id"] not in unseen]

    print(f"=== {country} ===")
    print(f"transfers {len(rows)}   train {len(train)}   test {len(test)}")
    print(f"days {days_sorted[0]}..{days_sorted[-1]}, split at {cut}, "
          f"{len(unseen)}/{len(stations)} stations held out")

    print(f"\ninterchange-time sensitivity (NOT fitted - see mct_sensitivity)")
    for m, rate in mct_sensitivity(train):
        print(f"   assume {m:2d} min -> {rate:.3f} of transfers succeed")
    print(f"   scoring below uses {mct:.0f} min")

    labels_test = [label(r, mct) for r in test]
    base_rate = float(np.mean([label(r, mct) for r in train])) if train else 0.5
    print(f"\nbase rate: train {base_rate:.3f}, test {np.mean(labels_test):.3f}")

    by_station, by_route = defaultdict(list), defaultdict(list)
    for r in train:
        by_station[r["station_id"]] += [r["feeder_delay"], r["conn_delay"]]
        by_route[(r["station_id"], r["feeder_route"])].append(r["feeder_delay"])
        by_route[(r["station_id"], r["conn_route"])].append(r["conn_delay"])
    q_station = {k: empirical_quantiles(v) for k, v in by_station.items()}
    q_route = {k: empirical_quantiles(v) for k, v in by_route.items()}
    m_station = {k: fit_lognormal_mixture(v) for k, v in by_station.items()
                 if len(v) >= MIN_SAMPLES_FOR_FIT}
    m_route = {k: fit_lognormal_mixture(v) for k, v in by_route.items()
               if len(v) >= MIN_SAMPLES_FOR_FIT}
    print(f"fitted mixtures: {sum(v is not None for v in m_station.values())} stations, "
          f"{sum(v is not None for v in m_route.values())} routes")

    names = ("base_rate", "buffer_rule", "shipped", "mixture")
    preds = {n: [] for n in names}
    used, buffers = [], []
    for r in test:
        fq = q_route.get((r["station_id"], r["feeder_route"])) or q_station.get(r["station_id"])
        cq = q_route.get((r["station_id"], r["conn_route"])) or q_station.get(r["station_id"])
        fm = m_route.get((r["station_id"], r["feeder_route"])) or m_station.get(r["station_id"])
        cm = m_route.get((r["station_id"], r["conn_route"])) or m_station.get(r["station_id"])
        if fq is None or fm is None:
            continue
        sp = predict_shipped(fq, cq, r["buffer_min"], mct)
        mp = predict_mixture(fm, cm, r["buffer_min"], mct, rng)
        if sp is None or mp is None:
            continue
        preds["shipped"].append(sp)
        preds["mixture"].append(mp)
        preds["base_rate"].append(base_rate)
        preds["buffer_rule"].append(1.0 if r["buffer_min"] >= mct + 5 else 0.0)
        used.append(label(r, mct))
        buffers.append(r["buffer_min"])

    if not used:
        print("no scoreable test rows")
        return
    used_a = np.array(used); buf = np.array(buffers)
    print(f"\nscored {len(used)} test transfers, observed success {used_a.mean():.3f}")
    print(f"\n{'model':14s} {'Brier':>8s}   (lower is better)")
    for n in names:
        print(f"{n:14s} {brier(preds[n], used):8.4f}")

    # The overall number is dominated by easy cases. Where it matters is a
    # tight change, so score those separately.
    print(f"\nby buffer band (the tight ones are the only ones anyone asks about)")
    print(f"  {'band':>12s} {'n':>7s} {'observed':>9s} " + " ".join(f"{n:>10s}" for n in names))
    for lo, hi in ((1, 5), (5, 10), (10, 20), (20, 60)):
        m = (buf >= lo) & (buf < hi)
        if m.sum() < 20:
            continue
        line = f"  {lo:4d}-{hi:2d} min {m.sum():7d} {used_a[m].mean():9.3f} "
        line += " ".join(f"{brier(np.array(preds[n])[m], used_a[m]):10.4f}" for n in names)
        print(line)

    for n in ("shipped", "mixture"):
        print(f"\nreliability - {n}")
        print(f"  {'bin':10s} {'n':>7s} {'predicted':>10s} {'observed':>9s}  gap")
        for b in reliability(preds[n], used):
            gap = b["predicted"] - b["observed"]
            flag = "  <-- overconfident" if gap > 0.1 else ("  <-- pessimistic" if gap < -0.1 else "")
            print(f"  {b['bin']:10s} {b['n']:7d} {b['predicted']:10.3f} {b['observed']:9.3f} {gap:+6.3f}{flag}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--country", default="DE")
    ap.add_argument("--days", type=int, default=21)
    ap.add_argument("--limit", type=int, default=400000)
    ap.add_argument("--mct", type=float, default=DEFAULT_MCT_MIN)
    ap.add_argument("--permille", type=int, default=40)
    args = ap.parse_args()
    run(args.country, args.days, args.limit, mct=args.mct,
        sample_permille=args.permille)
