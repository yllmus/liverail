#!/usr/bin/env python3
"""Silent-break monitoring for the four pollers.

A process-alive check is close to worthless here. Every serious failure this
project has actually had looked *healthy* from the outside:

  - DE's first real run resolved 0/30539 rows. The poller ran, the feed
    returned 200, and nothing reached the database, because the feed's
    stop_ids were platform-level children our stations table excludes.
  - AT silently lost ~90% of its feed for months to an unnoticed
    hafas-client result cap. Row counts stayed plausible.
  - PL lost ~5-6 hours of data on 2026-08-02 with no error anywhere.
    mkuran.pl's own feed page documents the likely cause: "the upstream
    endpoint is unstable, and the feed can remain stuck without updates
    for over an hour once or twice per day" - a stuck feed serves a
    perfectly valid 200 with perfectly valid, unchanging content.
  - PL/DE's dedup silently dropped every recurring punctual trip until
    2026-08-20, because the comparison wasn't scoped to the service day.

So the checks here are about the *data*, not the process: is it arriving,
is it moving, does it still have the shape we depend on, and does it still
resolve at the rate it used to.

Usage:
    python3 monitor.py           # DB-side checks only (cheap, every 15min)
    python3 monitor.py --deep    # also fetch every live feed (hourly)
    python3 monitor.py --json    # print the full status document

Exit codes: 0 ok, 1 warn, 2 critical. Status is always written to
monitor_status.json for /health to serve, and an alert is POSTed to
ALERT_WEBHOOK_URL (if set in .env) whenever the status is not ok.
"""
import json
import os
import sys
import time
import traceback
from datetime import datetime

import psycopg2
import requests

from config import DB_CONFIG

COUNTRIES = ("HU", "AT", "PL", "DE")
STATUS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor_status.json")

OK, WARN, CRIT = "ok", "warn", "critical"
_RANK = {OK: 0, WARN: 1, CRIT: 2}

# --- thresholds -------------------------------------------------------
# Every poller runs on a 5-minute timer, so nothing should ever be this
# stale; the slack is for a single missed run plus a slow poll.
MAX_ROW_AGE_MIN = 45
# Below this share of the same-hours-of-week baseline, volume has collapsed
# even if rows are technically still arriving. 0.2 is deliberately loose -
# real traffic swings a lot by hour and day, and a monitor that cries wolf
# gets ignored, which is worse than not having one.
LOW_VOLUME_RATIO = 0.2
MIN_ROWS_FOR_DISTRIBUTION = 20
# A feed serving content older than this has stalled upstream (see PL note).
MAX_FEED_AGE_MIN = 30
# Share of raw feed rows that must still resolve to a station+trip. These are
# floors well under the observed rates (PL ~95%, DE ~16% - DE's is low by
# design, most of gtfs.de's feed is non-rail traffic we correctly exclude),
# set to catch a collapse to ~zero rather than normal drift.
MIN_RESOLVE_RATIO = {"PL": 0.50, "DE": 0.05}


def _conn():
    return psycopg2.connect(**DB_CONFIG)


def _check(results, country, name, status, detail):
    results.append({"country": country, "check": name, "status": status, "detail": detail})


# --- DB-side checks ---------------------------------------------------

def check_freshness(cur, results):
    """Is data still arriving at all?"""
    cur.execute("""
        SELECT country_code,
               EXTRACT(EPOCH FROM (now() - MAX(actual_arrival))) / 60
        FROM delay_observations
        WHERE actual_arrival > now() - interval '2 days'
        GROUP BY country_code
    """)
    seen = dict(cur.fetchall())
    for country in COUNTRIES:
        age = seen.get(country)
        if age is None:
            _check(results, country, "freshness", CRIT, "no rows at all in the last 2 days")
        elif age > MAX_ROW_AGE_MIN:
            _check(results, country, "freshness", CRIT,
                   f"newest row is {age:.0f} min old (limit {MAX_ROW_AGE_MIN})")
        else:
            _check(results, country, "freshness", OK, f"newest row {age:.0f} min old")


def check_volume(cur, results):
    """Has the row rate collapsed against its own recent baseline?

    Compared against the same hours on the previous 7 days rather than a flat
    number, because HU/AT write every poll while PL/DE only write on change -
    a single shared threshold would be meaningless across the four."""
    for country in COUNTRIES:
        cur.execute("""
            SELECT
              COUNT(*) FILTER (WHERE actual_arrival > now() - interval '3 hours') AS recent,
              COUNT(*) FILTER (WHERE actual_arrival > now() - interval '10 days'
                               AND actual_arrival < now() - interval '1 day'
                               AND EXTRACT(HOUR FROM actual_arrival)
                                   BETWEEN EXTRACT(HOUR FROM now() - interval '3 hours')
                                       AND EXTRACT(HOUR FROM now())) AS baseline_total,
              COUNT(DISTINCT date_trunc('day', actual_arrival)) FILTER (
                    WHERE actual_arrival > now() - interval '10 days'
                      AND actual_arrival < now() - interval '1 day') AS baseline_days
            FROM delay_observations WHERE country_code = %s
        """, (country,))
        recent, baseline_total, baseline_days = cur.fetchone()
        if not baseline_days:
            _check(results, country, "volume", OK, f"{recent} rows/3h (no baseline yet)")
            continue
        expected = baseline_total / baseline_days
        if expected < 5:
            _check(results, country, "volume", OK, f"{recent} rows/3h (baseline too small to judge)")
        elif recent < expected * LOW_VOLUME_RATIO:
            _check(results, country, "volume", WARN,
                   f"{recent} rows/3h vs ~{expected:.0f} expected for this time of day")
        else:
            _check(results, country, "volume", OK, f"{recent} rows/3h (~{expected:.0f} expected)")


def check_distribution(cur, results):
    """Are the delays still plausible numbers, or has the pipeline gone
    constant/null? This is the shape DE's 0-resolution bug and a stuck feed
    both produce: rows keep arriving, but they stop meaning anything."""
    for country in COUNTRIES:
        cur.execute("""
            SELECT COUNT(*), COUNT(delay_minutes),
                   COALESCE(STDDEV_POP(delay_minutes), 0),
                   COUNT(*) FILTER (WHERE delay_minutes = 0)
            FROM delay_observations
            WHERE country_code = %s AND actual_arrival > now() - interval '3 hours'
        """, (country,))
        total, non_null, stddev, zeros = cur.fetchone()
        if total < MIN_ROWS_FOR_DISTRIBUTION:
            _check(results, country, "distribution", OK, f"only {total} rows/3h, not judged")
            continue
        if non_null == 0:
            _check(results, country, "distribution", CRIT, f"all {total} delays are NULL")
        elif non_null < total * 0.5:
            _check(results, country, "distribution", WARN,
                   f"{total - non_null}/{total} delays are NULL")
        elif float(stddev) == 0.0:
            _check(results, country, "distribution", CRIT,
                   f"all {total} delays identical - pipeline is emitting a constant")
        elif zeros == total:
            _check(results, country, "distribution", CRIT, f"all {total} delays are exactly zero")
        else:
            _check(results, country, "distribution", OK,
                   f"{total} rows/3h, stddev {float(stddev):.1f}, {zeros} exactly zero")


# --- feed-side checks (--deep) ----------------------------------------

def check_feed_hu(results):
    from api import GRAPHQL_URL, GRAPHQL_QUERY
    resp = requests.post(GRAPHQL_URL, json={"query": GRAPHQL_QUERY}, timeout=25)
    vehicles = resp.json().get("data", {}).get("vehiclePositions", [])
    if not vehicles:
        _check(results, "HU", "feed", CRIT, "live feed returned zero vehicles")
        return
    # Schema drift: these are the exact fields poller.py and get_live_hu()
    # depend on. If MÁV renames or drops one, delays quietly stop resolving.
    have_trip = sum(1 for v in vehicles if (v.get("trip") or {}).get("gtfsId"))
    have_stops = sum(1 for v in vehicles if (v.get("trip") or {}).get("stoptimes"))
    have_delay = sum(1 for v in vehicles if v.get("prevOrCurrentStop"))
    n = len(vehicles)
    missing = [name for name, got in
               (("trip.gtfsId", have_trip), ("trip.stoptimes", have_stops),
                ("prevOrCurrentStop", have_delay)) if got < n * 0.5]
    if missing:
        _check(results, "HU", "feed", CRIT, f"{n} vehicles but missing fields: {', '.join(missing)}")
    else:
        _check(results, "HU", "feed", OK, f"{n} vehicles, all expected fields present")


def check_feed_at(results):
    from austria_poller import call_radar, parse_at_vehicles
    movements = call_radar()
    if not movements:
        _check(results, "AT", "feed", CRIT, "radar returned zero movements")
        return
    trains = parse_at_vehicles(movements)
    if not trains:
        _check(results, "AT", "feed", CRIT,
               f"{len(movements)} movements but zero parsed as trains")
        return
    # The cap bug: radar.js asks for 4000 results. If the feed ever comes back
    # pinned at the requested cap, it is being truncated again and we are
    # silently missing trains.
    if len(movements) >= 4000:
        _check(results, "AT", "feed", WARN,
               f"radar returned {len(movements)} = the requested cap; feed may be truncated")
    else:
        _check(results, "AT", "feed", OK, f"{len(movements)} movements, {len(trains)} trains")


def _spread_sample(rows, size=400):
    """Evenly strided sample across the whole feed, never the first N.

    Found the hard way while writing this: sampling rows[:400] of DE's feed
    reported 0.2% resolution and fired a critical alert, because gtfs.de
    orders its ~96k entities by contributing agency - the head of the feed is
    almost entirely regional bus and tram traffic, which this project
    deliberately does not import. A biased sample makes the check itself the
    silent break it was written to catch."""
    if len(rows) <= size:
        return rows
    step = len(rows) // size
    return rows[::step][:size]


# How far below its own trailing median a country's resolve ratio may fall
# before this complains. Relative, not absolute, because 13.6% is healthy for
# Germany and catastrophic for Poland.
DECAY_RATIO = 0.70
DECAY_MIN_SAMPLES = 8


def _record_and_check_decay(results, country, ratio, detail):
    """Log the resolve ratio and compare it to this country's own history.

    The fixed floor above catches a collapse. This catches a DECAY, which is
    the shape that actually went unnoticed: Poland ran at 70.9% against a 50%
    floor for about three weeks - never alerting, while 29% of its trains went
    unrecorded, because a static import had drifted three weeks stale. The
    median is used rather than the mean so one bad poll cannot move the
    baseline, and the baseline deliberately excludes the last 6 hours so a
    sustained outage cannot quietly redefine "normal" as itself.
    """
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("""INSERT INTO resolve_history
                       (country_code, feed_rows, resolved_rows, ratio)
                       VALUES (%s, %s, %s, %s)""",
                    (country, 0, 0, float(ratio)))
        cur.execute("""
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio), count(*)
            FROM resolve_history
            WHERE country_code = %s
              AND measured_at < now() - interval '6 hours'
              AND measured_at > now() - interval '21 days'
        """, (country,))
        median, n = cur.fetchone()
        conn.commit()
    finally:
        cur.close()
        conn.close()
    if not n or n < DECAY_MIN_SAMPLES or not median:
        _check(results, country, "resolve_trend", OK,
               f"{ratio:.1%} (building baseline, {n or 0} samples)")
        return
    if ratio < median * DECAY_RATIO:
        _check(results, country, "resolve_trend", WARN,
               f"{ratio:.1%} is well below this country's usual {median:.1%} "
               f"- a static feed drifting stale looks exactly like this")
    else:
        _check(results, country, "resolve_trend", OK,
               f"{ratio:.1%} vs usual {median:.1%}")


def _check_gtfs_rt(results, country, feed, resolve_ratio_fn):
    header_age = None
    if feed.header.timestamp:
        header_age = (time.time() - feed.header.timestamp) / 60
    entities = [e for e in feed.entity if e.HasField("trip_update")]
    if not entities:
        _check(results, country, "feed", CRIT, "feed contains zero trip updates")
        return
    # A stuck upstream serves a valid 200 forever with a frozen timestamp -
    # this is the check for mkuran's documented multi-hour stalls.
    if header_age is not None and header_age > MAX_FEED_AGE_MIN:
        _check(results, country, "feed", CRIT,
               f"feed timestamp is {header_age:.0f} min old - upstream appears stuck")
    else:
        age_txt = f"{header_age:.0f} min old" if header_age is not None else "no timestamp"
        _check(results, country, "feed", OK, f"{len(entities)} trip updates, {age_txt}")
    ratio, detail = resolve_ratio_fn(entities)
    _record_and_check_decay(results, country, ratio, detail)
    floor = MIN_RESOLVE_RATIO[country]
    if ratio < floor:
        _check(results, country, "resolution", CRIT,
               f"only {ratio:.1%} of feed rows resolve (floor {floor:.0%}) - {detail}")
    else:
        _check(results, country, "resolution", OK, f"{ratio:.1%} resolve - {detail}")


def check_feed_pl(results):
    from poland_poller import fetch_feed, parse_pl_vehicles, resolve_delays
    feed = fetch_feed()

    def ratio(entities):
        rows = parse_pl_vehicles(feed)
        if not rows:
            return 0.0, "nothing parsed"
        conn = _conn()
        cur = conn.cursor()
        try:
            sample = _spread_sample(rows)
            resolved = resolve_delays(cur, sample)
        finally:
            cur.close()
            conn.close()
        return len(resolved) / len(sample), f"{len(resolved)}/{len(sample)} sampled"

    _check_gtfs_rt(results, "PL", feed, ratio)


def check_feed_de(results):
    from germany_poller import fetch_feed, parse_de_vehicles, resolve_delays, load_parent_map
    feed = fetch_feed()

    def ratio(entities):
        rows = parse_de_vehicles(feed)
        if not rows:
            return 0.0, "nothing parsed"
        parent_map = load_parent_map()
        conn = _conn()
        cur = conn.cursor()
        try:
            sample = _spread_sample(rows)
            resolved = resolve_delays(cur, sample, parent_map=parent_map)
        finally:
            cur.close()
            conn.close()
        return len(resolved) / len(sample), f"{len(resolved)}/{len(sample)} sampled"

    _check_gtfs_rt(results, "DE", feed, ratio)


DEEP_CHECKS = {"HU": check_feed_hu, "AT": check_feed_at, "PL": check_feed_pl, "DE": check_feed_de}


# --- alerting ---------------------------------------------------------

def send_alert(status, failures):
    """POST to whatever webhook is configured. Deliberately generic: an
    ntfy.sh topic, a healthchecks.io fail URL, a Slack/Discord webhook and a
    plain endpoint all accept a POST body, so this needs no per-service code
    and no new dependency (requests is already used by every poller)."""
    url = os.environ.get("ALERT_WEBHOOK_URL", "").strip()
    if not url:
        return "no ALERT_WEBHOOK_URL configured, alert not sent"
    lines = [f"[{f['status'].upper()}] {f['country']} {f['check']}: {f['detail']}" for f in failures]
    body = f"LiveRail pollers: {status.upper()}\n" + "\n".join(lines)
    try:
        requests.post(url, data=body.encode("utf-8"), timeout=15,
                      headers={"Title": f"LiveRail pollers {status}", "Priority": "high"})
        return f"alert sent to {url.split('/')[2]}"
    except Exception as exc:
        return f"alert POST failed: {exc}"


def main():
    deep = "--deep" in sys.argv
    results = []
    conn = _conn()
    cur = conn.cursor()
    try:
        check_freshness(cur, results)
        check_volume(cur, results)
        check_distribution(cur, results)
    finally:
        cur.close()
        conn.close()

    if deep:
        for country, fn in DEEP_CHECKS.items():
            try:
                fn(results)
            except Exception as exc:
                # A check that throws is itself a finding - the feed changed
                # shape, the endpoint died, or a dependency broke.
                _check(results, country, "feed", CRIT,
                       f"check raised {type(exc).__name__}: {exc}")
                if "--traceback" in sys.argv:
                    traceback.print_exc()

    status = OK
    for r in results:
        if _RANK[r["status"]] > _RANK[status]:
            status = r["status"]
    failures = [r for r in results if r["status"] != OK]

    doc = {
        "status": status,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
        "deep": deep,
        "results": results,
    }
    if failures:
        doc["alert"] = send_alert(status, failures)
    with open(STATUS_PATH, "w") as f:
        json.dump(doc, f, indent=1)

    if "--json" in sys.argv:
        print(json.dumps(doc, indent=1))
    else:
        print(f"{doc['checked_at']} status={status} deep={deep}")
        for r in results:
            if r["status"] != OK or "--verbose" in sys.argv:
                print(f"  [{r['status']}] {r['country']} {r['check']}: {r['detail']}")
        if "alert" in doc:
            print(f"  {doc['alert']}")
    return _RANK[status]


if __name__ == "__main__":
    sys.exit(main())
