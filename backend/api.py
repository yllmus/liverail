from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from html import escape as _esc
from urllib.parse import quote as _urlquote
import json
import math
import os
import psycopg2
import re
import psycopg2.extras
import requests as req
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2
from austria_poller import (
    call_radar, call_trip, call_journeys, parse_at_vehicles,
    station_name_candidates as at_station_name_candidates,
    resolve_trip_id as at_resolve_trip_id,
)
from poller import resolve_trip_id as hu_resolve_trip_id
from at_route_names import extract_at_train_number
from trip_resolution import local_now, COUNTRY_TIMEZONES
import pl_planner
from poland_poller import fetch_feed, parse_pl_vehicles, resolve_delays, _scheduled_epoch
from search_text import normalize_search_text
from germany_poller import (
    fetch_feed as fetch_de_feed,
    parse_de_vehicles,
    resolve_delays as resolve_de_delays,
    load_parent_map,
    load_platform_map,
)
from station_names import clean_display_name
from hu_route_names import clean_route_name, extract_train_number
from config import DB_CONFIG
import transfer_model

@asynccontextmanager
async def _lifespan(app):
    _start_warmer()
    yield


app = FastAPI(lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

COUNTRY_ATTRIBUTION = {
    "AT": "Data: ÖBB (data.oebb.at), CC BY 4.0",
    # PL attribution: mkuran.pl publishes the realtime feed under CC BY 4.0 plus
    # PKP PLK's public sector terms, and the static feed also carries Koleje
    # Mazowieckie's terms. PKP PLK requires stating the source and when the data was
    # obtained, which is why /live/board returns retrieved_at and the frontend
    # shows it next to this text.
    "PL": (
        "Schedules: PKP Polskie Linie Kolejowe S.A. and Koleje Mazowieckie, "
        "reused as public sector information under Poland's Open Data Act "
        "(11 Aug 2021). Live delays: mkuran.pl (Mikołaj Kuranowski), "
        "CC BY 4.0, derived from PKP PLK. Provided as is, without warranty."
    ),
    "DE": "Data: DELFI e.V., via gtfs.de - schedule CC BY 4.0, realtime CC BY-SA 4.0",
}

# /routes, /routes/summary and /stations/summary aggregate a whole country's
# delay_observations. For HU and PL that is I/O bound however it is indexed (the
# table does not fit in the page cache), and these are historical views, so they
# are cached like /live: one lock per (kind, country) lets concurrent callers
# share one computation.
_SUMMARY_CACHE = {}  # (kind, country) -> {"data": ..., "expires": epoch}
_SUMMARY_TTL = 900  # 15min - all-time reliability barely shifts minute to minute
_SUMMARY_LOCKS = {}

# The three Rankings/Compare queries are the expensive ones (measured through
# nginx: PL 27.8s/34.1s, DE 25.4s/23.3s, AT 12.1s/9.2s cold), and a 15min TTL
# meant any visitor arriving after a quiet quarter-hour paid that in full - the
# Rankings tab just sat empty while they waited, which is what "Rankings is
# blank" actually was. These are all-time historical aggregates over millions
# of rows; they do not meaningfully change within a day, so they get a long
# TTL and a background thread that refreshes them before it expires. Result: a
# request should never compute one of these itself.
_WARM_TTL = 21600      # 6h
_WARM_INTERVAL = 18000  # 5h - comfortably inside _WARM_TTL, so no gap opens
_WARM_KINDS = ("routes", "stations", "routes_list")

def _cached_summary(kind, country, compute_fn, ttl=None):
    cache_key = (kind, country)
    cached = _SUMMARY_CACHE.get(cache_key)
    if cached is not None and time.time() < cached["expires"]:
        return cached["data"]
    lock = _SUMMARY_LOCKS.setdefault(cache_key, threading.Lock())
    with lock:
        cached = _SUMMARY_CACHE.get(cache_key)
        if cached is not None and time.time() < cached["expires"]:
            return cached["data"]
        data = compute_fn()
        expires = time.time() + (ttl if ttl is not None else _SUMMARY_TTL)
        _SUMMARY_CACHE[cache_key] = {"data": data, "expires": expires}
        return data

def _warm_summaries():
    """Precompute the Rankings/Compare aggregates for every country.

    Runs in a daemon thread so it never blocks startup or a request. Countries
    are done one at a time on purpose - this box has a single vCPU shared with
    four pollers, and firing 12 full-table aggregates at once would starve
    exactly the requests this is meant to speed up."""
    while True:
        for country in ("HU", "AT", "PL", "DE"):
            for kind, compute in (
                ("routes", lambda c=country: _routes_summary_query(c)),
                ("stations", lambda c=country: _stations_summary_query(c)),
                ("routes_list", lambda c=country: _routes_query(c)),
            ):
                try:
                    _cached_summary(kind, country, compute, ttl=_WARM_TTL)
                except Exception as exc:
                    print(f"summary warm {kind}/{country} failed: {exc}")
            # Each aggregate costs 49-162 s per country, so a failure here must not stop
            # the other countries or the loop. Search degrades to the last successful
            # refresh.
            #
            # Unlike the summaries above, this cache lives in Postgres and survives a
            # restart, so it is rebuilt only when stale. Without the freshness check every
            # `systemctl restart` started minutes of full-table aggregation.
            try:
                if _search_index_stale(country):
                    refresh_search_index(country)
            except Exception as exc:
                print(f"search index refresh {country} failed: {exc}")
            # The delay distributions are percentile aggregates over the whole
            # archive and expensive (459 s for AT, the smallest country). Guarded on a 24h age
            # for the same reason search_index is: it lives in Postgres, so a
            # restart must reuse it rather than spend an hour rebuilding what
            # is already there. A day-old delay distribution is not meaningfully
            # worse than a fresh one; an hour of pegged I/O on a single vCPU is.
            try:
                if _delay_distribution_stale(country):
                    refresh_delay_distribution(country)
                    # Mixtures are fitted from raw observations in Python, so
                    # they cannot ride along with the SQL percentile pass.
                    # Only when the distributions were actually rebuilt.
                    try:
                        n = fit_station_mixtures(country)
                        print(f"fitted {n} station mixtures for {country}")
                    except Exception as exc:
                        print(f"mixture fit {country} failed: {exc}")
            except Exception as exc:
                print(f"delay distribution refresh {country} failed: {exc}")
        time.sleep(_WARM_INTERVAL)

def _start_warmer():
    # Not under pytest. Importing this module in a test starts a real
    # TestClient, which fires this startup hook - and the aggregates behind it
    # cost 49-162s per country (see above), for all four, on a single vCPU
    # shared with four pollers. That turned a ~2min suite into one that ran
    # past 8 minutes and starved the live API while it did. Nothing in the
    # tests reads the warmed cache; each test computes what it needs.
    if os.environ.get("PYTEST_CURRENT_TEST") or os.environ.get("TRAINAPI_NO_WARMER"):
        return
    threading.Thread(target=_warm_summaries, daemon=True).start()

# --- Searchable name index -------------------------------------------
# Backs both /search (typo tolerance) and /sitemap.xml (thin-page exclusion)
# from one table - see schema/006_search_index.sql for why they share it.

# Minimum observations before a station or route gets its own indexable page.
# Below this there is nothing on the page worth ranking: the SEO template
# renders an on-time percentage and an average delay, and both are noise (or
# blank) under a couple of dozen samples. Measured against the real data,
# this drops 10,245 zero-observation station pages from the sitemap.
_SEO_MIN_OBSERVATIONS = 20

def _search_index_stale(country, max_age_hours=20):
    """Has this country's index gone longer than max_age_hours without a rebuild?

    Deliberately shorter than a day but far longer than the warmer's 5h loop,
    so a normally-running box refreshes on the first loop after ~20h and a
    restarted one reuses what is already in Postgres.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT max(refreshed_at) FROM search_index WHERE country_code = %s
        """, (country,))
        newest = cur.fetchone()[0]
    finally:
        cur.close()
        conn.close()
    if newest is None:
        return True
    return (datetime.now() - newest) > timedelta(hours=max_age_hours)

def refresh_search_index(country):
    """Rebuild one country's searchable names. Called from the warmer thread.

    Written as delete-then-insert inside a single transaction rather than an
    upsert: names genuinely disappear between GTFS imports, and an upsert
    would leave those rows behind forever with a stale observation count. The
    whole operation is one transaction, so a concurrent /search either sees
    the previous contents or the new ones, never a half-empty table.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        # GROUP BY name, not station id - DE has 15,731 station rows sharing
        # 8,013 names, and four identical suggestions for "Harras" is a worse
        # search result than one. See schema/006_search_index.sql. There is no
        # count fan-out here the way there is for routes below: every
        # observation has exactly one station_id, so summing across the rows
        # that share a name is the honest total for that name.
        cur.execute("""
            SELECT s.name, MIN(s.display_name), COUNT(o.id)
            FROM stations s
            LEFT JOIN delay_observations o ON o.station_id = s.id
            WHERE s.country_code = %s AND s.display_name IS NOT NULL
            GROUP BY s.name
        """, (country,))
        stations = cur.fetchall()

        # Routes are counted WITHOUT joining `routes`, deliberately. Joining
        # to pick up long_name multiplies the count by the number of route
        # rows sharing that name - HU has 11,132 route rows for 3,813 distinct
        # names, and the first version of this reported route S70 with
        # 28,840,464 observations against a whole-table total of ~15M (the
        # real figure is 97,441). A wrong count here is not cosmetic: it
        # feeds both the search ranking and the sitemap's thin-page
        # threshold. Counts and labels are therefore two independent queries,
        # merged in Python.
        cur.execute("""
            SELECT route_name, COUNT(*)
            FROM delay_observations
            WHERE country_code = %s AND route_name IS NOT NULL
            GROUP BY route_name
        """, (country,))
        route_counts = cur.fetchall()

        cur.execute("""
            SELECT name, MIN(long_name)
            FROM routes
            WHERE country_code = %s AND name IS NOT NULL AND long_name IS NOT NULL
            GROUP BY name
        """, (country,))
        long_names = dict(cur.fetchall())

        rows = [(country, "station", n, d, normalize_search_text(f"{n} {d or ''}"), c)
                for n, d, c in stations]
        rows += [(country, "route", n, long_names.get(n),
                  normalize_search_text(f"{n} {long_names.get(n) or ''}"), c)
                 for n, c in route_counts]

        cur.execute("DELETE FROM search_index WHERE country_code = %s", (country,))
        psycopg2.extras.execute_values(cur, """
            INSERT INTO search_index
                (country_code, kind, name, display_name, search_text, observations)
            VALUES %s
        """, rows, page_size=1000)
        conn.commit()
        return len(rows)
    finally:
        cur.close()
        conn.close()

# --- Journey planning ---
# Connection risk needs itineraries with transfers. Rather than write a
# connection-scan search, HU asks MÁV's own OTP endpoint (the host the HU poller
# already uses) and AT asks HAFAS journeys(). PL and DE return an explicit 501:
# they would need a search over stop_times, and an empty result would suggest
# the capability is uniform when it is not.
#
# The response shape is ours, not OTP's, so a second country does not change
# its consumers. Two corrections apply to every HU leg:
#   * route.longName is OTP's grouped slash-list, not a route identifier, so
#     clean_route_name() turns it into this train's own name.
#   * tripShortName leads with the public train number, which
#     extract_train_number() pulls out so the leg joins to our own history.
_PLAN_CACHE = {}
_PLAN_LOCKS = {}
_PLAN_TTL = 600          # 10 min: a timetable does not move, delays do
_PLAN_MAX_ITINERARIES = 6

_PLAN_QUERY = """{
  plan(from: {lat: %(flat)f, lon: %(flon)f},
       to: {lat: %(tlat)f, lon: %(tlon)f},
       date: "%(date)s", time: "%(time)s",
       numItineraries: %(n)d, transportModes: [{mode: RAIL}]) {
    itineraries {
      startTime endTime duration
      legs {
        mode startTime endTime realTime departureDelay arrivalDelay
        from { name } to { name }
        trip { gtfsId tripShortName route { longName } }
      }
    }
  }
}"""

def _plan_station(cur, country, name):
    cur.execute("""SELECT id, name, COALESCE(display_name, name), lat, lon
                   FROM stations
                   WHERE country_code = %s AND name = %s AND lat IS NOT NULL
                   LIMIT 1""", (country, name))
    row = cur.fetchone()
    if row:
        return row
    # Fall back to the search index so a rider can type a display name or a
    # near-miss, rather than needing the exact GTFS join key.
    cur.execute("""SELECT s.id, s.name, COALESCE(s.display_name, s.name), s.lat, s.lon
                   FROM stations s
                   WHERE s.country_code = %s AND s.lat IS NOT NULL
                     AND (s.display_name ILIKE %s OR s.name ILIKE %s)
                   ORDER BY length(s.name) LIMIT 1""",
                (country, f"%{name}%", f"%{name}%"))
    return cur.fetchone()

def _epoch_ms_to_local(ms, country):
    if ms is None:
        return None
    tz = ZoneInfo(COUNTRY_TIMEZONES.get(country, "Europe/Budapest"))
    return datetime.fromtimestamp(ms / 1000, tz)

def _normalise_plan(raw, country, cur):
    """OTP's itineraries -> our shape, with transfers made explicit.

    The transfer list is the whole reason this endpoint exists: transfer risk reads
    the buffer between two legs at a shared station, so it is computed here
    once rather than re-derived by every caller.
    """
    out = []
    for it in raw:
        legs = []
        for leg in it.get("legs") or []:
            trip = leg.get("trip") or {}
            short = trip.get("tripShortName")
            route_name = clean_route_name((trip.get("route") or {}).get("longName"), short)
            number = extract_train_number(short)
            dep = _epoch_ms_to_local(leg.get("startTime"), country)
            arr = _epoch_ms_to_local(leg.get("endTime"), country)
            legs.append({
                "mode": leg.get("mode"),
                "from": (leg.get("from") or {}).get("name"),
                "to": (leg.get("to") or {}).get("name"),
                "depart": dep.isoformat() if dep else None,
                "arrive": arr.isoformat() if arr else None,
                "depart_hm": dep.strftime("%H:%M") if dep else None,
                "arrive_hm": arr.strftime("%H:%M") if arr else None,
                "route_name": route_name,
                # The public number, which is what our own history is keyed on
                # for HU - not OTP's gtfsId, which is internal to their
                # instance and joins to nothing here.
                "train_number": number,
                "live_trip_id": trip.get("gtfsId"),
                "departure_delay_min": (leg.get("departureDelay") or 0) / 60.0,
                "arrival_delay_min": (leg.get("arrivalDelay") or 0) / 60.0,
                "realtime": bool(leg.get("realTime")),
            })
        # OTP inserts zero-length WALK legs between two trains at the SAME
        # station even when only RAIL is requested - confirmed in a real
        # Debrecen->Sopron response, where a Budapest-Keleti walk leg of
        # 11:52->11:52 turned one genuine 38-minute transfer into two phantom
        # ones (a 0-minute change, and a change whose feeder was the walk).
        # Transfers and the change count come from transit legs only; the walk
        # legs stay in `legs` so the itinerary is not silently rewritten.
        transit = [l for l in legs if l.get("train_number") or l.get("route_name")]
        transfers = []
        for a, b in zip(transit, transit[1:]):
            buffer_min = None
            if a["arrive"] and b["depart"]:
                buffer_min = round(
                    (datetime.fromisoformat(b["depart"]) - datetime.fromisoformat(a["arrive"])
                     ).total_seconds() / 60.0, 1)
            transfers.append({
                "station": a["to"],
                "arrive": a["arrive"], "depart": b["depart"],
                "buffer_min": buffer_min,
                "feeder_route": a["route_name"], "feeder_number": a["train_number"],
                "connector_route": b["route_name"], "connector_number": b["train_number"],
                "_arrive_hm": a.get("arrive_hm"), "_depart_hm": b.get("depart_hm"),
                # Filled per transfer from the stored distributions;
                # carries its own refusal when no rung is deep enough, so a
                # consumer can never mistake "not computed" for "no risk".
                "risk": assess_transfer(cur, country, a["to"], buffer_min,
                                        a["route_name"], b["route_name"],
                                        when=_transfer_time(a.get("arrive"), country)),
            })
        start = _epoch_ms_to_local(it.get("startTime"), country)
        end = _epoch_ms_to_local(it.get("endTime"), country)
        out.append({
            "duration_min": round((it.get("duration") or 0) / 60.0),
            "depart": start.isoformat() if start else None,
            "arrive": end.isoformat() if end else None,
            "depart_hm": start.strftime("%H:%M") if start else None,
            "arrive_hm": end.strftime("%H:%M") if end else None,
            "changes": max(0, len(transit) - 1),
            "legs": legs,
            "transfers": transfers,
        })
    return out

# --- Transfer probability ---
# P(make it) = P(feeder delay - connector departure delay <= buffer - MCT).
#
# Two simplifications, both stated: the two delays are treated as independent,
# and a train's delay at a station is read as its delay there. (The independence
# assumption turned out to matter; see docs/delay-forecasting.md.)
#
# The distributions are rebuilt from stored quantiles (schema/012: p50, p75,
# p90, p95), interpolated between them. Below p50 the curve is held flat, which
# is the conservative direction: it never inflates the chance of a connection.
#
# Minimum interchange time has no source for three of the four countries
# (transfers.txt is absent from the MÁV, ÖBB and gtfs.de feeds), so the figure
# below is an assumption, is returned as one, and is not presented as data.
_MCT_DEFAULT_MIN = 5
_MCT_LARGE_STATION_MIN = 10
# Above this many recorded observations a station is treated as "large" and
# given the longer interchange. A proxy for platform count, which no feed
# publishes - crude, and labelled as such in the response.
_MCT_LARGE_STATION_OBS = 100000

def _interchange_minutes(station_samples):
    return (_MCT_LARGE_STATION_MIN if (station_samples or 0) >= _MCT_LARGE_STATION_OBS
            else _MCT_DEFAULT_MIN)

def _quantile_curve(row_map):
    """(quantile, delay) points from a stored distribution row, ascending."""
    # `worst` is a maximum, not a percentile (at one AT station it was 106 minutes
    # out of 14,175 observations). Interpolating from p95 to it handed about 4% of
    # the mass to near-worst delays and pinned every buffer from 8 to 40 minutes at
    # 0.96. The curve therefore stops at p95 and is held flat above it. The top 5%
    # is left unmodelled, and the probability is capped and reported as such.
    pts = []
    for q, key in ((0.50, "p50"), (0.75, "p75"), (0.90, "p90"), (0.95, "p95")):
        v = row_map.get(key)
        if v is not None:
            pts.append((q, float(v)))
    return pts

def _sample_at(pts, u):
    """Inverse CDF at u in [0,1), by linear interpolation between quantiles.

    Held flat below the lowest stored quantile (p50) - see the block comment:
    the lower tail is not stored and assuming the median is the conservative
    direction.
    """
    if not pts:
        return None
    if u <= pts[0][0]:
        return pts[0][1]
    for (q0, v0), (q1, v1) in zip(pts, pts[1:]):
        if u <= q1:
            if q1 == q0:
                return v1
            return v0 + (v1 - v0) * (u - q0) / (q1 - q0)
    return pts[-1][1]

_RISK_GRID = 24  # 24x24 = 576 pairs; finer than the stored quantiles justify

def _mixture_of(row):
    """The fitted mixture on a distribution row, if it has one."""
    if not row or row.get("mix_w1") is None:
        return None
    import numpy as _np
    return {"weight": _np.array([row["mix_w1"], 1.0 - row["mix_w1"]]),
            "mu": _np.array([row["mix_mu1"], row["mix_mu2"]]),
            "sigma": _np.array([row["mix_sig1"], row["mix_sig2"]]),
            "shift": row["mix_shift"]}


def _transfer_probability_mixture(feeder, connector, buffer_min, mct_min, n=4000):
    """Monte Carlo over fitted mixtures. None when the feeder has no fit.

    Preferred over the quantile curve wherever both are available: measured on
    18,227 held-out German transfers, Brier 0.0350 against the quantile
    model's 0.0403, and without the systematic pessimism that showed in every
    reliability bin of both countries tested. See transfer_backtest.py.
    """
    f = _mixture_of(feeder)
    if f is None:
        return None
    import numpy as _np
    from transfer_backtest import sample_mixture
    rng = _np.random.default_rng(0)   # deterministic: same inputs, same answer
    fd = sample_mixture(f, n, rng)
    c = _mixture_of(connector)
    cd = sample_mixture(c, n, rng) if c is not None else _np.zeros(n)
    return float(_np.mean((fd - cd) <= (buffer_min - mct_min)))


def _transfer_probability(feeder, connector, buffer_min, mct_min):
    """Grid convolution of two reconstructed curves. None if either is absent."""
    fpts = _quantile_curve(feeder) if feeder else []
    if not fpts:
        return None
    cpts = _quantile_curve(connector) if connector else []
    slack = (buffer_min or 0) - mct_min
    made = 0
    total = 0
    for i in range(_RISK_GRID):
        fu = (i + 0.5) / _RISK_GRID
        fd = _sample_at(fpts, fu)
        if cpts:
            for j in range(_RISK_GRID):
                cu = (j + 0.5) / _RISK_GRID
                cd = _sample_at(cpts, cu)
                total += 1
                if fd - cd <= slack:
                    made += 1
        else:
            # No distribution for the connector: assume it leaves on time,
            # which is the conservative reading - a late connector only ever
            # helps the passenger.
            total += 1
            if fd <= slack:
                made += 1
    if not total:
        return None
    # Capped: the curve stops at p95, so the top 5% of outcomes are unknown
    # rather than known-good. Never claim better than 0.95.
    return min(made / total, 0.95)

# Words, not a false-precision percentage. The study is explicit: "Tight" and
# "comfortable" are more honest than "68%" when the input is three weeks of
# data. The probability is still returned for callers that want it, but the
# label is what a page should lead with.
def _risk_label(p):
    if p is None:
        return None
    if p >= 0.95:
        return "comfortable"
    if p >= 0.80:
        return "usually fine"
    if p >= 0.55:
        return "tight"
    return "unlikely"

def _dist_row_map(row):
    if not row:
        return None
    keys = ("basis", "basis_key", "sample_count", "days_seen", "p50", "p75", "p90", "p95",
            "worst", "arr_p50", "arr_p90", "dep_p50", "dep_p90", "arr_samples",
            "dep_samples", "cancellation_rate",
            "mix_w1", "mix_mu1", "mix_sig1", "mix_mu2", "mix_sig2", "mix_shift", "mix_n")
    return dict(zip(keys, row))

def resolve_station_anywhere(cur, name):
    """Find a station in ANY country, preferring whichever holds real history.

    Cross-border journeys make this necessary and make the obvious approaches
    wrong, both measured rather than assumed:

      * A foreign terminus appears in several feeds. "Budapest-Keleti" exists
        in AT's, DE's and HU's imports, and only HU's carries its 163,068
        observations - so first-match resolution is a coin flip that usually
        loses.
      * The nominally correct country is often not the best one. "Salzburg
        Hbf" matches only DE by exact name, yet Salzburg is Austrian and AT
        holds it as "Salzburg Hauptbahnhof" with 43,160 observations. And AT
        genuinely holds MORE observations at Muenchen Hbf than DE does
        (74,300 vs 12,026), because OeBB's radar covers international services
        calling there. Depth is a better guide than nationality.

    So: expand the name through the same candidate list AT's poller uses (which
    strips HAFAS's parenthetical platform qualifiers and reconciles
    Hbf/Hauptbahnhof), then rank every match across all four countries by how
    many observations that station actually has.

    Returns (country, station_id, name, display_name, observations) or None.
    """
    # HAFAS appends platform groups in two shapes and only one is
    # parenthesised: "Wien Hbf (Bahnsteige 3-12)" the poller's helper already
    # strips, but "München Hbf Gl.5-10" it does not - and that one appeared on
    # the very first cross-border journey tested. Strip the bare form too,
    # here rather than in station_name_candidates(), which four pollers depend
    # on and which should not change shape for a planner's benefit.
    bare = re.sub(r"\s+(Gl\.|Gleis|Bstg\.?|Bahnsteige?)\s*[\d\s,\-–/]*$", "", name).strip()
    candidates = at_station_name_candidates(name)
    if bare and bare != name:
        for extra in at_station_name_candidates(bare):
            if extra not in candidates:
                candidates.append(extra)
    # gtfs.de prefixes interchange stations with their urban-rail modes:
    # Berlin's main station is "S+U Berlin Hauptbahnhof", not "Berlin Hbf",
    # and 28 DE stations are named this way. Nothing HAFAS returns carries
    # that prefix, so without adding it here Berlin Hbf resolved to AT's
    # thin copy (4,365 observations) instead of DE's own (7,030) - or, for a
    # station only Germany has, to nothing at all. Both directions are
    # generated: strip the prefix off an incoming name, and add it to one.
    for c in list(candidates):
        stripped = re.sub(r"^(S\+U|U\+S|S|U)\s+", "", c).strip()
        if stripped and stripped != c and stripped not in candidates:
            candidates.append(stripped)
        for prefix in ("S+U ", "S ", "U "):
            pref = prefix + c
            if pref not in candidates:
                candidates.append(pref)
    cur.execute("""
        SELECT s.country_code, s.id, s.name, COALESCE(s.display_name, s.name),
               COALESCE(d.sample_count, 0) AS obs
        FROM stations s
        LEFT JOIN delay_distribution d
          ON d.station_id = s.id AND d.scope = 'station'
         AND d.country_code = s.country_code
        WHERE s.name = ANY(%s)
        ORDER BY obs DESC, array_position(%s, s.name)
        LIMIT 1
    """, (candidates, candidates))
    exact = cur.fetchone()
    # Do not return an exact match immediately: it can be the worst answer. DE has
    # a station literally named "Bohumin" with no observations, while PL holds
    # "Bohumín" with 19,244. Try both routes and let observation depth decide.
    #
    # Fall back to the folded form: HAFAS returns ASCII-stripped names for stations
    # outside Austria ("Krakow" for "Kraków"), so no spelling variant can match
    # them. search_index already stores that folding, trigram-indexed, with an
    # observation count to rank by.
    folded = normalize_search_text(name)
    if not folded:
        return exact
    cur.execute("""
        SELECT s.country_code, s.id, s.name, COALESCE(s.display_name, s.name),
               si.observations
        FROM search_index si
        JOIN stations s ON s.country_code = si.country_code AND s.name = si.name
        WHERE si.kind = 'station' AND si.search_text LIKE %s
        ORDER BY si.observations DESC
        LIMIT 1
    """, (folded + "%",))
    fuzzy = cur.fetchone()
    if exact and fuzzy:
        return exact if (exact[4] or 0) >= (fuzzy[4] or 0) else fuzzy
    return exact or fuzzy

def _transfer_time(iso_str, country):
    """The moment a transfer happens, for the model's hour/weekday features.

    Falls back to now in the station's own timezone. This box runs UTC and
    GTFS times are local wall-clock, a mismatch that has already produced one
    real bug in this project (trip_resolution.local_now exists because of it),
    so the fallback goes through the same helper rather than datetime.now().
    """
    if iso_str:
        try:
            return datetime.fromisoformat(iso_str)
        except (TypeError, ValueError):
            pass
    return local_now(country)


def assess_transfer(cur, country, station_name, buffer_min,
                    feeder_route=None, connector_route=None, min_sample=None,
                    resolved=None, when=None):
    """Risk for one transfer, or an explicit refusal with the reason."""
    # AT's HAFAS names and its own GTFS names disagree on formatting for a
    # real chunk of stations - "Wörgl Hbf" vs "Wörgl Hauptbahnhof", trailing
    # " Bahnhof", parenthetical platform qualifiers. Confirmed here rather than
    # assumed: a Bregenz->Graz plan produced four transfers, three of which
    # resolved to no station at all under an exact match. The poller already
    # solves this with station_name_candidates(); the same list is used here so
    # a transfer is not silently unassessable for a station we do hold.
    if resolved is not None:
        country, station_id, _n, station_display = (
            resolved[0], resolved[1], resolved[2], resolved[3])
    else:
        names = at_station_name_candidates(station_name) if country == "AT" else [station_name]
        cur.execute("""SELECT id, COALESCE(display_name, name) FROM stations
                       WHERE country_code = %s AND name = ANY(%s)
                       ORDER BY array_position(%s, name) LIMIT 1""",
                    (country, names, names))
        srow = cur.fetchone()
        if not srow:
            return {"assessed": False, "reason": f"No station {station_name!r} on record."}
        station_id, station_display = srow

    feeder = _dist_row_map(lookup_delay_distribution(
        cur, country, station_id, None, feeder_route, min_sample))
    connector = _dist_row_map(lookup_delay_distribution(
        cur, country, station_id, None, connector_route, min_sample))

    # The station rung doubles as the size proxy for interchange time.
    station_all = _dist_row_map(lookup_delay_distribution(cur, country, station_id, None, None, 1))
    mct = _interchange_minutes(station_all.get("sample_count") if station_all else 0)

    if not feeder:
        # Suppressed entirely rather than computed from the connector alone:
        # the feeder's delay is the term that decides this, and a figure
        # missing it would be a guess wearing a number.
        return {"assessed": False, "station": station_name, "station_display": station_display,
                "buffer_min": buffer_min, "interchange_min": mct,
                "reason": ("No delay distribution deep enough for the arriving train at this "
                           "station yet. Stop-level history began on 21 August 2026.")}

    # Three rungs, best first, each falling through to the next rather than
    # guessing: the promoted model, the fitted mixture, the raw quantile curve.
    #
    # The champion goes first because it is the only one of the three that has
    # been MEASURED against the others on held-out days - DE's beats the
    # mixture 0.1434 to 0.1540 on tight transfers, and is three times better
    # calibrated (0.0075 vs 0.0231). It answers only for a country that has a
    # promoted model and only for stations it actually trained on; everywhere
    # else it returns None and costs nothing. See transfer_model.py.
    p, basis_model = None, None
    if buffer_min is not None:
        # Hour and weekday are features, so the model needs the time the
        # transfer happens. Callers inside /plan have it exactly; a bare
        # /transfer/risk call does not, and "now" in the station's own timezone
        # is the honest stand-in rather than a silent zero.
        at = when or local_now(country)
        p = transfer_model.predict(country, station_id, feeder_route,
                                   connector_route, buffer_min, at)
        if p is not None:
            basis_model = "lgbm"
    if p is None:
        p = _transfer_probability_mixture(feeder, connector, buffer_min, mct)
        basis_model = "mixture"
    if p is None:
        p = _transfer_probability(feeder, connector, buffer_min, mct)
        basis_model = "quantiles"
    if p is not None:
        # A cancelled feeder misses the connection whatever the buffer, and the
        # delay distribution knows nothing about that - cancelled rows carry a
        # NULL delay and are excluded from the percentiles by construction. So
        # the delay-based probability is conditional on the train running, and
        # has to be multiplied by the chance that it does.
        #
        # Without this the mixture happily returned exactly 1.0 for a
        # thirty-minute buffer, which is never true of a railway.
        run_rate = 1.0 - float(feeder.get("cancellation_rate") or 0.0)
        p = max(0.0, min(1.0, p * run_rate))
        # And never claim certainty. Disruption this model cannot see -
        # a strike, a line closure, a missed platform announcement - is not
        # in any delay distribution.
        p = min(p, 0.99)
    return {
        "assessed": True,
        "station": station_name,
        # Which country's records answered. On a cross-border journey this is
        # not always the country the station sits in - see
        # resolve_station_anywhere - and a rider deserves to know whose data
        # this is rather than have it implied by the itinerary.
        "data_country": country,
        "station_display": station_display,
        "buffer_min": buffer_min,
        # Flagged as an assumption every time it is returned, because it is one.
        "interchange_min": mct,
        "interchange_is_assumption": True,
        "interchange_note": ("No feed publishes a minimum interchange time for this country "
                             "(transfers.txt is absent from MÁV's, ÖBB's and both gtfs.de "
                             "feeds), so this is a documented default, not measured data."),
        "probability": round(p, 3) if p is not None else None,
        "label": _risk_label(p),
        # The stored curve stops at p95, so nothing above that is modelled and
        # the probability can never read better than 0.95. Said out loud so a
        # ceiling is not mistaken for a certainty.
        # Which curve answered. The quantile fallback stops at p95 and cannot
        # report better than 0.95; the mixture models the tail and has no such
        # ceiling.
        "model": basis_model,
        # Only the raw quantile curve stops at p95. The mixture models the
        # tail, and the champion learned it from outcomes.
        "tail_unmodelled_above_pct": 95 if basis_model == "quantiles" else None,
        # Provenance on both sides: which ladder rung answered and how deep it
        # was. A risk figure without these is not checkable.
        "feeder_basis": feeder.get("basis"), "feeder_samples": feeder.get("sample_count"),
        "feeder_days": feeder.get("days_seen"), "feeder_p50": feeder.get("p50"),
        "feeder_p90": feeder.get("p90"),
        "connector_basis": connector.get("basis") if connector else None,
        "connector_samples": connector.get("sample_count") if connector else None,
        "connector_assumed_on_time": connector is None,
        "cancellation_rate": feeder.get("cancellation_rate"),
    }

# When a change is tight, a warning alone is not actionable, so offer the next
# train that also reaches the destination, with its own risk figure.
#
# Only the later connector is offered, not an earlier feeder: it changes one
# leg downstream, which is the decision available to someone on a platform
# watching their feeder run late. It is answered from our own stop_times, so it
# needs no call to anyone's planner.
_SAFER_MAX_OFFERS = 2
_SAFER_LOOKAHEAD_HOURS = 3

def safer_connectors(cur, country, station_name, dest_name, feeder_arrive_hm,
                     current_depart_hm, service_date):
    """Later departures from this station that still reach the destination."""
    if not (station_name and dest_name and current_depart_hm and feeder_arrive_hm):
        return []
    dow = pl_planner._DOW[service_date.weekday()]
    hi_h = int(current_depart_hm[:2]) + _SAFER_LOOKAHEAD_HOURS
    hi = f"{hi_h:02d}{current_depart_hm[2:]}:59"
    cur.execute(f"""
        SELECT r.name, t.trip_short_name, st.scheduled_departure
        FROM stop_times st
        JOIN trips t ON t.id = st.trip_id
        LEFT JOIN routes r ON r.id = t.route_id
        JOIN stations s ON s.id = st.station_id
        JOIN stop_times d ON d.trip_id = t.id AND d.stop_sequence > st.stop_sequence
        JOIN stations ds ON ds.id = d.station_id
        WHERE t.country_code = %s AND s.name = %s AND ds.name = %s
          AND st.scheduled_departure > %s AND st.scheduled_departure <= %s
          AND NOT EXISTS (SELECT 1 FROM service_date sd
                          WHERE sd.country_code = t.country_code AND sd.service_id = t.service_id
                            AND sd.service_date = %s AND NOT sd.added)
          AND (EXISTS (SELECT 1 FROM service_calendar sc
                       WHERE sc.country_code = t.country_code AND sc.service_id = t.service_id
                         AND sc.{dow}
                         AND (sc.start_date IS NULL OR sc.start_date <= %s)
                         AND (sc.end_date   IS NULL OR sc.end_date   >= %s))
               OR EXISTS (SELECT 1 FROM service_date sd
                          WHERE sd.country_code = t.country_code AND sd.service_id = t.service_id
                            AND sd.service_date = %s AND sd.added))
        ORDER BY st.scheduled_departure
        LIMIT %s
    """, (country, station_name, dest_name, current_depart_hm + ":00", hi,
          service_date, service_date, service_date, service_date, _SAFER_MAX_OFFERS * 4))
    seen, out = set(), []
    arrive_sec = pl_planner.hms_to_seconds(feeder_arrive_hm + ":00")
    for route_name, short, dep in cur.fetchall():
        label = route_name or short
        if not label or label in seen:
            continue
        seen.add(label)
        dep_sec = pl_planner.hms_to_seconds(dep)
        if dep_sec is None or arrive_sec is None:
            continue
        buffer_min = round((dep_sec - arrive_sec) / 60.0, 1)
        risk = assess_transfer(cur, country, station_name, buffer_min, None, route_name)
        out.append({
            "route_name": route_name, "train_number": short,
            "depart_hm": pl_planner.seconds_to_hm(dep_sec),
            "buffer_min": buffer_min,
            "label": risk.get("label") if risk.get("assessed") else None,
            "probability": risk.get("probability") if risk.get("assessed") else None,
        })
        if len(out) >= _SAFER_MAX_OFFERS:
            break
    return out


def _attach_safer(cur, country, transfers, dest_name, service_date):
    """Offer alternatives only where they are actually needed.

    Deliberately not on every transfer: a "comfortable" change with two
    suggestions attached invites second-guessing a decision that is already
    fine, and each offer costs a query plus a risk assessment.
    """
    for t in transfers:
        risk = t.get("risk") or {}
        if not risk.get("assessed") or risk.get("label") not in ("tight", "unlikely"):
            continue
        arrive_hm = t.get("_arrive_hm")
        depart_hm = t.get("_depart_hm")
        try:
            t["alternatives"] = safer_connectors(
                cur, country, t.get("station"), dest_name, arrive_hm, depart_hm, service_date)
        except Exception as exc:
            # An alternative that cannot be computed must not take the
            # itinerary down with it - the warning itself is still useful.
            print(f"safer_connectors failed at {t.get('station')}: {exc}")
            t["alternatives"] = []

# --- Projecting where a running train ends up ---
# A late train does not stay that late. Over 64,095 fresh adjacent-stop
# segments, delay change per stop depends on how late the train already is:
#
#     early        +0.340     0-1 min   +0.096     1-3 min   -0.158 (flips)
#     3-10 min     -0.201     10-30     -0.199     30+       -0.780
#
# Small delays grow and large ones are absorbed, which is timetable padding at
# work, so carrying the current delay forward unchanged is wrong in a
# predictable direction.
#
# Germany only, because of the data: a rate needs the same train at consecutive
# stops close in time, and HU, AT and PL have almost no such segments. Rather
# than borrow Germany's numbers for networks that may behave differently, the
# other three get no projection.
_RECOVERY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recovery_rates.json")
_RECOVERY_RATES = None


def _recovery_rates(country):
    global _RECOVERY_RATES
    if _RECOVERY_RATES is None:
        try:
            with open(_RECOVERY_PATH) as fh:
                _RECOVERY_RATES = json.load(fh)
        except Exception:
            _RECOVERY_RATES = {}
    return _RECOVERY_RATES.get(country)


def project_arrival_delay(country, current_delay, stops_remaining):
    """Where this train probably ends up, given where it is now.

    Applied stop by stop rather than multiplied out, because the rate depends
    on the delay and the delay changes as it is applied - a train recovering
    from 12 minutes crosses into a different band on the way down.
    """
    rates = _recovery_rates(country)
    if rates is None or current_delay is None or not stops_remaining:
        return None
    bands = sorted((v[0], v[1], v[2]) for v in rates.values())
    d = float(current_delay)
    for _ in range(int(min(stops_remaining, 60))):
        step = next((r for lo, hi, r in bands if lo <= d < hi), None)
        if step is None:
            break
        d += step
    return round(d, 1)


# --- Journey reliability: which departure should I take? ---
# The planner orders itineraries by time. A rider wants the one likely to work,
# so each leg's history from delay_distribution is attached. Two figures:
#   expected_delay_min  the median outcome
#   on_time_chance      P(within ON_TIME_MIN of schedule)
#
# The final leg decides the arrival, and the transfer risks from /transfer/risk
# are multiplied in, because a journey you cannot complete is not on time.
ON_TIME_MIN = 5.0
RELIABILITY_MIN_DAYS = 5


def _leg_distribution(cur, country, station_name, route_name):
    resolved = resolve_station_anywhere(cur, station_name) if station_name else None
    if not resolved:
        return None
    row = lookup_delay_distribution(cur, resolved[0], resolved[1], None, route_name)
    return _dist_row_map(row)


def _rank_itineraries(itineraries):
    """Most reliable first, keeping unassessed ones in timetable order behind.

    An itinerary with no history is NOT ranked as if it were poor - it is
    simply not ranked, and sorts after the ones that could be judged. Guessing
    would put a train nobody has data on below one measured at 40%, which
    would be an invention.
    """
    scored = [i for i in itineraries if (i.get("reliability") or {}).get("assessed")]
    unscored = [i for i in itineraries if not (i.get("reliability") or {}).get("assessed")]
    scored.sort(key=lambda i: (-(i["reliability"]["on_time_chance"]),
                               i.get("duration_min") or 1e9))
    return scored + unscored


def journey_reliability(cur, country, itinerary):
    """How likely is this itinerary to deliver you roughly on time?

    Deliberately conservative about saying nothing: a journey whose final leg
    has no usable history returns assessed=False rather than a number derived
    from the country average, because "we do not know" and "it is average" are
    different claims and only one of them is true.
    """
    legs = [l for l in (itinerary.get("legs") or []) if l.get("mode") != "WALK"]
    if not legs:
        return {"assessed": False, "reason": "no rail legs"}
    last = legs[-1]
    dist = _leg_distribution(cur, country, last.get("to"), last.get("route_name"))
    if not dist or (dist.get("days_seen") or 0) < RELIABILITY_MIN_DAYS:
        return {"assessed": False,
                "reason": ("No arrival history yet for the last leg of this journey. "
                           "Reliability needs a few weeks of the same train.")}

    import numpy as _np
    mix = _mixture_of(dist)
    if mix is not None:
        from transfer_backtest import sample_mixture
        draws = sample_mixture(mix, 4000, _np.random.default_rng(0))
        on_time = float(_np.mean(draws <= ON_TIME_MIN))
        expected = float(_np.median(draws))
        basis_model = "mixture"
    else:
        # Percentile fallback: p50 is the expected outcome, and the share of
        # stored quantiles at or under the threshold is a coarse on-time rate.
        qs = [dist.get(k) for k in ("p50", "p75", "p90", "p95")]
        qs = [q for q in qs if q is not None]
        if not qs:
            return {"assessed": False, "reason": "no usable distribution"}
        expected = float(dist.get("p50") or 0.0)
        on_time = sum(1 for q in qs if q <= ON_TIME_MIN) / len(qs)
        basis_model = "quantiles"

    # A journey you cannot complete is not an on-time journey.
    for t in itinerary.get("transfers") or []:
        r = (t.get("risk") or {})
        if r.get("assessed") and r.get("probability") is not None:
            on_time *= r["probability"]

    cancel = float(dist.get("cancellation_rate") or 0.0)
    on_time *= (1.0 - cancel)
    on_time = max(0.0, min(on_time, 0.99))

    if on_time >= 0.85:
        label = "reliable"
    elif on_time >= 0.65:
        label = "usually fine"
    elif on_time >= 0.40:
        label = "risky"
    else:
        label = "unreliable"
    return {
        "assessed": True,
        "on_time_chance": round(on_time, 3),
        "on_time_within_min": ON_TIME_MIN,
        "expected_delay_min": round(expected, 1),
        "label": label,
        "model": basis_model,
        "basis": dist.get("basis"),
        "samples": dist.get("sample_count"),
        "days": dist.get("days_seen"),
        "cancellation_rate": round(cancel, 4),
        "data_country": country,
    }


@app.get("/transfer/risk")
def get_transfer_risk(country: str = "HU", station: str = "", buffer_min: float = 0,
                      feeder_route: str = "", connector_route: str = "",
                      min_sample: int = None):
    """Odds of making one transfer."""
    country = country.upper()
    if not station:
        raise HTTPException(status_code=400, detail="station is required")
    conn = get_conn()
    cur = conn.cursor()
    try:
        result = assess_transfer(cur, country, station, buffer_min,
                                 feeder_route or None, connector_route or None, min_sample)
    finally:
        cur.close()
        conn.close()
    result["country"] = country
    return result

def _normalise_plan_at(raw, country, cur):
    """ÖBB's journeys() -> the same shape /plan returns for Hungary.

    Deliberately produces the identical structure rather than a near-miss:
    the frontend, the transfer risk call and every future consumer are shared,
    and a second shape would mean every one of them growing a per-country
    branch. Differences from OTP that have to be absorbed here: ISO timestamps
    instead of epoch milliseconds, `line` instead of a route long_name, an
    explicit `walking` flag instead of a mode string, and fahrtNr carrying the
    train number outright (which radar() never fills - see austria_poller).
    """
    out = []
    for j in raw or []:
        legs = []
        for leg in j.get("legs") or []:
            dep = _parse_iso(leg.get("departure"))
            arr = _parse_iso(leg.get("arrival"))
            walking = leg.get("walking")
            line = leg.get("line")
            legs.append({
                "mode": "WALK" if walking else "RAIL",
                "from": leg.get("from"), "to": leg.get("to"),
                "depart": dep.isoformat() if dep else None,
                "arrive": arr.isoformat() if arr else None,
                "depart_hm": dep.strftime("%H:%M") if dep else None,
                "arrive_hm": arr.strftime("%H:%M") if arr else None,
                "route_name": None if walking else line,
                # fahrtNr where HAFAS gives it, else pulled from the line name
                # by the same function the AT poller resolves trips with, so a
                # leg is joinable to our history either way.
                "train_number": None if walking else (
                    leg.get("fahrtNr") or extract_at_train_number(line)),
                "live_trip_id": leg.get("tripId"),
                "departure_delay_min": (leg.get("departureDelay") or 0) / 60.0,
                "arrival_delay_min": (leg.get("arrivalDelay") or 0) / 60.0,
                "realtime": leg.get("departureDelay") is not None,
                "cancelled": bool(leg.get("cancelled")),
            })
        transit = [l for l in legs if l["mode"] != "WALK"]
        transfers = []
        for a, b in zip(transit, transit[1:]):
            buffer_min = None
            if a["arrive"] and b["depart"]:
                buffer_min = round((datetime.fromisoformat(b["depart"])
                                    - datetime.fromisoformat(a["arrive"])).total_seconds() / 60.0, 1)
            transfers.append({
                "station": a["to"], "arrive": a["arrive"], "depart": b["depart"],
                "buffer_min": buffer_min,
                "feeder_route": a["route_name"], "feeder_number": a["train_number"],
                "connector_route": b["route_name"], "connector_number": b["train_number"],
                "_arrive_hm": a.get("arrive_hm"), "_depart_hm": b.get("depart_hm"),
                "risk": assess_transfer(cur, country, a["to"], buffer_min,
                                        a["route_name"], b["route_name"],
                                        when=_transfer_time(a.get("arrive"), country)),
            })
        start = _parse_iso((legs[0] or {}).get("depart")) if legs else None
        end = _parse_iso((legs[-1] or {}).get("arrive")) if legs else None
        out.append({
            "duration_min": round((end - start).total_seconds() / 60.0) if (start and end) else None,
            "depart": start.isoformat() if start else None,
            "arrive": end.isoformat() if end else None,
            "depart_hm": start.strftime("%H:%M") if start else None,
            "arrive_hm": end.strftime("%H:%M") if end else None,
            "changes": max(0, len(transit) - 1),
            "legs": legs, "transfers": transfers,
        })
    return out

def _normalise_plan_pl(cur, origin_id, destination_id, service_date, from_sec, n,
                       country="PL"):
    """Poland: search our own imported timetable, then emit the shared shape.

    The only country of the four planned from data we hold rather than an
    operator's planner - MÁV and ÖBB have one to call, Germany's is shut down
    and mkuran.pl is a data mirror. This is a connection scan over stop_times;
    see pl_planner.py for the algorithm and what it deliberately omits.

    Later departures are produced by re-scanning from just after each previous
    itinerary's first departure, which is the cheap way to get alternatives out
    of an earliest-arrival search: the connection list is loaded once and every
    extra scan is a few hundredths of a second over it.
    """
    conns = pl_planner.load_connections(cur, country, service_date, from_sec)
    seen, itineraries, cursor_sec = set(), [], from_sec
    for _ in range(n):
        chain = pl_planner.scan(conns, origin_id, destination_id, cursor_sec)
        if not chain:
            break
        legs_raw = pl_planner.chain_to_legs(chain)
        if not legs_raw:
            break
        key = tuple((l["trip_id"], l["depart_sec"]) for l in legs_raw)
        if key in seen:
            break
        seen.add(key)
        cursor_sec = legs_raw[0]["depart_sec"] + 60
        ids = {l["from_station"] for l in legs_raw} | {l["to_station"] for l in legs_raw}
        cur.execute("""SELECT id, name, COALESCE(display_name, name) FROM stations
                       WHERE id = ANY(%s)""", (list(ids),))
        names = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        legs = []
        for l in legs_raw:
            frm = names.get(l["from_station"], (None, None))
            to = names.get(l["to_station"], (None, None))
            legs.append({
                "mode": "RAIL", "from": frm[0], "to": to[0],
                "depart": None, "arrive": None,
                "depart_hm": pl_planner.seconds_to_hm(l["depart_sec"]),
                "arrive_hm": pl_planner.seconds_to_hm(l["arrive_sec"]),
                "route_name": l["route_name"],
                "train_number": l.get("trip_short_name"),
                "live_trip_id": None,
                "departure_delay_min": 0.0, "arrival_delay_min": 0.0,
                # Timetable only. Saying so matters: HU and AT carry live
                # delays on their legs and PL's would otherwise read as
                # "running exactly on time" rather than "not known".
                "realtime": False,
            })
        transfers = []
        for a, b in zip(legs_raw, legs_raw[1:]):
            station_name = names.get(a["to_station"], (None, None))[0]
            buffer_min = round((b["depart_sec"] - a["arrive_sec"]) / 60.0, 1)
            transfers.append({
                "station": station_name,
                "arrive": None, "depart": None, "buffer_min": buffer_min,
                "feeder_route": a["route_name"], "feeder_number": a.get("trip_short_name"),
                "connector_route": b["route_name"], "connector_number": b.get("trip_short_name"),
                "_arrive_hm": pl_planner.seconds_to_hm(a["arrive_sec"]),
                "_depart_hm": pl_planner.seconds_to_hm(b["depart_sec"]),
                "risk": assess_transfer(cur, country, station_name, buffer_min,
                                        a["route_name"], b["route_name"]),
            })
        itineraries.append({
            "duration_min": round((legs_raw[-1]["arrive_sec"] - legs_raw[0]["depart_sec"]) / 60.0),
            "depart": None, "arrive": None,
            "depart_hm": pl_planner.seconds_to_hm(legs_raw[0]["depart_sec"]),
            "arrive_hm": pl_planner.seconds_to_hm(legs_raw[-1]["arrive_sec"]),
            "changes": max(0, len(legs_raw) - 1),
            "legs": legs, "transfers": transfers,
        })
    return itineraries

def _parse_iso(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(v)
    except ValueError:
        return None

# Which countries can plan at all, and how. A registry rather than an if/elif
# chain for the same reason _LIVE_PROVIDERS is one: adding a country should be
# an entry, not another copy of the surrounding plumbing.
# "INT" is the cross-border planner. It is a country code only in the sense
# that the endpoint takes one; what it really selects is "plan this with ÖBB's
# HAFAS and resolve every station against all four countries at once".
_PLAN_PROVIDERS = {"HU": "mav-otp", "AT": "oebb-hafas", "PL": "csa-timetable",
                   "DE": "csa-timetable", "INT": "oebb-hafas-international"}

def _plan_international(cur, origin, destination, date, time_, n):
    """Cross-border journeys, planned by ÖBB's HAFAS.

    The feasibility study concluded cross-border was out of reach because our
    four national feeds share no station identifiers and no planner. That is
    true of the feeds; it is not true of ÖBB's HAFAS, which was already being
    called for Austria and turns out to plan any pair across all four countries
    and beyond - verified against Warszawa->Berlin and Praha->Budapest, where
    neither endpoint is Austrian.

    So routing is borrowed and the real work is attribution: every station is
    resolved across all four countries by observation depth
    (resolve_station_anywhere), and each transfer reports which country's
    records answered. Stations none of the four hold - Praha, and Berlin Hbf,
    which DE's import is missing - return an honest refusal instead of a
    fabricated figure.
    """
    key = ("INT", origin.lower(), destination.lower(), date, time_[:2], n)
    cached = _PLAN_CACHE.get(key)
    if cached and time.time() < cached["expires"]:
        return cached["data"]
    payload = call_journeys(origin, destination, f"{date}T{time_}:00", n)
    if not payload:
        raise HTTPException(status_code=502, detail="ÖBB's planner did not respond.")
    if payload.get("error") == "station-not-found":
        raise HTTPException(status_code=404, detail=(
            "ÖBB's planner does not know one of these stations: "
            f"from={payload.get('fromResolved')!r} to={payload.get('toResolved')!r}"))
    itineraries = []
    countries_used = set()
    for j in payload.get("journeys") or []:
        legs = []
        for leg in j.get("legs") or []:
            dep, arr = _parse_iso(leg.get("departure")), _parse_iso(leg.get("arrival"))
            walking = leg.get("walking")
            legs.append({
                "mode": "WALK" if walking else "RAIL",
                "from": leg.get("from"), "to": leg.get("to"),
                "depart": dep.isoformat() if dep else None,
                "arrive": arr.isoformat() if arr else None,
                "depart_hm": dep.strftime("%H:%M") if dep else None,
                "arrive_hm": arr.strftime("%H:%M") if arr else None,
                "route_name": None if walking else leg.get("line"),
                "train_number": None if walking else (
                    leg.get("fahrtNr") or extract_at_train_number(leg.get("line"))),
                "live_trip_id": leg.get("tripId"),
                "departure_delay_min": (leg.get("departureDelay") or 0) / 60.0,
                "arrival_delay_min": (leg.get("arrivalDelay") or 0) / 60.0,
                "realtime": leg.get("departureDelay") is not None,
                "cancelled": bool(leg.get("cancelled")),
            })
        transit = [l for l in legs if l["mode"] != "WALK"]
        transfers = []
        for a, b in zip(transit, transit[1:]):
            buffer_min = None
            if a["arrive"] and b["depart"]:
                buffer_min = round((datetime.fromisoformat(b["depart"])
                                    - datetime.fromisoformat(a["arrive"])).total_seconds() / 60.0, 1)
            resolved = resolve_station_anywhere(cur, a["to"])
            if resolved:
                risk = assess_transfer(cur, resolved[0], a["to"], buffer_min,
                                       a["route_name"], b["route_name"], resolved=resolved,
                                       when=_transfer_time(a.get("arrive"), resolved[0]))
            else:
                risk = {"assessed": False, "station": a["to"], "buffer_min": buffer_min,
                        "reason": ("This station is not in any of the four national feeds "
                                   "this site imports, so no delay history exists for it.")}
            # Only count a country once it actually produced a figure. Merely
            # resolving a station there says nothing about whose data backed
            # the answer, and listing it would overstate the coverage.
            if risk.get("assessed") and risk.get("data_country"):
                countries_used.add(risk["data_country"])
            transfers.append({
                "station": a["to"], "arrive": a["arrive"], "depart": b["depart"],
                "buffer_min": buffer_min,
                "feeder_route": a["route_name"], "feeder_number": a["train_number"],
                "connector_route": b["route_name"], "connector_number": b["train_number"],
                "risk": risk,
            })
        start = _parse_iso((legs[0] or {}).get("depart")) if legs else None
        end = _parse_iso((legs[-1] or {}).get("arrive")) if legs else None
        itineraries.append({
            "duration_min": round((end - start).total_seconds() / 60.0) if (start and end) else None,
            "depart": start.isoformat() if start else None,
            "arrive": end.isoformat() if end else None,
            "depart_hm": start.strftime("%H:%M") if start else None,
            "arrive_hm": end.strftime("%H:%M") if end else None,
            "changes": max(0, len(transit) - 1),
            "legs": legs, "transfers": transfers,
        })
    for _it in itineraries:
        _it["reliability"] = journey_reliability(cur, "INT", _it)
    itineraries = _rank_itineraries(itineraries)
    result = {
        "country": "INT",
        "origin": {"name": payload.get("fromResolved"), "display": payload.get("fromResolved")},
        "destination": {"name": payload.get("toResolved"), "display": payload.get("toResolved")},
        "date": date, "time": time_,
        "planner": _PLAN_PROVIDERS["INT"],
        # Whose history backed the risk figures, so a cross-border journey
        # cannot silently borrow one country's credibility for another's legs.
        "risk_data_countries": sorted(countries_used),
        "itineraries": itineraries,
        "attribution": COUNTRY_ATTRIBUTION.get("AT"),
    }
    _PLAN_CACHE[key] = {"data": result, "expires": time.time() + _PLAN_TTL}
    return result

@app.get("/plan")
def get_plan(country: str = "HU", origin: str = "", destination: str = "",
             date: str = None, time_: str = None, itineraries: int = 3):
    """Plan a journey. Hungary only - see the block comment above for why."""
    country = country.upper()
    if country not in _PLAN_PROVIDERS:
        raise HTTPException(
            status_code=501,
            detail=(f"Journey planning is not available for {country}. Hungary uses MÁV's "
                    "OTP and Austria uses ÖBB's HAFAS; Germany's HAFAS is shut down and "
                    "Poland's feed is a data mirror with no planning API, so both need a "
                    "connection-scan search over the imported stop_times. That is real "
                    "work, not a missing switch."))
    if not origin or not destination:
        raise HTTPException(status_code=400, detail="origin and destination are required")
    n = max(1, min(itineraries, _PLAN_MAX_ITINERARIES))
    now_local = local_now(country)
    date = date or now_local.strftime("%Y-%m-%d")
    time_ = time_ or now_local.strftime("%H:%M")
    try:
        datetime.strptime(date, "%Y-%m-%d")
        datetime.strptime(time_, "%H:%M")
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD and time HH:MM")

    conn = get_conn()
    cur = conn.cursor()
    try:
        if country == "INT":
            # No per-country lookup: the endpoints may be in different
            # countries, or in none of ours. HAFAS resolves them itself and
            # journeys.js reports what it matched.
            return _plan_international(cur, origin, destination, date, time_, n)
        frm = _plan_station(cur, country, origin)
        to = _plan_station(cur, country, destination)
        if not frm:
            raise HTTPException(status_code=404, detail=f"No station matching {origin!r}")
        if not to:
            raise HTTPException(status_code=404, detail=f"No station matching {destination!r}")
        if frm[0] == to[0]:
            raise HTTPException(status_code=400, detail="origin and destination are the same station")

        # Cached per origin/destination/hour: this is an outbound call on a
        # single-vCPU box already shared with four pollers, and the same
        # commuter pair gets asked for repeatedly.
        key = (country, frm[0], to[0], date, time_[:2], n)
        cached = _PLAN_CACHE.get(key)
        if cached and time.time() < cached["expires"]:
            return cached["data"]
        lock = _PLAN_LOCKS.setdefault(key, threading.Lock())
        with lock:
            cached = _PLAN_CACHE.get(key)
            if cached and time.time() < cached["expires"]:
                return cached["data"]
            if country in ("PL", "DE"):
                # Both planned by connection scan over our own imported
                # timetable, because neither has a planner to borrow: DB's
                # HAFAS is shut down and mkuran.pl is a data mirror. Germany
                # was blocked on two things until now - its calendar was never
                # imported (service_calendar held 0 DE rows, so no service ever
                # counted as running) and its poller sampled too few stops per
                # trip to search over. Both fixed.
                itineraries = _normalise_plan_pl(
                    cur, frm[0], to[0], datetime.strptime(date, "%Y-%m-%d").date(),
                    pl_planner.hms_to_seconds(time_ + ":00"), n, country)
            elif country == "AT":
                dep_iso = f"{date}T{time_}:00"
                # ÖBB resolves by its own station names, so pass the names we
                # hold. journeys.js reports what it matched so a wrong match is
                # visible rather than silently planning from somewhere else.
                payload = call_journeys(frm[1], to[1], dep_iso, n)
                if not payload:
                    raise HTTPException(status_code=502, detail="ÖBB's planner did not respond.")
                if payload.get("error") == "station-not-found":
                    raise HTTPException(
                        status_code=404,
                        detail=("ÖBB's planner does not know one of these stations: "
                                f"from={payload.get('fromResolved')!r} to={payload.get('toResolved')!r}"))
                itineraries = _normalise_plan_at(payload.get("journeys"), country, cur)
            else:
                q = _PLAN_QUERY % {"flat": frm[3], "flon": frm[4], "tlat": to[3], "tlon": to[4],
                                   "date": date, "time": time_ + ":00", "n": n}
                try:
                    resp = req.post(GRAPHQL_URL, json={"query": q}, timeout=25)
                    body = resp.json()
                except Exception as exc:
                    raise HTTPException(status_code=502, detail=f"MÁV's planner did not respond: {exc}")
                if body.get("errors"):
                    raise HTTPException(status_code=502,
                                        detail=f"MÁV's planner rejected the query: {body['errors'][:1]}")
                raw = ((body.get("data") or {}).get("plan") or {}).get("itineraries") or []
                itineraries = _normalise_plan(raw, country, cur)
            dest_name = to[1]
            svc_date = datetime.strptime(date, "%Y-%m-%d").date()
            for _it in itineraries:
                _attach_safer(cur, country, _it.get("transfers") or [], dest_name, svc_date)
                for _t in _it.get("transfers") or []:
                    _t.pop("_arrive_hm", None)
                    _t.pop("_depart_hm", None)
                _it["reliability"] = journey_reliability(cur, country, _it)
            # Ordered by how likely each is to work, not by departure time.
            # A timetable can only sort by clock; this is the ordering a rider
            # actually wants, and `rank_basis` says which one they are seeing.
            itineraries = _rank_itineraries(itineraries)
            result = {
                "country": country,
                "origin": {"name": frm[1], "display": frm[2]},
                "destination": {"name": to[1], "display": to[2]},
                "date": date, "time": time_,
                # Named so a consumer can tell a borrowed planner from one of
                # ours when Austria/Poland/Germany arrive by different routes.
                "planner": _PLAN_PROVIDERS[country],
                "itineraries": itineraries,
                "attribution": COUNTRY_ATTRIBUTION.get(country),
            }
            _PLAN_CACHE[key] = {"data": result, "expires": time.time() + _PLAN_TTL}
            return result
    finally:
        cur.close()
        conn.close()

# --- Precomputed delay distributions -------------------------
# See schema/012_delay_distribution.sql for the full reasoning. Short version:
# connection risk needs delay DISTRIBUTIONS, not averages, and computing a
# percentile over 15.8M rows inside a request is exactly how /routes/summary
# became a multi-second endpoint. Refreshed from the warmer thread instead.

# Below this, a cell is not a distribution - it is a handful of anecdotes.
# Callers must fall to a coarser rung rather than quote it.
_DIST_MIN_SAMPLE = 10
# Cells thinner than this are not even stored: with three ladder rungs across
# four countries this is what keeps the table from growing a row per train per
# stop per day, most of them meaningless.
_DIST_MIN_STORED = 3

# One statement per ladder rung. Kept as (scope, key-expression, group-by)
# rather than three near-identical blocks so a fourth rung is a tuple, not a
# copy-paste.
_DIST_RUNGS = (
    ("trip",    "o.trip_id::text", "o.trip_id, o.station_id", "AND o.trip_id IS NOT NULL"),
    ("route",   "o.route_name",    "o.route_name, o.station_id", "AND o.route_name IS NOT NULL"),
    ("station", "''",              "o.station_id", ""),
)

def _delay_distribution_stale(country, max_age_hours=24):
    """Has this country's distribution gone longer than max_age_hours?

    Same contract as _search_index_stale: this cache lives in Postgres, so a
    restart must reuse it. Measured cost of rebuilding one country is ~8
    minutes for the smallest of the four, so an unguarded rebuild on every
    restart would be an hour of pegged I/O on a single vCPU shared with four
    pollers - the exact failure that guard was added to prevent for search.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT max(refreshed_at) FROM delay_distribution
                       WHERE country_code = %s""", (country,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return True
        return (datetime.now() - row[0]).total_seconds() > max_age_hours * 3600
    finally:
        cur.close()
        conn.close()

def fit_station_mixtures(country, min_samples=200):
    """Fit a two-component lognormal mixture per station and store it.

    Runs after refresh_delay_distribution, on the station rung only. The
    trip and route rungs are far more numerous and far thinner; a mixture
    fitted on twenty observations is worse than the percentiles it would
    replace, and the percentiles remain as the fallback everywhere.

    Backtested before shipping (see transfer_backtest.py and
    schema/014_delay_mixture.sql): on 18,227 held-out German transfers the
    mixture scored 0.0350 against the percentile model's 0.0403, and fixed a
    systematic pessimism that appeared in every reliability bin of both
    countries tested.
    """
    from transfer_backtest import fit_lognormal_mixture
    conn = get_conn()
    cur = conn.cursor()
    written = 0
    try:
        cur.execute("""
            SELECT o.station_id, o.delay_minutes
            FROM delay_observations o
            WHERE o.country_code = %s AND o.delay_minutes IS NOT NULL
              AND NOT o.is_cancelled
              AND o.observed_date >= CURRENT_DATE - 30
        """, (country,))
        by_station = {}
        for station_id, delay in cur.fetchall():
            by_station.setdefault(station_id, []).append(float(delay))
        for station_id, values in by_station.items():
            if len(values) < min_samples:
                continue
            fit = fit_lognormal_mixture(values)
            if fit is None:
                continue
            cur.execute("""
                UPDATE delay_distribution
                SET mix_w1 = %s, mix_mu1 = %s, mix_sig1 = %s,
                    mix_mu2 = %s, mix_sig2 = %s, mix_shift = %s, mix_n = %s
                WHERE country_code = %s AND station_id = %s AND scope = 'station'
            """, (float(fit["weight"][0]), float(fit["mu"][0]), float(fit["sigma"][0]),
                  float(fit["mu"][1]), float(fit["sigma"][1]), float(fit["shift"]),
                  len(values), country, station_id))
            written += cur.rowcount
        conn.commit()
        return written
    finally:
        cur.close()
        conn.close()


def refresh_delay_distribution(country):
    """Recompute all three ladder rungs for one country. Warmer thread only."""
    conn = get_conn()
    cur = conn.cursor()
    written = 0
    try:
        cur.execute("DELETE FROM delay_distribution WHERE country_code = %s", (country,))
        for scope, key_expr, group_by, extra_where in _DIST_RUNGS:
            # percentile_cont ignores NULLs, so cancelled rows (delay_minutes
            # is NULL for them by design) correctly do not drag the percentiles
            # toward zero - they are counted in cancellation_rate instead.
            cur.execute(f"""
                INSERT INTO delay_distribution
                    (country_code, scope, scope_key, station_id,
                     sample_count, days_seen, p50, p75, p90, p95, worst,
                     arr_p50, arr_p90, dep_p50, dep_p90, arr_samples, dep_samples,
                     cancellation_rate)
                SELECT %s, %s, {key_expr}, o.station_id,
                       COUNT(o.delay_minutes),
                       COUNT(DISTINCT o.observed_date),
                       percentile_cont(0.50) WITHIN GROUP (ORDER BY o.delay_minutes),
                       percentile_cont(0.75) WITHIN GROUP (ORDER BY o.delay_minutes),
                       percentile_cont(0.90) WITHIN GROUP (ORDER BY o.delay_minutes),
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY o.delay_minutes),
                       MAX(o.delay_minutes),
                       percentile_cont(0.50) WITHIN GROUP (ORDER BY o.delay_arrival_minutes),
                       percentile_cont(0.90) WITHIN GROUP (ORDER BY o.delay_arrival_minutes),
                       percentile_cont(0.50) WITHIN GROUP (ORDER BY o.delay_departure_minutes),
                       percentile_cont(0.90) WITHIN GROUP (ORDER BY o.delay_departure_minutes),
                       COUNT(o.delay_arrival_minutes),
                       COUNT(o.delay_departure_minutes),
                       COUNT(*) FILTER (WHERE o.is_cancelled)::float / NULLIF(COUNT(*), 0)
                FROM delay_observations o
                WHERE o.country_code = %s AND o.station_id IS NOT NULL {extra_where}
                GROUP BY {group_by}
                HAVING COUNT(*) >= %s
                ON CONFLICT (country_code, scope, scope_key, station_id, hour_band, weekday_type)
                DO NOTHING
            """, (country, scope, country, _DIST_MIN_STORED))
            written += cur.rowcount
        conn.commit()
        return written
    finally:
        cur.close()
        conn.close()

def lookup_delay_distribution(cur, country, station_id, trip_id=None, route_name=None,
                              min_sample=None):
    """Walk the ladder until a rung clears the sample minimum.

    Returns the row plus which rung answered, because a figure whose
    provenance is invisible is worse than no figure - "based on this exact
    train" and "based on the station generally" are different claims and the
    caller has to be able to say which it is making. Returns None when even
    the station rung is too thin, rather than quoting a number from four
    observations.
    """
    min_sample = _DIST_MIN_SAMPLE if min_sample is None else min_sample
    ladder = []
    if trip_id is not None:
        ladder.append(("trip", str(trip_id)))
    if route_name:
        ladder.append(("route", route_name))
    ladder.append(("station", ""))
    for scope, key in ladder:
        cur.execute("""
            SELECT scope, scope_key, sample_count, days_seen, p50, p75, p90, p95, worst,
                   arr_p50, arr_p90, dep_p50, dep_p90, arr_samples, dep_samples,
                   cancellation_rate,
                   mix_w1, mix_mu1, mix_sig1, mix_mu2, mix_sig2, mix_shift, mix_n
            FROM delay_distribution
            WHERE country_code = %s AND station_id = %s AND scope = %s AND scope_key = %s
              AND hour_band IS NULL AND weekday_type IS NULL
        """, (country, station_id, scope, key))
        row = cur.fetchone()
        if row and (row[2] or 0) >= min_sample:
            return row
    return None

@app.get("/distribution")
def get_distribution(country: str = "HU", station: str = "", trip_id: int = None,
                     route_name: str = "", min_sample: int = None):
    """The delay distribution at one station, with its provenance.

    Transfer probability consumes this; it is exposed on its own so
    the numbers behind a risk figure can be inspected rather than taken on
    trust.
    """
    country = country.upper()
    if not station:
        raise HTTPException(status_code=400, detail="station is required")
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT id, COALESCE(display_name, name) FROM stations
                       WHERE name = %s AND country_code = %s LIMIT 1""", (station, country))
        srow = cur.fetchone()
        if not srow:
            raise HTTPException(status_code=404, detail=f"No station {station!r} in {country}")
        station_id, station_display = srow
        row = lookup_delay_distribution(cur, country, station_id, trip_id,
                                        route_name or None, min_sample)
    finally:
        cur.close()
        conn.close()

    base = {"country": country, "station": station, "station_display": station_display,
            "min_sample": _DIST_MIN_SAMPLE if min_sample is None else min_sample}
    if not row:
        # Explicit, not an empty object: the ladder ran and every rung was too
        # thin. That is a real answer and a different one from "no such stop".
        base.update({"found": False, "basis": None,
                     "reason": ("No rung of the ladder has enough observations yet. "
                                "Stop-level history began on 21 August 2026 and "
                                "three of the four countries hold about three weeks.")})
        return base
    keys = ("basis", "basis_key", "sample_count", "days_seen", "p50", "p75", "p90", "p95",
            "worst", "arr_p50", "arr_p90", "dep_p50", "dep_p90", "arr_samples",
            "dep_samples", "cancellation_rate",
            "mix_w1", "mix_mu1", "mix_sig1", "mix_mu2", "mix_sig2", "mix_shift", "mix_n")
    base.update({"found": True})
    base.update(dict(zip(keys, row)))
    # AT never publishes arrival delays at all (0/314 measured) - structural,
    # not a thin sample, and the caller must not read empty as "never late".
    base["arrival_available"] = country != "AT"
    return base

@app.get("/search")
def search(q: str, country: str = "HU", limit: int = 10, kind: str = None,
           scope: str = "country"):
    """Typo-tolerant station and route search.

    Replaces the frontend's client-side substring match, which required an
    exact prefix - "Budpest" or "Munchen" returned nothing at all. Ranking
    blends trigram similarity with how much data we actually hold for the
    name, so a busy main station outranks a rural halt whose name happens to
    score marginally higher.

    scope="all" searches every country at once and labels each result with the
    one it came from. The default stays country-scoped so nothing that already
    calls this changes behaviour, but the whole point of a four-country site is
    that a rider looking for Wien while reading Hungarian delays should find
    it. Cross-country results are deduplicated the same way the cross-border
    planner resolves stations - by observation depth - because a foreign
    terminus appears in several feeds and only one copy has any history.
    """
    raw = (q or "").strip()
    if not raw:
        return {"query": raw, "country": country, "results": []}
    needle = normalize_search_text(raw)
    limit = max(1, min(limit, 50))

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # word_similarity(), not similarity(): similarity() normalises over the whole
        # target, so "Wien" scored 0.125 against "Wien Hauptbahnhof" and returned
        # Gratwein-Gratkorn. similarity() stays as a GREATEST arm because it wins for
        # multi-word needles ("Warszawa Centr").
        #
        # The popularity term makes the ranking usable: "Munchen" ranked "Munchhausen"
        # (0 observations) above "München Hbf" (11,674). ln() stops one huge station
        # dominating every query, and the cap and 0.25 weight let popularity break a
        # near-tie but never outrank a better textual match.
        cur.execute("""
            SELECT kind, name, display_name, observations, country_code,
                   GREATEST(
                       word_similarity(%(q)s, search_text),
                       similarity(%(q)s, search_text)
                   ) AS score,
                   (search_text LIKE %(like)s) AS substring_hit
            FROM search_index
            WHERE (%(country)s::text IS NULL OR country_code = %(country)s)
              AND (%(kind)s::text IS NULL OR kind = %(kind)s)
              AND (
                    search_text LIKE %(like)s
                 OR %(q)s <%% search_text
                 OR %(q)s %% search_text
              )
            ORDER BY (
                       GREATEST(
                           word_similarity(%(q)s, search_text),
                           similarity(%(q)s, search_text)
                       )
                       + LEAST(ln(observations + 1) / 14.0, 1.0) * 0.25
                     ) DESC,
                     observations DESC
            LIMIT %(limit)s
        """, {"q": needle, "like": f"%{needle}%",
              "country": None if scope == "all" else country,
              "kind": kind, "limit": limit * (4 if scope == "all" else 1)})
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    if scope == "all":
        # One physical station can exist in several feeds - Budapest-Keleti is
        # in AT's, DE's and HU's - and showing it three times, twice with no
        # history behind it, is worse than useless. Keep the deepest copy,
        # which is the same rule resolve_station_anywhere applies.
        best = {}
        for r in rows:
            # "Hauptbahnhof" and "Hbf" are the same word in two feeds'
            # spellings, so fold one into the other before comparing -
            # otherwise Muenchen appears twice, as AT's "Muenchen
            # Hauptbahnhof" (72,793 observations) and DE's "Muenchen Hbf"
            # (11,691), which is one station wearing two hats.
            canon = normalize_search_text(r["display_name"] or r["name"])
            canon = canon.replace("hauptbahnhof", "hbf").replace("glowny", "gl")
            key = (r["kind"], canon)
            keep = best.get(key)
            if keep is None or (r["observations"] or 0) > (keep["observations"] or 0):
                best[key] = r
        rows = sorted(best.values(),
                      key=lambda r: (-(float(r["score"] or 0)
                                       + min((r["observations"] or 0) and
                                             math.log((r["observations"] or 0) + 1) / 14.0, 1.0) * 0.25),
                                     -(r["observations"] or 0)))[:limit]
    for r in rows:
        r["score"] = round(float(r["score"] or 0), 3)
        r["label"] = r["display_name"] or r["name"]
    return {"query": raw, "country": country, "scope": scope, "results": rows}

@app.get("/countries")
def get_countries():
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT code, name FROM countries ORDER BY name")
        rows = cur.fetchall()
        return [{**dict(r), "attribution": COUNTRY_ATTRIBUTION.get(r["code"])} for r in rows]
    finally:
        conn.close()

@app.get("/stations")
def get_stations(country: str = "HU"):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # display_name IS NOT NULL excludes confirmed non-rail stops (see
        # station_names.py's clean_display_name) - name stays the matching key
        # for /station/{name}/... lookups, display_name is for showing to a
        # rider. Not called anywhere in the current frontend, so this response
        # shape is free to change (see /stations/summary below for the
        # endpoint that isn't).
        cur.execute("""
            SELECT DISTINCT name, display_name FROM stations
            WHERE country_code = %s AND display_name IS NOT NULL
            ORDER BY display_name
        """, (country,))
        rows = cur.fetchall()
        return [{"name": r["name"], "display_name": r["display_name"]} for r in rows]
    finally:
        conn.close()

@app.get("/station/{station_name:path}/delays")
def get_station_delays(station_name: str, country: str = "HU"):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) as total,
                ROUND(AVG(delay_minutes)::numeric, 2) as avg_delay,
                ROUND(MAX(delay_minutes)::numeric, 2) as max_delay,
                ROUND(100.0 * SUM(CASE WHEN delay_minutes > 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_late
            FROM delay_observations o
            JOIN stations s ON o.station_id = s.id
            WHERE s.name = %s AND o.country_code = %s
        """, (station_name, country))
        row = cur.fetchone()
        return row
    finally:
        conn.close()

def _routes_query(country):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT DISTINCT route_name FROM delay_observations
            WHERE route_name IS NOT NULL AND country_code = %s
            ORDER BY route_name
        """, (country,))
        rows = cur.fetchall()
        return [r["route_name"] for r in rows]
    finally:
        conn.close()

@app.get("/routes")
def get_routes(country: str = "HU"):
    return _cached_summary("routes_list", country, lambda: _routes_query(country), ttl=_WARM_TTL)

def _routes_summary_query(country):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                route_name,
                COUNT(*) as total,
                ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_on_time,
                ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
            FROM delay_observations
            WHERE route_name IS NOT NULL AND country_code = %s
            GROUP BY route_name
            HAVING COUNT(*) >= 5
            ORDER BY pct_on_time DESC
        """, (country,))
        rows = cur.fetchall()
        # route_display: friendly corridor/operator name alongside the raw
        # route_name join key, additive only - same convention as
        # stations/summary's display_name (never substituted, never reordered
        # relative to the existing route_name field index.html already keys
        # click-through off). See _route_display_map for why this is real data
        # for AT/PL and legitimately absent for DE/HU.
        route_mapping = _route_display_map(country)
        out = [dict(r) for r in rows]
        for r in out:
            r["route_display"] = _display_route(route_mapping, r["route_name"])
        return out
    finally:
        conn.close()

@app.get("/routes/summary")
def get_routes_summary(country: str = "HU", format: str = None):
    rows = _cached_summary("routes", country, lambda: _routes_summary_query(country), ttl=_WARM_TTL)
    # Same cached rows either way - the export is a rendering choice, not a
    # second query path that could drift from what the page shows.
    return _csv_response(rows, f"routes-{country}") if format == "csv" else rows

@app.get("/route/{route_name:path}/delays")
def get_route_delays(route_name: str, country: str = "HU"):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) as total,
                ROUND(AVG(delay_minutes)::numeric, 2) as avg_delay,
                ROUND(MAX(delay_minutes)::numeric, 2) as max_delay,
                ROUND(100.0 * SUM(CASE WHEN delay_minutes > 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_late
            FROM delay_observations
            WHERE route_name = %s AND country_code = %s
        """, (route_name, country))
        row = cur.fetchone()
        return row
    finally:
        conn.close()

@app.get("/route/{route_name:path}/stats")
def get_route_stats(route_name: str, country: str = "HU"):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) as total,
                ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_on_time,
                ROUND(AVG(delay_minutes)::numeric, 2) as avg_delay
            FROM delay_observations
            WHERE route_name = %s AND country_code = %s
        """, (route_name, country))
        summary = cur.fetchone()
        cur.execute("""
            SELECT EXTRACT(HOUR FROM actual_arrival)::int as hour,
                   COUNT(*) as total,
                   ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
            FROM delay_observations
            WHERE route_name = %s AND country_code = %s AND actual_arrival IS NOT NULL
            GROUP BY hour ORDER BY hour
        """, (route_name, country))
        by_hour = [dict(r) for r in cur.fetchall()]
        cur.execute("""
            SELECT EXTRACT(DOW FROM actual_arrival)::int as dow,
                   COUNT(*) as total,
                   ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
            FROM delay_observations
            WHERE route_name = %s AND country_code = %s AND actual_arrival IS NOT NULL
            GROUP BY dow ORDER BY dow
        """, (route_name, country))
        by_dow = [dict(r) for r in cur.fetchall()]
        return {**dict(summary), "by_hour": by_hour, "by_dow": by_dow}
    finally:
        conn.close()

@app.get("/station/{station_name:path}/stats")
def get_station_stats(station_name: str, country: str = "HU"):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) as total,
                ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_on_time,
                ROUND(AVG(delay_minutes)::numeric, 2) as avg_delay
            FROM delay_observations o
            JOIN stations s ON o.station_id = s.id
            WHERE s.name = %s AND o.country_code = %s
        """, (station_name, country))
        summary = cur.fetchone()
        cur.execute("""
            SELECT EXTRACT(HOUR FROM actual_arrival)::int as hour,
                   COUNT(*) as total,
                   ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
            FROM delay_observations o
            JOIN stations s ON o.station_id = s.id
            WHERE s.name = %s AND o.country_code = %s AND actual_arrival IS NOT NULL
            GROUP BY hour ORDER BY hour
        """, (station_name, country))
        by_hour = [dict(r) for r in cur.fetchall()]
        cur.execute("""
            SELECT EXTRACT(DOW FROM actual_arrival)::int as dow,
                   COUNT(*) as total,
                   ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
            FROM delay_observations o
            JOIN stations s ON o.station_id = s.id
            WHERE s.name = %s AND o.country_code = %s AND actual_arrival IS NOT NULL
            GROUP BY dow ORDER BY dow
        """, (station_name, country))
        by_dow = [dict(r) for r in cur.fetchall()]
        return {**dict(summary), "by_hour": by_hour, "by_dow": by_dow}
    finally:
        conn.close()

@app.get("/heatmap")
def get_heatmap(station: str = None, route: str = None, country: str = "HU"):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if station:
            cur.execute("""
                SELECT EXTRACT(DOW FROM actual_arrival)::int as dow,
                       EXTRACT(HOUR FROM actual_arrival)::int as hour,
                       COUNT(*) as total,
                       ROUND(AVG(delay_minutes)::numeric,1) as avg_delay
                FROM delay_observations o
                JOIN stations s ON o.station_id=s.id
                WHERE s.name=%s AND o.country_code = %s AND actual_arrival IS NOT NULL
                GROUP BY dow,hour ORDER BY dow,hour
            """, (station, country))
        elif route:
            cur.execute("""
                SELECT EXTRACT(DOW FROM actual_arrival)::int as dow,
                       EXTRACT(HOUR FROM actual_arrival)::int as hour,
                       COUNT(*) as total,
                       ROUND(AVG(delay_minutes)::numeric,1) as avg_delay
                FROM delay_observations
                WHERE route_name=%s AND country_code = %s AND actual_arrival IS NOT NULL
                GROUP BY dow,hour ORDER BY dow,hour
            """, (route, country))
        else:
            return []
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def _stations_summary_query(country):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # station_name stays s.name (the live click-through lookup key index.html
        # passes straight into /station/{name}/stats) - display_name is added
        # additively for a rider-facing label, never substituted in. The
        # display_name IS NOT NULL filter is a real, live-facing improvement:
        # it keeps confirmed non-rail stops (e.g. DE's Bushaltestelle rows) off
        # this leaderboard if they ever cross the 20-observation threshold.
        cur.execute("""
            SELECT s.name as station_name, s.display_name,
                   COUNT(*) as total,
                   ROUND(100.0 * SUM(CASE WHEN o.delay_minutes <= 2 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) as pct_on_time,
                   ROUND(AVG(o.delay_minutes)::numeric, 1) as avg_delay
            FROM delay_observations o
            JOIN stations s ON o.station_id = s.id
            WHERE o.country_code = %s AND s.display_name IS NOT NULL
            GROUP BY s.name, s.display_name
            HAVING COUNT(*) >= 20
            ORDER BY pct_on_time DESC
        """, (country,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

@app.get("/stations/summary")
def get_stations_summary(country: str = "HU", format: str = None):
    rows = _cached_summary("stations", country, lambda: _stations_summary_query(country), ttl=_WARM_TTL)
    return _csv_response(rows, f"stations-{country}") if format == "csv" else rows

@app.get("/trend")
def get_trend(route: str = None, station: str = None, days: int = 30, country: str = "HU"):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cutoff = datetime.now() - timedelta(days=days)
        if route:
            cur.execute("""
                SELECT DATE(actual_arrival) as day,
                       ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay,
                       COUNT(*) as total
                FROM delay_observations
                WHERE route_name = %s AND country_code = %s AND actual_arrival >= %s
                GROUP BY day ORDER BY day
            """, (route, country, cutoff))
        elif station:
            cur.execute("""
                SELECT DATE(o.actual_arrival) as day,
                       ROUND(AVG(o.delay_minutes)::numeric, 1) as avg_delay,
                       COUNT(*) as total
                FROM delay_observations o
                JOIN stations s ON o.station_id = s.id
                WHERE s.name = %s AND o.country_code = %s AND o.actual_arrival >= %s
                GROUP BY day ORDER BY day
            """, (station, country, cutoff))
        else:
            return []
        rows = cur.fetchall()
        return [{"day": str(r["day"]), "avg_delay": float(r["avg_delay"] or 0), "total": int(r["total"])} for r in rows]
    finally:
        conn.close()

GRAPHQL_URL = "https://mavplusz.hu/otp2-backend/otp/routers/default/index/graphql"
GRAPHQL_QUERY = """
{
  vehiclePositions(
    swLat: 45.7, swLon: 16.0,
    neLat: 48.6, neLon: 22.9,
    modes: [RAIL, RAIL_REPLACEMENT_BUS, SUBURBAN_RAILWAY, TRAMTRAIN]
  ) {
    lat
    lon
    trip {
      gtfsId
      tripShortName
      tripHeadsign
      route { longName }
      stoptimes { stop { name lat lon platformCode } scheduledDeparture departureDelay scheduledArrival arrivalDelay realtimeState }
    }
    stopRelationship { stop { name lat lon platformCode } status }
    prevOrCurrentStop { arrivalDelay departureDelay realtimeState }
  }
}
"""

def _fmt_time(sec):
    if sec is None:
        return None
    h = (sec // 3600) % 24
    m = (sec % 3600) // 60
    return f"{h:02d}:{m:02d}"

# Registry-driven /live dispatch. Each non-HU country provides a zero-arg
# fetch function that returns rows already shaped for the response; adding a
# country means writing one small _live_xx() function and one entry in
# _LIVE_PROVIDERS, not another copy-pasted cache+fetch+reshape block. HU
# stays a special case outside the registry since get_live_hu() has no
# cache/TTL (see its own definition below).
_LIVE_CACHE = {}  # country_code -> {"data": [...], "expires": epoch}

def _live_result(rows, stop_key):
    # headsign, lat and lon are filled for AT, headsign only for PL, and neither
    # for DE, where .get() falls through to None.
    #
    # position_source separates AT's real GPS ("live") from PL and DE's
    # schedule-interpolated marker ("estimated"). A consumer must always be able to
    # tell which kind of position a row holds.
    return [
        {
            "route_name": row["route_name"],
            "train_number": row.get("train_number") or "",
            "headsign": row.get("headsign"),
            # Whatever identifier this country's feed uses to address one
            # specific train, passed through opaquely so /train/{country}/{id}
            # can look the same train up again. Deliberately NOT normalized
            # into a shared scheme - AT's is a HAFAS composite key, PL/DE's is
            # a GTFS trip id, and pretending they're interchangeable would
            # be a mistake.
            "train_id": row.get("live_trip_id") or row.get("gtfs_trip_id"),
            "current_stop": row[stop_key],
            "delay_minutes": row["delay_minutes"],
            "status": "CANCELLED" if row.get("is_cancelled") else "IN_TRANSIT_TO",
            "stops": [],
            "stop_idx": None,
            "lat": row.get("lat"),
            "lon": row.get("lon"),
            "position_source": row.get("position_source"),
            # A cancelled train carries delay_minutes = None, so every consumer
            # must branch on this rather than on the delay - "no delay figure"
            # and "not running" look identical otherwise. cancellation_kind
            # separates "the whole service is cancelled" from "it runs but
            # skips this stop"; see schema/005_cancellations_platform.sql.
            "is_cancelled": bool(row.get("is_cancelled")),
            "cancellation_kind": row.get("cancellation_kind"),
            "platform": row.get("platform"),
        }
        for row in rows
    ]

def _live_at():
    rows = parse_at_vehicles(call_radar())
    for row in rows:
        row["position_source"] = "live" if row.get("lat") is not None else None
    return _live_result(rows, "stop_name")

def _live_delays(rows):
    """Delay figures for the aggregate stats, cancellations excluded.

    A cancelled train has no delay (NULL, not 0). Folding it in as 0 - which
    `r["delay_minutes"] or 0` silently did - counts every cancellation as a
    punctual arrival, so the more trains an operator cancels the better its
    on-time percentage looks. Cancellations are reported as their own count
    instead.
    """
    return [r["delay_minutes"] for r in rows
            if not r.get("is_cancelled") and r.get("delay_minutes") is not None]

def _live_pl():
    conn = get_conn()
    cur = conn.cursor()
    try:
        rows = resolve_delays(cur, parse_pl_vehicles(fetch_feed()))
    finally:
        cur.close()
        conn.close()
    return _live_result(rows, "station_name")

def _live_de():
    # PL/DE's /live is schedule-vs-actual for whatever's currently in the
    # TripUpdates feed, not a vehicle-position snapshot the way HU/AT's
    # radar-style calls are - and DE's resolve rate is ~16% of the raw feed
    # (most of gtfs.de's realtime-free.pb is non-rail traffic outside DE's
    # rail-only static import, same rail-only scope as HU/AT/PL).
    conn = get_conn()
    cur = conn.cursor()
    try:
        rows = resolve_de_delays(cur, parse_de_vehicles(fetch_de_feed()))
    finally:
        cur.close()
        conn.close()
    return _live_result(rows, "station_name")

_LIVE_PROVIDERS = {
    # AT's radar() call used to be cheap (~1-2s), which is why 25s was fine.
    # Raising radar.js's results cap 256 -> 2000 (it was silently truncating
    # ~90% of the national feed) made the payload ~9x bigger and the cold
    # call ~13-16s, so a 25s TTL against the frontend's 30s auto-refresh
    # meant every single refresh tick paid the full cost. 60s, matching DE.
    "AT": (_live_at, 60),
    # PL/DE's fetch_fn is a fresh GTFS-RT pull + a per-row schedule join
    # (~30s for PL's ~22k trip updates, confirmed) - not cheap like AT's
    # radar() call. A 25s TTL (matched almost exactly to the frontend's old
    # 30s auto-refresh interval) meant this expensive path reran on nearly
    # every refresh tick. Longer TTL since the underlying data doesn't
    # actually move faster than our own poller samples it (every 5min).
    "PL": (_live_pl, 90),
    "DE": (_live_de, 60),
}
# One lock per non-HU provider so concurrent requests for the same country
# (board + map fire together on every tab switch and every 30s auto-refresh)
# don't independently duplicate the same ~30-90s PL/DE resolve - found while
# investigating the PL map timeout: two callers racing a cold cache each ran
# the full fetch+resolve concurrently, roughly doubling load on this box's
# single vCPU instead of one computing while the other waits and reuses it.
_LIVE_LOCKS = {country: threading.Lock() for country in _LIVE_PROVIDERS}

@app.get("/live")
def get_live(country: str = "HU"):
    if country == "HU":
        return get_live_hu()
    provider = _LIVE_PROVIDERS.get(country)
    if provider is None:
        return []
    fetch_fn, ttl = provider
    cached = _LIVE_CACHE.get(country)
    if cached is not None and time.time() < cached["expires"]:
        return cached["data"]
    with _LIVE_LOCKS[country]:
        # Re-check: another thread may have refreshed the cache while this
        # one waited on the lock.
        cached = _LIVE_CACHE.get(country)
        if cached is not None and time.time() < cached["expires"]:
            return cached["data"]
        data = fetch_fn()
        _LIVE_CACHE[country] = {"data": data, "expires": time.time() + ttl}
        return data

# name -> display_name per country, for turning the raw station strings the
# live feeds emit into rider-facing labels. Cached because it only changes on
# a GTFS re-import, and DE's map alone is ~7.8k rows.
_DISPLAY_CACHE = {}  # country_code -> {"map": {...}, "expires": epoch}
_DISPLAY_TTL = 3600

def _display_map(country):
    now = time.time()
    cached = _DISPLAY_CACHE.get(country)
    if cached is not None and now < cached["expires"]:
        return cached["map"]
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT name, display_name FROM stations
            WHERE country_code = %s AND display_name IS NOT NULL
        """, (country,))
        mapping = dict(cur.fetchall())
    finally:
        cur.close()
        conn.close()
    _DISPLAY_CACHE[country] = {"map": mapping, "expires": now + _DISPLAY_TTL}
    return mapping

def _display_stop(mapping, raw, country):
    # AT is the reason for the fallback: its live HAFAS names disagree with
    # its own GTFS names for most stations (3/40 matched in real data - the
    # documented name-matching limitation), so an unmatched name still gets
    # the same formatting rules applied rather than being shown raw.
    if not raw:
        return raw
    return mapping.get(raw) or clean_display_name(raw, country) or raw

# routes.name (short code) -> routes.long_name (friendly corridor/operator
# name), for the raw AT/PL line codes. Same cache shape/TTL as
# _display_map. DE's routes.long_name is confirmed 0/1061 filled (gtfs.de's
# two static feeds simply don't carry it - see schema/004_route_long_name.sql)
# so this mapping is legitimately empty for DE and every route falls through
# to raw-code-only, same as before this existed - not a bug.
_ROUTE_DISPLAY_CACHE = {}  # country_code -> {"map": {...}, "expires": epoch}

def _route_display_map(country):
    now = time.time()
    cached = _ROUTE_DISPLAY_CACHE.get(country)
    if cached is not None and now < cached["expires"]:
        return cached["map"]
    conn = get_conn()
    cur = conn.cursor()
    try:
        # A short code can legitimately belong to more than one gtfs_route_id
        # (confirmed: 10 PL / 55 AT / 187 DE names each cover multiple route
        # rows - direction/branch variants sharing one line code). Rather
        # than guess which is "the" long name, DISTINCT ON picks one
        # deterministically (lowest id): the raw code stays
        # authoritative everywhere this is used, the friendly name is best-
        # effort supplementary context, never a replacement.
        cur.execute("""
            SELECT DISTINCT ON (name) name, long_name FROM routes
            WHERE country_code = %s AND long_name IS NOT NULL
            ORDER BY name, id
        """, (country,))
        mapping = dict(cur.fetchall())
        if country == "AT":
            # AT needs a second join, and PL/DE must not use it. AT's live route_name is a
            # train identity ("NJ 456"), not a line code, so it never matches routes.name
            # ("REX81"). trips.trip_short_name holds those identities and bridges them.
            # Ambiguous identities (263 of 7,982) are dropped rather than guessed.
            cur.execute("""
                SELECT tsn, MIN(long_name) FROM (
                    SELECT t.trip_short_name AS tsn, r.long_name
                    FROM trips t JOIN routes r ON r.id = t.route_id
                    WHERE t.country_code = %s
                      AND t.trip_short_name IS NOT NULL AND r.long_name IS NOT NULL
                    GROUP BY t.trip_short_name, r.long_name
                ) x
                GROUP BY tsn HAVING COUNT(*) = 1
            """, (country,))
            # Line-code entries stay as a fallback for anything the
            # train-identity join misses; live rows hit the identity map.
            mapping.update(dict(cur.fetchall()))
    finally:
        cur.close()
        conn.close()
    _ROUTE_DISPLAY_CACHE[country] = {"map": mapping, "expires": now + _DISPLAY_TTL}
    return mapping

def _display_route(mapping, raw):
    # Unlike _display_stop, no clean_display_name-style fallback formatting -
    # an unmapped route code just has no friendly name available (this is
    # the norm for DE, and for AT/PL codes with no static match), and there
    # is nothing to reformat a bare line code into. None here means "show
    # only the raw code", handled by the caller/frontend.
    if not raw:
        return None
    return mapping.get(raw)

@app.get("/live/board")
def get_live_board(country: str = "HU", limit: int = 300, q: str = ""):
    """Live board for the frontend: full-feed aggregates plus a capped,
    delay-ranked slice of trains.

    /live stays untouched (a bare array, whatever size the feed is). This
    exists because that shape can't drive a real UI for PL/DE: PL's feed is
    22k+ TripUpdates (a 4.8MB response), so the client needs server-side
    ranking, searching, and capping - and it needs the aggregates computed
    over the *whole* feed, not just the slice it renders.
    """
    rows = get_live(country)
    mapping = _display_map(country)
    route_mapping = _route_display_map(country)

    # Captured before the `q` filter below narrows `rows` - "total" means the
    # whole feed, which is what the aggregates are documented to describe. It
    # used to come from len(delays), which was safe only while every row had a
    # delay figure; cancelled rows don't, so the two counts have parted ways.
    total_rows = len(rows)
    delays = _live_delays(rows)
    delayed = sum(1 for d in delays if d > 2)
    cancelled = [r for r in rows if r.get("is_cancelled")]
    positioned = [r for r in rows if r.get("lat") is not None]
    sources = {}
    for r in positioned:
        key = r.get("position_source") or "unknown"
        sources[key] = sources.get(key, 0) + 1

    needle = q.strip().lower()
    if needle:
        def matches(r):
            for key in ("route_name", "current_stop", "headsign", "train_number"):
                val = r.get(key)
                if val and needle in str(val).lower():
                    return True
            return False
        rows = [r for r in rows if matches(r)]

    # Cancellations sort above every delay: "not running at all" is the most
    # severe thing that can happen to a train, and a cancelled row's delay is
    # None, which would otherwise sort it to the bottom with the punctual ones.
    ranked = sorted(
        rows,
        key=lambda r: (1 if r.get("is_cancelled") else 0, r["delay_minutes"] or 0),
        reverse=True,
    )
    shown = ranked[:limit] if limit and limit > 0 else ranked

    trains = []
    for r in shown:
        t = dict(r)
        # current_stop stays the raw name - index.html passes it straight to
        # /station/{name}/stats, which matches on stations.name. Only ever
        # add the display label alongside it, never substitute it in.
        t["current_stop_display"] = _display_stop(mapping, r.get("current_stop"), country)
        # Raw operator line code (AT/PL/DE) alongside a friendly name where
        # one exists (route_name itself is left untouched - it's the join
        # key /route/{name}/* uses). None for DE and any AT/PL code with no
        # static match - see _route_display_map.
        t["route_display"] = _display_route(route_mapping, r.get("route_name"))
        if r.get("stops"):
            t["stops"] = [
                {**s, "display": _display_stop(mapping, s.get("name"), country)}
                for s in r["stops"]
            ]
        trains.append(t)

    return {
        "meta": {
            "country": country,
            "total": total_rows,
            "matched": len(ranked),
            "shown": len(trains),
            "delayed": delayed,
            # Computed over running trains only - see _live_delays().
            "on_time_pct": round(100.0 * (len(delays) - delayed) / len(delays), 1) if delays else None,
            "avg_delay": round(sum(delays) / len(delays), 1) if delays else None,
            "worst_delay": max(delays) if delays else None,
            "cancelled": len(cancelled),
            "cancelled_trips": sum(1 for r in cancelled if r.get("cancellation_kind") == "trip"),
            "with_position": len(positioned),
            "position_sources": sources,
            "attribution": COUNTRY_ATTRIBUTION.get(country),
            # PKP PLK's reuse terms require stating when the information was
            # obtained, not just its source - see COUNTRY_ATTRIBUTION. Emitted
            # for every country because it costs nothing and is honest
            # everywhere; the frontend renders it beside the attribution.
            "retrieved_at": datetime.now().isoformat(timespec="seconds"),
        },
        "trains": trains,
    }

@app.get("/live/positions")
def get_live_positions(country: str = "HU"):
    """Only the trains that actually resolved a coordinate - what the map
    needs. Separate from /live/board because the board is delay-ranked and
    capped, while the map wants every positioned train regardless of delay
    (and for PL/DE that's a small subset: 441/22353 and 1292/5533 in real
    polls, since their positions are schedule-interpolated, not GPS)."""
    mapping = _display_map(country)
    out = []
    for r in get_live(country):
        if r.get("lat") is None or r.get("lon") is None:
            continue
        out.append({
            "route_name": r["route_name"],
            "train_number": r.get("train_number") or "",
            "headsign": r.get("headsign"),
            "current_stop": r.get("current_stop"),
            "current_stop_display": _display_stop(mapping, r.get("current_stop"), country),
            "delay_minutes": r["delay_minutes"],
            "status": r.get("status"),
            "lat": r["lat"],
            "lon": r["lon"],
            "position_source": r.get("position_source"),
            "is_cancelled": bool(r.get("is_cancelled")),
            "cancellation_kind": r.get("cancellation_kind"),
            "platform": r.get("platform"),
        })
    return out

@app.get("/live/worst")
def get_live_worst(country: str = "HU", kind: str = "route", limit: int = 10):
    """"Worst right now" - ranked by *current* delay, not the Rankings tab's
    historical reliability (see /routes/summary, /stations/summary). Unlike those, this
    uses a rolling window.

    Deliberately reuses the same get_live(country) snapshot /live/board
    already fetches (same cache, same TTL) rather than a new fetch or a DB
    query - this is a live-only view, computed by grouping the in-memory
    snapshot, not a persisted-history aggregate.
    """
    if kind not in ("route", "station"):
        raise HTTPException(status_code=400, detail="kind must be 'route' or 'station'")
    key = "route_name" if kind == "route" else "current_stop"
    groups = {}
    cancels = {}
    for r in get_live(country):
        name = r.get(key)
        if not name:
            continue
        if r.get("is_cancelled"):
            # Counted, never averaged: a cancellation has no delay to average,
            # and treating it as 0 would make a route look *better* the more
            # of its trains stop running.
            cancels[name] = cancels.get(name, 0) + 1
            groups.setdefault(name, [])
            continue
        delay = r.get("delay_minutes")
        if delay is None:
            continue
        groups.setdefault(name, []).append(delay)

    mapping = _display_map(country) if kind == "station" else _route_display_map(country)
    worst = []
    for name, delays in groups.items():
        entry = {
            "name": name,
            "train_count": len(delays) + cancels.get(name, 0),
            "avg_delay": round(sum(delays) / len(delays), 1) if delays else None,
            "max_delay": round(max(delays), 1) if delays else None,
            "cancelled": cancels.get(name, 0),
        }
        if kind == "station":
            entry["display_name"] = _display_stop(mapping, name, country)
        else:
            entry["display_name"] = _display_route(mapping, name)
        worst.append(entry)
    # A group that is only cancellations has no average delay at all; it still
    # belongs near the top, not sorted as if it were punctual.
    worst.sort(key=lambda e: (e["cancelled"] > 0, e["avg_delay"] if e["avg_delay"] is not None else 0),
               reverse=True)

    return {
        "meta": {
            "country": country,
            "kind": kind,
            "groups": len(worst),
            "attribution": COUNTRY_ATTRIBUTION.get(country),
            "retrieved_at": datetime.now().isoformat(timespec="seconds"),
        },
        "worst": worst[:limit] if limit and limit > 0 else worst,
    }

def get_live_hu():
    try:
        resp = req.post(GRAPHQL_URL, json={"query": GRAPHQL_QUERY}, timeout=20)
        vehicles = resp.json().get("data", {}).get("vehiclePositions", [])
    except Exception:
        return []

    result = []
    for v in vehicles:
        trip = v.get("trip")
        stop_rel = v.get("stopRelationship")
        prev = v.get("prevOrCurrentStop")
        if not trip or not stop_rel or not prev:
            continue
        route_name = clean_route_name(trip.get("route", {}).get("longName"), trip.get("tripShortName"))
        stop_name = stop_rel.get("stop", {}).get("name")
        delay_sec = prev.get("arrivalDelay") or prev.get("departureDelay") or 0
        # See poller.py's parse_vehicles(): OTP models CANCELED but MAV
        # publishes none today, and a vehiclePositions query can only describe
        # trains that are running. Read so a real one is surfaced if that
        # changes; HU is not claimed as having cancellation coverage.
        is_cancelled = prev.get("realtimeState") == "CANCELED"
        if not route_name or not stop_name:
            continue

        raw_stops = trip.get("stoptimes") or []
        stops = []
        stop_idx = None
        for i, st in enumerate(raw_stops):
            stop_info = st.get("stop") or {}
            name = stop_info.get("name") or ""
            # A train doesn't depart its final stop, so MÁV reports
            # departureDelay 0 there - which rendered a 52-minutes-late train
            # as arriving "on time" at its destination, the one stop a rider
            # cares most about. Use the arrival figures for the last stop.
            is_last = i == len(raw_stops) - 1
            if is_last and st.get("arrivalDelay") is not None:
                delay_sec_stop = st.get("arrivalDelay")
                sched = st.get("scheduledArrival")
            else:
                delay_sec_stop = st.get("departureDelay") or 0
                sched = st.get("scheduledDeparture")
            # Checked against the raw feed: MÁV reports 0 for *both* arrival and
            # departure delay at a trip's final stop even when the train is
            # running an hour late (every intermediate stop carries the real
            # figure). That's the feed declining to predict the final arrival,
            # not a prediction of punctuality - so it's reported as unknown
            # rather than telling a rider on a 52-late train that it arrives on
            # time, which is the one stop they care most about.
            stop_delay_min = round(delay_sec_stop / 60, 1)
            if is_last and stop_delay_min == 0 and (delay_sec or 0) / 60 > 2:
                stop_delay_min = None
            stop_cancelled = st.get("realtimeState") == "CANCELED"
            stops.append({
                "name": name,
                "dep": _fmt_time(sched),
                "delay_min": None if stop_cancelled else stop_delay_min,
                # Carried so /train/HU/{id} can draw the journey on a map,
                # the same way AT's stopovers and PL/DE's stations do.
                "lat": stop_info.get("lat"),
                "lon": stop_info.get("lon"),
                "platform": stop_info.get("platformCode"),
                "cancelled": stop_cancelled,
            })
            if name == stop_name and stop_idx is None:
                stop_idx = i

        stop_obj = stop_rel.get("stop") or {}
        lat = v.get("lat") or stop_obj.get("lat")
        lon = v.get("lon") or stop_obj.get("lon")
        result.append({
            "route_name": route_name,
            "train_number": trip.get("tripShortName") or "",
            "headsign": trip.get("tripHeadsign"),
            # See _live_result()'s train_id comment. HU's is OTP's own
            # trip.gtfsId - opaque and unjoinable to our static trips table
            # (that's why live_trip_id exists at all), but it does reliably
            # address one train within the live feed, which is all
            # /train/HU/{id} needs.
            "train_id": trip.get("gtfsId"),
            "current_stop": stop_name,
            "delay_minutes": None if is_cancelled else round(delay_sec / 60, 1),
            "status": "CANCELLED" if is_cancelled else (stop_rel.get("status") or "IN_TRANSIT_TO"),
            "is_cancelled": is_cancelled,
            "cancellation_kind": "stop" if is_cancelled else None,
            "platform": stop_obj.get("platformCode"),
            "stops": stops,
            "stop_idx": stop_idx,
            "lat": lat,
            "lon": lon,
            # Real position off the live feed (either the vehicle's own
            # reported lat/lon, or its current stop's known location as a
            # fallback) - not a schedule-interpolated estimate, so "live"
            # either way. See _live_result()'s comment for why this field
            # exists: PL/DE fall back to "estimated", never silently blended.
            "position_source": "live" if lat is not None else None,
        })

    return result

@app.get("/health")
def health():
    """Last result from monitor.py, served as JSON.

    monitor.py writes the file on a schedule rather than this endpoint doing
    the checks: they hit all four upstream feeds, which is far too expensive
    to run per request, and an endpoint that only reports when polled is not
    monitoring anyway (see monitor.py's own docstring for the failures this
    exists to catch). Reading a file also means /health still answers when
    the database is the thing that's broken."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor_status.json")
    try:
        with open(path) as f:
            doc = json.load(f)
    except FileNotFoundError:
        return {"status": "unknown", "detail": "monitor.py has not run yet"}
    except Exception as exc:
        return {"status": "unknown", "detail": f"could not read status: {exc}"}
    age_min = None
    try:
        checked = datetime.fromisoformat(doc["checked_at"])
        age_min = round((datetime.now() - checked).total_seconds() / 60, 1)
    except (KeyError, TypeError, ValueError):
        pass  # no usable timestamp: report the status without an age
    # A stale status file is itself a failure - it means the monitor stopped.
    if age_min is not None and age_min > 60:
        doc = {**doc, "status": "critical",
               "detail": f"monitor.py last ran {age_min:.0f} min ago"}
    doc["age_minutes"] = age_min
    return doc

# --- One train's full journey ---
# /train/{country}/{id} turns the delay statistics into a tracker: where one
# train is now and its remaining timetable. Each country answers differently:
#   HU  the OTP response already carries the full stoptimes array.
#   AT  radar() returns only about 4 upcoming stopovers, so a second HAFAS call,
#       client.trip() (hafas/trip.js), fetches the whole journey.
#   PL  the feed has a stop_time_update per stop but only stop_sequence and an
#       absolute time, so names and scheduled times come from our own
#       stop_times and stations, as in resolve_delays().
#   DE  the feed has stop_id, delay and time per stop, but stop_ids are
#       platform-level children, so they go through load_parent_map().
#
# A raw feed fetch is cheap (PL 0.5 s, DE 0.8 s). The expensive part of /live
# was the 22k-row DB join, which this does for one trip, so there is no cache.
_JOURNEY_ROUTE_STATS_TTL = 900

# A delay this large is a schedule-version mismatch, not a delay. A few PL
# trips, international ones especially, are renumbered upstream, so matching
# sequence to sequence pairs the wrong stops and yields roughly 20-hour
# "delays" (PLK_LEO_2026_516260411 is 15-31 in the feed and 0-16 in
# stop_times). They are reported as unknown instead of showing "1191 minutes
# late".
_MAX_PLAUSIBLE_DELAY_MIN = 720

def _plausible_delay(delay_min):
    if delay_min is None or abs(delay_min) > _MAX_PLAUSIBLE_DELAY_MIN:
        return None
    return delay_min

def _journey_stop(name, mapping, country, scheduled=None, expected=None,
                  delay_min=None, lat=None, lon=None, platform=None,
                  cancelled=False):
    return {
        "name": name,
        "display": _display_stop(mapping, name, country),
        "scheduled": scheduled,
        "expected": expected,
        "delay_min": delay_min,
        "lat": lat,
        "lon": lon,
        "platform": platform,
        # A stop the train will not call at. Distinct from delay_min being
        # None (which means "no prediction available") - one says the train
        # isn't stopping, the other says we don't know when it will.
        "cancelled": cancelled,
    }

def _epoch_to_hhmm(epoch, tz):
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, tz).strftime("%H:%M")

def _iso_to_hhmm(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).strftime("%H:%M")
    except ValueError:
        return None

def _journey_hu(train_id, mapping):
    for t in get_live_hu():
        if t.get("train_id") != train_id:
            continue
        stops = [
            _journey_stop(s["name"], mapping, "HU", scheduled=s.get("dep"),
                          delay_min=s.get("delay_min"),
                          lat=s.get("lat"), lon=s.get("lon"),
                          platform=s.get("platform"),
                          cancelled=bool(s.get("cancelled")))
            for s in t.get("stops") or []
        ]
        # HU's feed gives a scheduled departure and a per-stop delay but no
        # expected time, so derive it rather than leaving the column blank.
        for s in stops:
            s["expected"] = _shift_hhmm(s["scheduled"], s["delay_min"])
        return {**t, "stops": stops}
    return None

def _shift_hhmm(hhmm, delay_min):
    if not hhmm or delay_min is None:
        return hhmm
    try:
        h, m = (int(x) for x in hhmm.split(":"))
    except ValueError:
        return hhmm
    total = (h * 60 + m + int(round(delay_min))) % (24 * 60)
    return f"{total // 60:02d}:{total % 60:02d}"

def _journey_at(train_id, mapping):
    live = None
    for t in get_live("AT"):
        if t.get("train_id") == train_id:
            live = t
            break
    trip = call_trip(train_id, live.get("route_name") if live else None)
    if trip is None:
        return None
    stops = []
    for s in trip.get("stopovers") or []:
        # Prefer arrival for an intermediate stop, departure for the origin
        # (which has no arrival at all) - matching what a rider reads off a
        # platform display.
        delay_sec = s.get("arrivalDelay")
        if delay_sec is None:
            delay_sec = s.get("departureDelay")
        planned = s.get("plannedArrival") or s.get("plannedDeparture")
        actual = s.get("arrival") or s.get("departure")
        stop_cancelled = bool(s.get("cancelled"))
        stops.append(_journey_stop(
            s.get("name"), mapping, "AT",
            scheduled=_iso_to_hhmm(planned),
            expected=_iso_to_hhmm(actual) or _iso_to_hhmm(planned),
            delay_min=(None if stop_cancelled
                       else (round(delay_sec / 60, 1) if delay_sec is not None else None)),
            lat=s.get("latitude"), lon=s.get("longitude"),
            platform=s.get("platform"),
            cancelled=stop_cancelled,
        ))
    loc = trip.get("currentLocation") or {}
    return {
        "route_name": trip.get("line") or (live or {}).get("route_name"),
        "train_number": trip.get("line") or "",
        "headsign": trip.get("direction") or trip.get("destination"),
        "train_id": train_id,
        "current_stop": (live or {}).get("current_stop"),
        "delay_minutes": (live or {}).get("delay_minutes"),
        "status": "CANCELLED" if trip.get("cancelled") else "IN_TRANSIT_TO",
        "stops": stops,
        "lat": loc.get("latitude") or (live or {}).get("lat"),
        "lon": loc.get("longitude") or (live or {}).get("lon"),
        "position_source": "live",
    }

def _journey_pl(train_id, mapping):
    feed = fetch_feed()
    update = None
    for entity in feed.entity:
        if entity.HasField("trip_update") and entity.trip_update.trip.trip_id == train_id:
            update = entity.trip_update
            break
    if update is None:
        return None
    start_date = update.trip.start_date
    tz = ZoneInfo("Europe/Warsaw")
    conn = get_conn()
    cur = conn.cursor()
    try:
        # One query for the trip's whole schedule, rather than PL's
        # per-stop-sequence lookup - this is a single trip, so the whole
        # thing is ~17 rows.
        cur.execute("""
            SELECT st.stop_sequence, s.name, st.scheduled_arrival,
                   st.scheduled_departure, s.lat, s.lon
            FROM stop_times st
            JOIN stations s ON s.id = st.station_id
            WHERE st.trip_id = (
                SELECT id FROM trips WHERE gtfs_trip_id = %s AND country_code = 'PL'
            )
            ORDER BY st.stop_sequence
        """, (train_id,))
        schedule = {row[0]: row for row in cur.fetchall()}
        cur.execute("""
            SELECT r.name, t.headsign
            FROM trips t LEFT JOIN routes r ON r.id = t.route_id
            WHERE t.gtfs_trip_id = %s AND t.country_code = 'PL'
        """, (train_id,))
        meta = cur.fetchone() or (None, None)
    finally:
        cur.close()
        conn.close()

    trip_cancelled = update.trip.schedule_relationship == gtfs_realtime_pb2.TripDescriptor.CANCELED
    actual_by_seq = {}
    skipped_seqs = set()
    for stu in update.stop_time_update:
        if stu.schedule_relationship == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED:
            skipped_seqs.add(stu.stop_sequence)
            continue
        epoch = None
        if stu.HasField("arrival") and stu.arrival.HasField("time"):
            epoch = stu.arrival.time
        elif stu.HasField("departure") and stu.departure.HasField("time"):
            epoch = stu.departure.time
        if epoch:
            actual_by_seq[stu.stop_sequence] = epoch

    stops = []
    for seq in sorted(schedule):
        _, name, sched_arr, sched_dep, lat, lon = schedule[seq]
        sched_str = sched_dep or sched_arr
        sched_epoch = _scheduled_epoch(start_date, sched_str) if sched_str else None
        actual_epoch = actual_by_seq.get(seq)
        stop_cancelled = trip_cancelled or seq in skipped_seqs
        delay = None
        if sched_epoch and actual_epoch and not stop_cancelled:
            delay = _plausible_delay(round((actual_epoch - sched_epoch) / 60, 1))
        stops.append(_journey_stop(
            name, mapping, "PL",
            scheduled=_epoch_to_hhmm(sched_epoch, tz),
            # If the delay was rejected as implausible the "expected" time it
            # implies is equally wrong, so fall back to the scheduled time
            # rather than showing a confidently wrong arrival.
            expected=(_epoch_to_hhmm(actual_epoch, tz) if delay is not None
                      else _epoch_to_hhmm(sched_epoch, tz)),
            delay_min=delay, lat=lat, lon=lon,
            cancelled=stop_cancelled,
        ))
    return {
        "route_name": meta[0], "train_number": "", "headsign": meta[1],
        "train_id": train_id, "stops": stops, "position_source": "estimated",
        # Full route: the stop list comes from our own stop_times, so it spans
        # origin to destination even where the feed only predicts part of it.
        "journey_scope": "full",
        "is_cancelled": trip_cancelled,
        "cancellation_kind": "trip" if trip_cancelled else ("stop" if skipped_seqs else None),
    }

def _journey_de(train_id, mapping):
    feed = fetch_de_feed()
    update = None
    for entity in feed.entity:
        if entity.HasField("trip_update") and entity.trip_update.trip.trip_id == train_id:
            update = entity.trip_update
            break
    if update is None:
        return None
    parent_map = load_parent_map()
    platform_map = load_platform_map()
    tz = ZoneInfo("Europe/Berlin")
    stop_ids = [
        parent_map.get(stu.stop_id, stu.stop_id)
        for stu in update.stop_time_update if stu.stop_id
    ]
    names = {}
    conn = get_conn()
    cur = conn.cursor()
    try:
        if stop_ids:
            cur.execute("""
                SELECT gtfs_stop_id, name, lat, lon FROM stations
                WHERE country_code = 'DE' AND gtfs_stop_id = ANY(%s)
            """, (stop_ids,))
            names = {row[0]: row[1:] for row in cur.fetchall()}
        cur.execute("""
            SELECT r.name, t.headsign
            FROM trips t LEFT JOIN routes r ON r.id = t.route_id
            WHERE t.gtfs_trip_id = %s AND t.country_code = 'DE'
        """, (train_id,))
        meta = cur.fetchone() or (None, None)
    finally:
        cur.close()
        conn.close()

    skipped = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
    all_skipped = bool(update.stop_time_update) and all(
        s.schedule_relationship == skipped for s in update.stop_time_update
    )
    any_skipped = any(s.schedule_relationship == skipped for s in update.stop_time_update)

    stops = []
    for stu in update.stop_time_update:
        parent = parent_map.get(stu.stop_id, stu.stop_id)
        row = names.get(parent)
        if not row:
            # Same graceful-skip convention as the pollers: a stop we can't
            # name is left out rather than shown as a blank row.
            continue
        name, lat, lon = row
        stop_cancelled = stu.schedule_relationship == skipped
        delay_sec = None
        actual = None
        if stu.HasField("arrival"):
            delay_sec = stu.arrival.delay if stu.arrival.HasField("delay") else None
            actual = stu.arrival.time if stu.arrival.HasField("time") else None
        if actual is None and stu.HasField("departure"):
            delay_sec = stu.departure.delay if stu.departure.HasField("delay") else delay_sec
            actual = stu.departure.time if stu.departure.HasField("time") else None
        # gtfs.de gives actual time and delay, so the scheduled time is the
        # difference - no stop_times import needed for DE (it has none).
        sched_epoch = (actual - delay_sec) if (actual and delay_sec is not None) else actual
        stops.append(_journey_stop(
            name, mapping, "DE",
            scheduled=_epoch_to_hhmm(sched_epoch, tz),
            expected=_epoch_to_hhmm(actual, tz),
            delay_min=(None if stop_cancelled
                       else (round(delay_sec / 60, 1) if delay_sec is not None else None)),
            lat=lat, lon=lon,
            platform=platform_map.get(stu.stop_id),
            cancelled=stop_cancelled,
        ))
    return {
        "route_name": meta[0], "train_number": "", "headsign": meta[1],
        "train_id": train_id, "stops": stops, "position_source": "estimated",
        # gtfs.de never sets the trip-level CANCELED flag, so a fully cancelled
        # train is only recognizable as one whose stops are all skipped - see
        # germany_poller.parse_de_vehicles().
        "is_cancelled": all_skipped,
        "cancellation_kind": "trip" if all_skipped else ("stop" if any_skipped else None),
        # DE is the one country with no stop_times imported, so the journey can
        # only be as complete as the feed itself - and gtfs.de's TripUpdates
        # carry only a trip's *remaining* stops (confirmed: a train near the end
        # of its run reports 2, one just starting reports up to 70). Flagged so
        # the UI can say "remaining stops" instead of implying a full route.
        "journey_scope": "remaining",
    }

_JOURNEY_PROVIDERS = {
    "HU": _journey_hu,
    "AT": _journey_at,
    "PL": _journey_pl,
    "DE": _journey_de,
}

# --- One train's own history ---
# "Is this specific train usually late", not "is this route". It reads
# delay_observations.trip_id, so it needs no new data source.
#
# What each country can answer differs, so the answer comes back with the data
# (see identity_basis and history_since):
#   DE and PL  every row has a real trips.id.
#   HU and AT  only since the train-number resolvers landed (about 48% and 54%).
#
# HU's live id is OTP-internal and AT's is a HAFAS composite key. Neither is
# stable across days or joinable to the schedule, so both are resolved by the
# train's public number, as their pollers do. Where even that fails,
# live_trip_id gives same-day continuity only, and the response says so.

_HISTORY_MAX_DAYS = 365

def _resolve_history_trip(cur, country, train_id):
    """Map a live train id onto a static trips.id, per country.

    Returns (trip_id, basis, route_name) where basis names which identity path
    worked, so the caller can be honest about what the history covers, and
    route_name is the live train's route where we know it (used to scope the
    opaque-id fallback - see below).
    """
    if country in ("PL", "DE"):
        # The feed's own gtfs_trip_id is what both pollers already write, so
        # this is a direct lookup - no heuristics involved.
        cur.execute(
            "SELECT id FROM trips WHERE gtfs_trip_id = %s AND country_code = %s",
            (train_id, country),
        )
        row = cur.fetchone()
        if row:
            return row[0], "gtfs_trip_id", None
        return None, None, None

    # HU/AT: the live id is opaque, so find this train in the current live
    # snapshot and resolve it by its public number, exactly as the poller does.
    live = None
    for t in (get_live_hu() if country == "HU" else get_live("AT")):
        if t.get("train_id") == train_id:
            live = t
            break
    if live is None:
        return None, None, None
    route_name = live.get("route_name")
    if country == "HU":
        number = extract_train_number(live.get("train_number"))
        trip_id = hu_resolve_trip_id(cur, number, live.get("current_stop"),
                                     live.get("delay_minutes"), local_now("HU"))
    else:
        number = extract_at_train_number(live.get("route_name"))
        trip_id = at_resolve_trip_id(cur, number, live.get("current_stop"),
                                     live.get("delay_minutes"), local_now("AT"))
    if trip_id is not None:
        return trip_id, "train_number", route_name
    return None, None, route_name

@app.get("/train/{country}/{train_id:path}/history")
def get_train_history(country: str, train_id: str, days: int = 30):
    """How this one train has actually run on previous days."""
    country = country.upper()
    if country not in _JOURNEY_PROVIDERS:
        raise HTTPException(status_code=400, detail=f"Unknown country {country}")
    days = max(1, min(days, _HISTORY_MAX_DAYS))

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        plain = conn.cursor()
        trip_id, basis, live_route = _resolve_history_trip(plain, country, train_id)
        plain.close()

        # A cancelled run has no delay to average but is the most important
        # thing in a train's record, so it is counted separately throughout
        # rather than being dropped or folded in as a zero.
        if trip_id is not None:
            where, params = "o.trip_id = %s", [trip_id]
        else:
            # No static identity: fall back to the raw feed id, which HU/AT
            # pollers store unresolved. MAV's OTP ids turn out to be far more
            # stable across days than expected (one sampled id ran as the same
            # route for 15 consecutive days), so this is genuinely useful -
            # but it is NOT safe on its own: 931 of 5,328 HU live ids have
            # carried more than one route_name, i.e. the id gets reused for a
            # different train. Scoping to the route the live train is actually
            # running turns a silently-blended history into a correct one.
            where = "o.live_trip_id = %s AND o.country_code = %s"
            params = [train_id, country]
            if live_route:
                where += " AND o.route_name = %s"
                params.append(live_route)
            basis = "live_trip_id"

        cur.execute(f"""
            SELECT o.observed_date AS day,
                   COUNT(*) FILTER (WHERE NOT o.is_cancelled) AS observations,
                   ROUND(AVG(o.delay_minutes)::numeric, 1) AS avg_delay,
                   ROUND(MAX(o.delay_minutes)::numeric, 1) AS worst_delay,
                   COUNT(*) FILTER (WHERE o.is_cancelled) AS cancellations,
                   ROUND(100.0 * COUNT(*) FILTER (WHERE o.delay_minutes <= 2)
                         / NULLIF(COUNT(*) FILTER (WHERE NOT o.is_cancelled), 0), 1) AS pct_on_time
            FROM delay_observations o
            WHERE {where} AND o.observed_date >= CURRENT_DATE - %s::int
            GROUP BY o.observed_date
            ORDER BY o.observed_date DESC
        """, params + [days])
        runs = [dict(r) for r in cur.fetchall()]

        cur.execute(f"""
            SELECT EXTRACT(ISODOW FROM o.observed_date)::int AS dow,
                   COUNT(*) FILTER (WHERE NOT o.is_cancelled) AS observations,
                   ROUND(AVG(o.delay_minutes)::numeric, 1) AS avg_delay,
                   COUNT(*) FILTER (WHERE o.is_cancelled) AS cancellations,
                   ROUND(100.0 * COUNT(*) FILTER (WHERE o.delay_minutes <= 2)
                         / NULLIF(COUNT(*) FILTER (WHERE NOT o.is_cancelled), 0), 1) AS pct_on_time
            FROM delay_observations o
            WHERE {where} AND o.observed_date >= CURRENT_DATE - %s::int
            GROUP BY dow ORDER BY dow
        """, params + [days])
        by_dow = [dict(r) for r in cur.fetchall()]

        cur.execute(f"""
            SELECT COUNT(*) FILTER (WHERE NOT o.is_cancelled) AS observations,
                   COUNT(DISTINCT o.observed_date) AS days_seen,
                   COUNT(DISTINCT o.route_name) AS distinct_routes,
                   MIN(o.observed_date) AS first_seen,
                   ROUND(AVG(o.delay_minutes)::numeric, 1) AS avg_delay,
                   ROUND(MAX(o.delay_minutes)::numeric, 1) AS worst_delay,
                   COUNT(*) FILTER (WHERE o.is_cancelled) AS cancellations,
                   ROUND(100.0 * COUNT(*) FILTER (WHERE o.delay_minutes <= 2)
                         / NULLIF(COUNT(*) FILTER (WHERE NOT o.is_cancelled), 0), 1) AS pct_on_time
            FROM delay_observations o
            WHERE {where} AND o.observed_date >= CURRENT_DATE - %s::int
        """, params + [days])
        summary = dict(cur.fetchone() or {})
    finally:
        cur.close()
        conn.close()

    return {
        "country": country,
        "train_id": train_id,
        "days": days,
        # Which identity path produced this history, and therefore how far it
        # can be trusted: "gtfs_trip_id" is a real join to the static
        # schedule, "train_number" is the poller's own resolver, and
        # "live_trip_id" is an opaque same-day-only correlation.
        "identity_basis": basis,
        "has_history": bool(summary.get("observations") or summary.get("cancellations")),
        # A last-resort integrity check rather than a silent wrong answer: if
        # the rows we just aggregated span more than one route, the identifier
        # has been reused for a different train and this "history" is a blend
        # of two. The frontend says so instead of presenting it as one train's
        # record.
        "mixed_identity": (summary.get("distinct_routes") or 0) > 1,
        "summary": summary,
        "runs": runs,
        "by_dow": by_dow,
        "attribution": COUNTRY_ATTRIBUTION.get(country),
    }

# --- How this train runs at each stop ---
# Reads idx_delay_obs_trip_stop (schema/008): every observation of one trip at
# one stop. /history says whether a train is usually late. This says where it
# loses the time, which is what a transfer depends on.
#
# Three things are surfaced, not hidden behind an average:
#   Sample depth. stop_sequence has only been written since 2026-08-21, so each
#   stop has its own observation and day counts, and `sufficient_sample` marks
#   those that clear _MIN_STOP_SAMPLE.
#   AT has no arrival delays. radar() never sets arrivalDelay, so the poller
#   writes NULL, and `arrival_available` says so, since an empty column would
#   read as "never late arriving".
#   No stop identity without a resolved trip. A train that fell back to
#   live_trip_id has no stop rows, which returns an explicit reason.
_MIN_STOP_SAMPLE = 20  # days of history before a per-stop median means anything

@app.get("/train/{country}/{train_id:path}/stops")
def get_train_stops(country: str, train_id: str, days: int = 30):
    """Per-stop delay profile for one train: where it loses or makes up time."""
    country = country.upper()
    if country not in _JOURNEY_PROVIDERS:
        raise HTTPException(status_code=400, detail=f"Unknown country {country}")
    days = max(1, min(days, _HISTORY_MAX_DAYS))

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        plain = conn.cursor()
        trip_id, basis, _live_route = _resolve_history_trip(plain, country, train_id)
        plain.close()

        if trip_id is None:
            # Deliberately not falling back to live_trip_id the way /history
            # does: those rows carry no stop_sequence by construction, so the
            # fallback could only ever return nothing.
            return {
                "country": country, "train_id": train_id, "days": days,
                "identity_basis": basis, "stops": [],
                "arrival_available": country != "AT",
                "reason": ("This train has no resolved static trip, so no stop "
                           "identity was recorded for it. Stop-level history "
                           "needs the trip resolved at poll time."),
                "attribution": COUNTRY_ATTRIBUTION.get(country),
            }

        # Median rather than mean throughout: delay distributions are strongly
        # right-skewed (one 90-minute failure drags a mean the whole way up),
        # and p90 is what actually matters for "will I make the change".
        cur.execute("""
            SELECT o.stop_sequence,
                   MIN(s.name)                     AS station_name,
                   MIN(s.display_name)             AS station_display,
                   MIN(o.scheduled_arrival)        AS scheduled_arrival,
                   MIN(o.scheduled_departure)      AS scheduled_departure,
                   COUNT(*) FILTER (WHERE NOT o.is_cancelled)     AS observations,
                   COUNT(DISTINCT o.observed_date)                AS days_seen,
                   COUNT(*) FILTER (WHERE o.is_cancelled)         AS cancellations,
                   COUNT(o.delay_arrival_minutes)                 AS n_arrival,
                   ROUND(percentile_cont(0.5) WITHIN GROUP (
                       ORDER BY o.delay_arrival_minutes)::numeric, 1)   AS median_arrival_delay,
                   ROUND(percentile_cont(0.9) WITHIN GROUP (
                       ORDER BY o.delay_arrival_minutes)::numeric, 1)   AS p90_arrival_delay,
                   COUNT(o.delay_departure_minutes)               AS n_departure,
                   ROUND(percentile_cont(0.5) WITHIN GROUP (
                       ORDER BY o.delay_departure_minutes)::numeric, 1) AS median_departure_delay,
                   ROUND(percentile_cont(0.9) WITHIN GROUP (
                       ORDER BY o.delay_departure_minutes)::numeric, 1) AS p90_departure_delay,
                   ROUND(AVG(o.delay_minutes)::numeric, 1)        AS avg_delay
            FROM delay_observations o
            JOIN stations s ON s.id = o.station_id
            WHERE o.country_code = %s AND o.trip_id = %s
              AND o.stop_sequence IS NOT NULL
              AND o.observed_date >= CURRENT_DATE - %s::int
            GROUP BY o.stop_sequence
            ORDER BY o.stop_sequence
        """, (country, trip_id, days))
        stops = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    for s in stops:
        s["sufficient_sample"] = (s.get("days_seen") or 0) >= _MIN_STOP_SAMPLE

    return {
        "country": country,
        "train_id": train_id,
        "days": days,
        "identity_basis": basis,
        # False for AT for a structural reason, not a data-volume one - see the
        # block comment above. The frontend must not render an empty arrival
        # column as "always on time".
        "arrival_available": country != "AT",
        "min_sample_days": _MIN_STOP_SAMPLE,
        "sufficient_sample": any(s["sufficient_sample"] for s in stops),
        "stops": stops,
        "attribution": COUNTRY_ATTRIBUTION.get(country),
    }

@app.get("/train/{country}/{train_id:path}")
def get_train(country: str, train_id: str):
    country = country.upper()
    provider = _JOURNEY_PROVIDERS.get(country)
    if provider is None:
        return {"found": False, "error": f"Unknown country {country}"}
    mapping = _display_map(country)
    try:
        journey = provider(train_id, mapping)
    except Exception as exc:
        # A train tracker that 500s when one upstream feed hiccups is worse
        # than one that says so - the frontend renders this as a real message.
        return {"found": False, "error": str(exc)}
    if journey is None:
        return {"found": False, "error": "This train is no longer in the live feed."}

    stops = journey.get("stops") or []
    # Which stop is the train at/heading for: the live current_stop if the
    # feed named one, else the last stop whose expected time has passed.
    idx = None
    current = journey.get("current_stop")
    if current:
        for i, s in enumerate(stops):
            if s["name"] == current:
                idx = i
                break
    if idx is None and stops:
        now_hhmm = datetime.now().strftime("%H:%M")
        passed = [i for i, s in enumerate(stops) if s.get("expected") and s["expected"] <= now_hhmm]
        idx = passed[-1] if passed else 0
    for i, s in enumerate(stops):
        s["passed"] = idx is not None and i < idx
        s["current"] = i == idx

    delay = journey.get("delay_minutes")
    if delay is None and idx is not None and stops:
        delay = stops[idx].get("delay_min")

    route_name = journey.get("route_name")
    # Where this train probably ends up, not where it is. Carrying the current
    # delay forward is the obvious answer and a measurably wrong one - see
    # project_arrival_delay. None outside Germany, deliberately: the other
    # three feeds do not observe consecutive stops often enough to derive a
    # rate, and borrowing Germany's would be a guess dressed as a figure.
    remaining = max(0, len(stops) - (idx or 0) - 1)
    projected = project_arrival_delay(country, delay, remaining)
    route_name = journey.get("route_name")
    return {
        "found": True,
        "country": country,
        "projected_arrival_delay_min": projected,
        "stops_remaining": remaining,
        "train_id": journey.get("train_id") or train_id,
        "train_number": journey.get("train_number") or "",
        "route_name": route_name,
        "headsign": journey.get("headsign"),
        "origin": stops[0]["display"] if stops else None,
        "destination": stops[-1]["display"] if stops else None,
        "current_stop": _display_stop(mapping, current, country),
        "delay_minutes": None if journey.get("is_cancelled") else delay,
        "status": journey.get("status") or "IN_TRANSIT_TO",
        "is_cancelled": bool(journey.get("is_cancelled")),
        "cancellation_kind": journey.get("cancellation_kind"),
        "platform": journey.get("platform") or (stops[idx].get("platform") if (idx is not None and stops) else None),
        "stop_idx": idx,
        "stops": stops,
        "lat": journey.get("lat"),
        "lon": journey.get("lon"),
        "position_source": journey.get("position_source"),
        "journey_scope": journey.get("journey_scope"),
        "route_stats": _train_route_stats(route_name, country),
        "attribution": COUNTRY_ATTRIBUTION.get(country),
    }

def _train_route_stats(route_name, country):
    """This route's historical reliability, to sit under the live journey.

    The whole point of pairing a tracker with this project's data: not just
    "your train is 12 minutes late" but "and this route runs late 40% of the
    time". Cached per route because it's the same whole-country-scan class of
    query as /route/{name}/stats."""
    if not route_name:
        return None
    def compute():
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                SELECT COUNT(*) AS total,
                       ROUND(AVG(delay_minutes)::numeric, 1) AS avg_delay,
                       ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END)
                             / NULLIF(COUNT(*), 0), 1) AS pct_on_time,
                       MAX(delay_minutes) AS worst_delay
                FROM delay_observations
                WHERE route_name = %s AND country_code = %s
            """, (route_name, country))
            row = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        return dict(row) if row and row["total"] else None
    return _cached_summary("route_stats:" + route_name, country, compute)

# --- Historical station board -----------------------------------------

@app.get("/station/{station_name:path}/board")
def get_station_board(station_name: str, country: str = "HU", date: str = None, limit: int = 200):
    """What actually called at this station on a given day, and how late.

    The live board answers "what is here now"; this answers "what was here",
    which is what someone checking whether their train ran yesterday needs.
    Defaults to today so it doubles as a same-day record.
    """
    if date:
        try:
            day = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
    else:
        day = None
    limit = max(1, min(limit, 1000))

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # DISTINCT ON collapses the repeated 5-minute samples of one train into
        # its latest reading for the day - without it a single train that sat
        # in the feed all afternoon fills the entire board.
        cur.execute("""
            SELECT DISTINCT ON (COALESCE(o.trip_id::text, o.live_trip_id, o.route_name), o.route_name)
                   o.route_name, o.headsign, o.delay_minutes, o.is_cancelled,
                   o.cancellation_kind, o.platform, o.actual_arrival, o.observed_date,
                   o.trip_id, o.live_trip_id
            FROM delay_observations o
            JOIN stations s ON s.id = o.station_id
            WHERE s.name = %s AND o.country_code = %s
              AND o.observed_date = COALESCE(%s::date, CURRENT_DATE)
            ORDER BY COALESCE(o.trip_id::text, o.live_trip_id, o.route_name), o.route_name, o.id DESC
        """, (station_name, country, day))
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    rows.sort(key=lambda r: (r["actual_arrival"] is None, r["actual_arrival"]), reverse=True)
    shown = rows[:limit]
    delays = [r["delay_minutes"] for r in rows
              if not r["is_cancelled"] and r["delay_minutes"] is not None]
    mapping = _display_map(country)
    route_mapping = _route_display_map(country)
    for r in shown:
        r["route_display"] = _display_route(route_mapping, r.get("route_name"))
        if r.get("actual_arrival"):
            r["seen_at"] = r["actual_arrival"].strftime("%H:%M")

    return {
        "meta": {
            "country": country,
            "station": station_name,
            "station_display": _display_stop(mapping, station_name, country),
            "date": str(day) if day else str(datetime.now().date()),
            "total": len(rows),
            "shown": len(shown),
            "avg_delay": round(sum(delays) / len(delays), 1) if delays else None,
            "pct_on_time": (round(100.0 * sum(1 for d in delays if d <= 2) / len(delays), 1)
                            if delays else None),
            "cancellations": sum(1 for r in rows if r["is_cancelled"]),
            "attribution": COUNTRY_ATTRIBUTION.get(country),
        },
        "trains": shown,
    }

# --- Operator / category league table ---------------------------------

def _operators_summary_query(country):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # routes.agency_name is the operator. It is joined through route_name, not
        # trip_id, because route_name is on every row in every country and trip_id is
        # not. DISTINCT ON picks one agency per name, since a name can map to several
        # route rows (direction and branch variants).
        #
        # AT needs a different join and returned zero rows without it: its route_name
        # is a train identity ("RJ 61"), not a line code, so trips.trip_short_name
        # bridges the two. Ambiguous identities are dropped rather than guessed.
        if country == "AT":
            route_agency = """
                SELECT tsn AS name, MIN(agency_name) AS agency_name FROM (
                    SELECT t.trip_short_name AS tsn, r.agency_name
                    FROM trips t JOIN routes r ON r.id = t.route_id
                    WHERE t.country_code = %s
                      AND t.trip_short_name IS NOT NULL AND r.agency_name IS NOT NULL
                    GROUP BY t.trip_short_name, r.agency_name
                ) x
                GROUP BY tsn HAVING COUNT(*) = 1
            """
        else:
            route_agency = """
                SELECT DISTINCT ON (name) name, agency_name
                FROM routes
                WHERE country_code = %s AND agency_name IS NOT NULL
                ORDER BY name, id
            """
        cur.execute(f"""
            WITH route_agency AS ({route_agency})
            SELECT ra.agency_name AS operator,
                   COUNT(*) FILTER (WHERE NOT o.is_cancelled) AS total,
                   COUNT(DISTINCT o.route_name) AS routes,
                   ROUND(AVG(o.delay_minutes)::numeric, 2) AS avg_delay,
                   ROUND(MAX(o.delay_minutes)::numeric, 1) AS worst_delay,
                   COUNT(*) FILTER (WHERE o.is_cancelled) AS cancellations,
                   ROUND(100.0 * COUNT(*) FILTER (WHERE o.delay_minutes <= 2)
                         / NULLIF(COUNT(*) FILTER (WHERE NOT o.is_cancelled), 0), 1) AS pct_on_time
            FROM delay_observations o
            JOIN route_agency ra ON ra.name = o.route_name
            WHERE o.country_code = %s
            GROUP BY ra.agency_name
            HAVING COUNT(*) >= 20
            ORDER BY pct_on_time DESC NULLS LAST
        """, (country, country))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

# Why a country's operator table can legitimately be empty. Both are gaps in
# the source data, not bugs to be found later, so they are stated rather than
# rendered as an unexplained blank table.
_OPERATOR_GAP_NOTE = {
    "PL": ("Poland's source feed carries no operator names at all "
           "(0 of 181 routes), so this ranking cannot be built for PL."),
    "AT": ("No Austrian trains could be matched to an operator: OBB's live "
           "feed identifies trains by number rather than by line, and those "
           "numbers did not resolve to a single operator."),
}

@app.get("/operators/summary")
def get_operators_summary(country: str = "HU", format: str = None):
    """Reliability ranked by operator, not by route or station."""
    rows = _cached_summary("operators", country, lambda: _operators_summary_query(country),
                           ttl=_WARM_TTL)
    if format == "csv":
        return _csv_response(rows, f"operators-{country}")
    return {
        "meta": {
            "country": country,
            "operators": len(rows),
            # An empty leaderboard must say why. Silence would read as "no
            # data yet" for a gap that is permanent at the source.
            "note": _OPERATOR_GAP_NOTE.get(country) if not rows else None,
            "attribution": COUNTRY_ATTRIBUTION.get(country),
        },
        "operators": rows,
    }

# --- CSV export -------------------------------------------------------
# Zugfinder charges EUR 3.99/mo for data export. For this project it is a
# query parameter over queries that already exist, so it is simply free.

def _csv_response(rows, filename):
    import csv as _csv
    import io as _io
    buf = _io.StringIO()
    if rows:
        writer = _csv.DictWriter(buf, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return PlainTextResponse(
        buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}.csv"'},
    )

# --- SEO surface ---
# Everything above is the JSON API behind index.html, a single client-rendered
# page with no per-entity URLs for search engines to index. These endpoints
# render plain server-side HTML per station and route, so each has a crawlable,
# shareable URL. They are deliberately minimal (no JS, inline styling) and link
# into the SPA.
#
# Absolute URLs (canonical, OG, sitemap) come from the request's Host header,
# not a hardcoded domain, since a hardcoded host once broke production.

_COUNTRY_NAMES = {"HU": "Hungary", "AT": "Austria", "PL": "Poland", "DE": "Germany"}

def _base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.headers.get('host', request.url.netloc)}"

def _seo_page(request: Request, title: str, description: str, path: str, body_html: str, json_ld: dict = None, noindex: bool = False) -> str:
    base = _base_url(request)
    canonical = f"{base}{path}"
    # Thin pages are excluded from /sitemap.xml, but a sitemap only controls
    # what we *submit* - a crawler that reaches a quiet halt by following an
    # internal link would index it anyway. This is the half of the fix that
    # actually binds. "follow" is kept so the links out of the page still
    # pass through to the pages that do have data.
    robots_tag = '<meta name="robots" content="noindex,follow">' if noindex else ""
    json_ld_tag = ""
    if json_ld is not None:
        import json as _json
        # station/route names are GTFS data, not user input, but this is
        # cheap insurance: json.dumps doesn't escape "</script>", which
        # would otherwise let a value like that break out of the tag.
        safe_json = _json.dumps(json_ld).replace("</", "<\\/")
        json_ld_tag = f'<script type="application/ld+json">{safe_json}</script>'
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_esc(title)}</title>
<meta name="description" content="{_esc(description)}">
{robots_tag}
<link rel="canonical" href="{_esc(canonical)}">
<meta property="og:type" content="website">
<meta property="og:title" content="{_esc(title)}">
<meta property="og:description" content="{_esc(description)}">
<meta property="og:url" content="{_esc(canonical)}">
<meta name="twitter:card" content="summary">
<meta name="twitter:title" content="{_esc(title)}">
<meta name="twitter:description" content="{_esc(description)}">
<style>
body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:640px;margin:2rem auto;padding:0 1rem;color:#1b1d22;line-height:1.5}}
a{{color:#1d4e89}} table{{border-collapse:collapse;width:100%;margin:1rem 0}}
td,th{{text-align:left;padding:.35rem .6rem;border-bottom:1px solid #e1e6e8}}
.cta{{display:inline-block;margin-top:1rem;padding:.6rem 1.1rem;background:#1d4e89;color:#fff;border-radius:4px;text-decoration:none}}
footer{{margin-top:2rem;font-size:.85rem;color:#565c64}}
</style>
{json_ld_tag}
</head>
<body>
{body_html}
<footer>Live delay tracking, rankings, and route comparison for Hungary, Austria, Poland, and Germany — <a href="/">open the live map</a>.</footer>
</body>
</html>"""

@app.get("/t/{country}/station/{name:path}", response_class=HTMLResponse)
def seo_station_page(request: Request, country: str, name: str):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT s.display_name,
                   COUNT(o.id) as total,
                   ROUND(100.0 * SUM(CASE WHEN o.delay_minutes <= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(o.id), 0)::numeric, 1) as pct_on_time,
                   ROUND(AVG(o.delay_minutes)::numeric, 1) as avg_delay
            FROM stations s
            LEFT JOIN delay_observations o ON o.station_id = s.id
            WHERE s.name = %s AND s.country_code = %s
            GROUP BY s.display_name
        """, (name, country))
        row = cur.fetchone()
        if not row:
            return HTMLResponse(_seo_page(request, "Station not found", "This station isn't in our data.", request.url.path, "<h1>Station not found</h1>", noindex=True), status_code=404)
        display = row["display_name"] or name
        country_name = _COUNTRY_NAMES.get(country, country)
        title = f"{display} train delays — {country_name} | LiveRail"
        if row["total"]:
            description = f"{display} ({country_name}): {row['pct_on_time']}% of trains on time, {row['avg_delay']} min average delay, based on {row['total']} tracked observations."
        else:
            description = f"Live and historical train delay tracking for {display}, {country_name}."
        body = f"""<h1>{_esc(display)}</h1>
    <p>{_esc(country_name)} — live and historical train delay tracking.</p>
    <table>
    <tr><th>Observations tracked</th><td>{row['total'] or 0}</td></tr>
    <tr><th>On-time rate (≤2 min)</th><td>{row['pct_on_time'] if row['pct_on_time'] is not None else '—'}%</td></tr>
    <tr><th>Average delay</th><td>{row['avg_delay'] if row['avg_delay'] is not None else '—'} min</td></tr>
    </table>
    <a class="cta" href="/?country={_esc(country)}&station={_esc(name)}">Open live map for {_esc(display)}</a>"""
        json_ld = {
            "@context": "https://schema.org",
            "@type": "TrainStation",
            "name": display,
            "address": {"@type": "PostalAddress", "addressCountry": country},
        }
        return _seo_page(request, title, description, request.url.path, body, json_ld,
                         noindex=(row["total"] or 0) < _SEO_MIN_OBSERVATIONS)
    finally:
        conn.close()

@app.get("/t/{country}/route/{name:path}", response_class=HTMLResponse)
def seo_route_page(request: Request, country: str, name: str):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT COUNT(*) as total,
                   ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)::numeric, 1) as pct_on_time,
                   ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
            FROM delay_observations
            WHERE route_name = %s AND country_code = %s
        """, (name, country))
        row = cur.fetchone()
        if not row or not row["total"]:
            return HTMLResponse(_seo_page(request, "Route not found", "This route isn't in our data.", request.url.path, "<h1>Route not found</h1>", noindex=True), status_code=404)
        country_name = _COUNTRY_NAMES.get(country, country)
        title = f"{name} delays — {country_name} | LiveRail"
        description = f"{name} ({country_name}): {row['pct_on_time']}% of trains on time, {row['avg_delay']} min average delay, based on {row['total']} tracked observations."
        body = f"""<h1>{_esc(name)}</h1>
    <p>{_esc(country_name)} — live and historical train delay tracking.</p>
    <table>
    <tr><th>Observations tracked</th><td>{row['total']}</td></tr>
    <tr><th>On-time rate (≤2 min)</th><td>{row['pct_on_time']}%</td></tr>
    <tr><th>Average delay</th><td>{row['avg_delay']} min</td></tr>
    </table>
    <a class="cta" href="/?country={_esc(country)}&route={_esc(name)}">Open live map for {_esc(name)}</a>"""
        json_ld = {
            "@context": "https://schema.org",
            "@type": "TrainTrip",
            "name": name,
        }
        return _seo_page(request, title, description, request.url.path, body, json_ld,
                         noindex=row["total"] < _SEO_MIN_OBSERVATIONS)
    finally:
        conn.close()

@app.get("/sitemap.xml")
def sitemap(request: Request):
    # Reads search_index, which carries a per-name observation count, so this
    # avoids the whole-table scan and gets the thin-page filter from the same rows.
    #
    # Thin pages are excluded: below _SEO_MIN_OBSERVATIONS a page has no statistic
    # worth reading. Before this filter, 10,245 of 21,168 submitted station pages had
    # zero observations. Thousands of near-identical empty pages is the
    # doorway-page pattern search engines demote a whole domain for.
    #
    # The pages are not removed or 404'd, and say plainly that data is thin. They
    # are just not submitted for indexing.
    base = _base_url(request)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        urls = [f"{base}/"]
        for country in _COUNTRY_NAMES:
            cur.execute("""
                SELECT kind, name FROM search_index
                WHERE country_code = %s AND observations >= %s
                ORDER BY observations DESC
            """, (country, _SEO_MIN_OBSERVATIONS))
            for r in cur.fetchall():
                urls.append(f"{base}/t/{country}/{r['kind']}/{_urlquote(r['name'])}")
        body = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
        body += [f"<url><loc>{_esc(u)}</loc></url>" for u in urls]
        body.append("</urlset>")
        return PlainTextResponse("\n".join(body), media_type="application/xml")
    finally:
        conn.close()

@app.get("/robots.txt")
def robots_txt(request: Request):
    base = _base_url(request)
    return PlainTextResponse(f"User-agent: *\nAllow: /\nSitemap: {base}/sitemap.xml\n")

# --- Compensation eligibility and claim assist ---
# Backed by compensation_rules, seeded with sourced EU Regulation 2021/782
# figures (schema/compensation_rules_seed.sql). No third-party claims service
# for trains exists in Europe, since passengers claim from the operator, so this
# is not a broker. It computes eligibility from a delay already visible in the
# app and helps assemble what the operator's claim form asks for. No login, no
# payment, nothing stored.

_COUNTRY_CURRENCY = {"HU": "HUF", "AT": "EUR", "DE": "EUR", "PL": "PLN"}
_COUNTRY_CLAIM_URL = {
    "HU": "https://www.mavcsoport.hu/mav-szemelyszallitas/gyik/kesesigazolas-karterites",
    "AT": "https://www.oebb.at/en/fragen-und-antworten/rechte-pflichten/fahrgastrechte",
    "DE": "https://int.bahn.de/en/help/contact/passengers-rights-claim-form",
    "PL": "https://www.intercity.pl/en/dokumenty/wniosek_miedzynarodowy.pdf",
}

def _compensation_lookup(country: str, delay_minutes: float):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT min_delay_minutes, refund_pct, source_url, verified, notes
            FROM compensation_rules
            WHERE country_code = %s AND min_delay_minutes <= %s
            ORDER BY min_delay_minutes DESC
            LIMIT 1
        """, (country, delay_minutes))
        row = cur.fetchone()
        return row
    finally:
        conn.close()

# Cancellation is not a very large delay, and the rights differ in kind. Under
# EU Regulation 2021/782, a cancellation leaving you 60+ minutes late gives a
# choice a delay does not:
#   * a refund within 30 days (plus return transport if stranded), or
#   * re-routing at the earliest opportunity at no extra cost.
# Compensation on top follows the same 25%/50% scale as a delay.
#
# Refund and re-routing are unconditional, while compensation can be refused
# for extraordinary circumstances (severe weather, sabotage). A strike is not
# one. So a rider told "no compensation" must still be told they can claim
# their money back.
# Source: https://europa.eu/youreurope/citizens/travel/passenger-rights/rail/index_en.htm
_CANCELLATION_THRESHOLD_MIN = 60
_EU_RAIL_RIGHTS_URL = "https://europa.eu/youreurope/citizens/travel/passenger-rights/rail/index_en.htm"


def _cancellation_rights(country, ticket_price=None, resulting_delay=None):
    """What a cancelled train entitles you to. See the block comment above.

    resulting_delay is how late you ended up at your final destination, which
    is what the regulation actually keys on - not the fact of cancellation. If
    the operator re-routed you and you arrived within the hour, the refund
    right does not arise, and saying otherwise would set someone up for a
    rejected claim.
    """
    result = {
        "country": country,
        "cancelled": True,
        "currency": _COUNTRY_CURRENCY.get(country),
        "claim_url": _COUNTRY_CLAIM_URL.get(country),
        "source_url": _EU_RAIL_RIGHTS_URL,
        "threshold_minutes": _CANCELLATION_THRESHOLD_MIN,
    }
    if resulting_delay is None:
        result.update({
            "eligible": None,
            "message": ("A cancelled train gives you rights once it leaves you 60+ minutes "
                        "late at your final destination. Tell us how late you actually "
                        "arrived and we can say what you can claim."),
            "needs": "resulting_delay_minutes",
        })
        return result

    result["resulting_delay_minutes"] = resulting_delay
    if resulting_delay < _CANCELLATION_THRESHOLD_MIN:
        result.update({
            "eligible": False,
            "message": (f"You were re-routed and arrived {resulting_delay:g} minutes late, "
                        "under the 60-minute threshold, so no refund or compensation "
                        "right arises from the cancellation itself."),
        })
        return result

    # The unconditional half. Stated first and separately, because it survives
    # an extraordinary-circumstances refusal that kills the compensation.
    result["choice"] = [
        {"option": "refund",
         "detail": ("A refund within 30 days - the full fare, or the part of the journey "
                    "you did not make - plus a return to where you started if the "
                    "cancellation left you stranded.")},
        {"option": "re_routing",
         "detail": ("Re-routing to your destination at the earliest opportunity under "
                    "comparable conditions, at no extra cost, or travel rescheduled to "
                    "a date that suits you.")},
    ]
    rule = _compensation_lookup(country, resulting_delay)
    result["eligible"] = True
    if rule:
        pct = float(rule["refund_pct"])
        result["refund_pct"] = pct
        result["source_url"] = rule["source_url"] or _EU_RAIL_RIGHTS_URL
        if ticket_price:
            result["estimated_refund"] = round(ticket_price * pct / 100, 2)
        result["message"] = (f"Your train was cancelled and you arrived {resulting_delay:g} "
                             f"minutes late. You can claim a refund or free re-routing, and "
                             f"{pct:g}% compensation on top.")
    else:
        result["message"] = (f"Your train was cancelled and you arrived {resulting_delay:g} "
                             "minutes late. You can claim a refund or free re-routing.")
    result["caveat"] = ("Compensation can be refused for extraordinary circumstances such as "
                        "severe weather or sabotage - but the refund and re-routing rights "
                        "hold regardless, and a strike does not count as extraordinary.")
    return result


@app.get("/compensation/check")
def compensation_check(country: str = "HU", delay_minutes: float = 0,
                       ticket_price: float = None, cancelled: bool = False,
                       resulting_delay_minutes: float = None):
    if cancelled:
        return _cancellation_rights(country, ticket_price, resulting_delay_minutes)
    rule = _compensation_lookup(country, delay_minutes)
    result = {
        "country": country,
        "delay_minutes": delay_minutes,
        "currency": _COUNTRY_CURRENCY.get(country),
        "claim_url": _COUNTRY_CLAIM_URL.get(country),
    }
    if not rule:
        result["eligible"] = False
        result["message"] = "This delay is below the compensation threshold for this country."
        return result
    result["eligible"] = True
    result["refund_pct"] = float(rule["refund_pct"])
    result["threshold_minutes"] = rule["min_delay_minutes"]
    result["source_url"] = rule["source_url"]
    result["notes"] = rule["notes"]
    if ticket_price:
        result["estimated_refund"] = round(ticket_price * float(rule["refund_pct"]) / 100, 2)
    result["message"] = f"This delay likely qualifies for a {rule['refund_pct']:g}% refund under EU passenger-rights rules."
    return result

@app.get("/compensation/claim")
def compensation_claim(country: str = "HU", route_name: str = "", station_name: str = "",
                        delay_minutes: float = 0, actual_arrival: str = None, ticket_price: float = None):
    """Assembles what the operator's own claim form asks for, from data
    this app already shows the user - not a submission, just a
    fill-in-the-blanks aid. actual_arrival is an ISO timestamp string from
    the frontend (already displayed there); scheduled time is derived by
    subtracting the delay, good enough for a claim aid, not a legal
    timestamp authority."""
    rule = _compensation_lookup(country, delay_minutes)
    scheduled = None
    if actual_arrival:
        try:
            actual_dt = datetime.fromisoformat(actual_arrival)
            scheduled = (actual_dt - timedelta(minutes=delay_minutes)).isoformat()
        except ValueError:
            pass
    summary = {
        "country": _COUNTRY_NAMES.get(country, country),
        "train": route_name,
        "station": station_name,
        "scheduled_time": scheduled,
        "actual_time": actual_arrival,
        "delay_minutes": delay_minutes,
        "eligible": bool(rule),
        "refund_pct": float(rule["refund_pct"]) if rule else None,
        "estimated_refund": round(ticket_price * float(rule["refund_pct"]) / 100, 2) if (rule and ticket_price) else None,
        "currency": _COUNTRY_CURRENCY.get(country),
        "claim_url": _COUNTRY_CLAIM_URL.get(country),
        "checklist": [
            "Train/route number and date of travel",
            "Departure and arrival station names",
            "Scheduled vs. actual arrival time (shown above)",
            "Your ticket (or booking reference, if bought online)",
            "Bank details or voucher preference, if the claim is approved",
        ],
    }
    return summary

@app.get("/partner-links")
def get_partner_links(country: str = "HU", kind: str = None):
    """Active sponsored/partner links for a country, so the frontend can
    render a real "book tickets" / "file a claim" CTA without hardcoding
    URLs client-side - swapping a target_url (e.g. once a real Trainline/
    Omio affiliate account exists) never needs a frontend deploy."""
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if kind:
            cur.execute("""
                SELECT id, kind, label FROM partner_links
                WHERE active AND scope = 'global' AND (country_code = %s OR country_code IS NULL) AND kind = %s
            """, (country, kind))
        else:
            cur.execute("""
                SELECT id, kind, label FROM partner_links
                WHERE active AND scope = 'global' AND (country_code = %s OR country_code IS NULL)
            """, (country,))
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        conn.close()

@app.get("/out/{link_id}")
def outbound_click(link_id: int, request: Request):
    """Logs the click (for affiliate reporting) then redirects. Anonymous -
    no login exists yet, user_id stays NULL, same as any real affiliate
    program's non-logged-in click flow."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT target_url FROM partner_links WHERE id = %s AND active", (link_id,))
        row = cur.fetchone()
        if not row:
            return PlainTextResponse("Link not found", status_code=404)
        referer = request.headers.get("referer", "")
        cur.execute(
            "INSERT INTO outbound_clicks (partner_link_id, referer_path) VALUES (%s, %s)",
            (link_id, referer),
        )
        conn.commit()
        return RedirectResponse(row[0], status_code=302)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Delay alerts (Web Push)
#
# No login: a browser's push subscription is its identity (see schema/017).
#
# Every endpoint here is a POST, including the one that only reads. A push
# endpoint is a capability URL, so putting it in a query string would write it
# into nginx's access log and every intermediary's.
# ---------------------------------------------------------------------------
from pydantic import BaseModel
from typing import Optional
import config as _cfg
import push_alerts as _push


class _PushKeys(BaseModel):
    p256dh: str
    auth: str


class _WatchBody(BaseModel):
    endpoint: str
    keys: _PushKeys
    country: str
    train_id: str
    label: Optional[str] = None
    station: Optional[str] = None
    hours: Optional[int] = 24


class _EndpointBody(BaseModel):
    endpoint: str
    country: Optional[str] = None
    train_id: Optional[str] = None


@app.get("/push/key")
def push_key():
    """The VAPID public key the browser needs to subscribe. Public by design.
    `enabled: false` lets the frontend hide the whole feature rather than
    offering a button that cannot work."""
    return {"enabled": _cfg.PUSH_ENABLED, "public_key": _cfg.VAPID_PUBLIC_KEY}


def _upsert_subscription(cur, body):
    cur.execute("""
        INSERT INTO push_subscription (endpoint, p256dh, auth)
        VALUES (%s,%s,%s)
        ON CONFLICT (endpoint) DO UPDATE
          SET p256dh = EXCLUDED.p256dh, auth = EXCLUDED.auth,
              disabled_at = NULL, failures = 0
        RETURNING id
    """, (body.endpoint, body.keys.p256dh, body.keys.auth))
    return cur.fetchone()[0]


@app.post("/push/watch")
def push_watch(body: _WatchBody):
    """Start alerting this browser about one train."""
    if not _cfg.PUSH_ENABLED:
        raise HTTPException(status_code=503, detail="Push alerts are not configured")
    if body.country not in ("HU", "AT", "PL", "DE"):
        raise HTTPException(status_code=400, detail="Unknown country")
    # Clamped rather than trusted. A watch outliving its journey would alert
    # about a different train wearing a recycled id - see schema/017.
    hours = max(1, min(int(body.hours or 24), 48))
    conn = get_conn()
    cur = conn.cursor()
    try:
        sub_id = _upsert_subscription(cur, body)
        cur.execute("""
            INSERT INTO push_watch (subscription_id, country_code, train_id,
                                    label, station, expires_at)
            VALUES (%s,%s,%s,%s,%s, now() + make_interval(hours => %s))
            ON CONFLICT (subscription_id, country_code, train_id) DO UPDATE
              SET label = EXCLUDED.label, station = EXCLUDED.station,
                  expires_at = EXCLUDED.expires_at
            RETURNING id, expires_at
        """, (sub_id, body.country, body.train_id, body.label, body.station, hours))
        wid, expires = cur.fetchone()
        conn.commit()
        return {"watching": True, "watch_id": wid, "expires_at": expires.isoformat(),
                "note": "Alerts stop when this expires. Train ids are reissued "
                        "between service days, so a watch is for one journey."}
    finally:
        cur.close(); conn.close()


@app.post("/push/unwatch")
def push_unwatch(body: _EndpointBody):
    """Stop alerting about one train, or - with no train_id - about everything."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        if body.train_id and body.country:
            cur.execute("""DELETE FROM push_watch w USING push_subscription s
                           WHERE w.subscription_id = s.id AND s.endpoint = %s
                             AND w.country_code = %s AND w.train_id = %s""",
                        (body.endpoint, body.country, body.train_id))
        else:
            cur.execute("""DELETE FROM push_watch w USING push_subscription s
                           WHERE w.subscription_id = s.id AND s.endpoint = %s""",
                        (body.endpoint,))
        n = cur.rowcount
        conn.commit()
        return {"removed": n}
    finally:
        cur.close(); conn.close()


@app.post("/push/watches")
def push_watches(body: _EndpointBody):
    """What this browser is currently watching, with each train's last known
    state so the UI can show it without a second round of calls."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT w.id, w.country_code, w.train_id, w.label, w.station, w.expires_at
            FROM push_watch w JOIN push_subscription s ON s.id = w.subscription_id
            WHERE s.endpoint = %s AND w.expires_at > now()
            ORDER BY w.created_at DESC
        """, (body.endpoint,))
        out = []
        for wid, cc, tid, label, station, expires in cur.fetchall():
            state = _push.latest_state(cur, cc, tid, station)
            out.append({"watch_id": wid, "country": cc, "train_id": tid,
                        "label": label, "station": station,
                        "expires_at": expires.isoformat(),
                        "delay_minutes": state.get("delay"),
                        "is_cancelled": state.get("cancelled", False),
                        "current_stop": state.get("stop"),
                        "found": state.get("found", False)})
        return {"watches": out}
    finally:
        cur.close(); conn.close()
