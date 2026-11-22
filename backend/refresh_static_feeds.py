"""Re-import every country's static GTFS, and merge the stations it splits.

Static feeds drift away from the live ones without any error. When gtfs.de
reissued its stop ids, Germany's resolution collapsed for days, and Poland ran
with a large share of trips unrecorded for weeks. monitor.py detects drift;
this job prevents it by re-importing every month.

The hard part is the merge, not the download. `stations` is unique on
(country_code, gtfs_stop_id), so a feed that reissues ids makes every station
look new and the import inserts a second row instead of updating the first.
Re-importing without merging would split the station table every month, so
the merge is part of this job.

For each duplicate group it keeps the row with more observations (moving the
smaller set is cheaper and keeps the busier station page continuous) and
adopts the gtfs_stop_id the current feed publishes, since that is the id the
poller will look up next.

Nothing is deleted until its dependent rows are moved, and the dependent
tables are read from pg_constraint, so a table added later stops the merge
with a message instead of breaking it halfway. Dry run by default.

    python3 refresh_static_feeds.py --country PL            # report only
    python3 refresh_static_feeds.py --country PL --write
    python3 refresh_static_feeds.py --all --write           # what cron runs
"""
import argparse
import os
import subprocess
import sys
import time
import zipfile

import psycopg2

from config import DB_CONFIG

WORK = os.path.expanduser("~/gtfs_refresh")
# A tripwire for real corruption, not for ordinary feed churn - see
# merge_stations. Three rounds of re-imports produced exactly one triple.
MAX_GROUP_SIZE = 6

# Tables whose station_id a merge must repoint at the surviving row, and tables
# whose rows are derived and can simply be dropped.
#
# merge_stations reads the tables that actually reference stations from the
# Postgres catalog and refuses to run if one is in neither set, so a table
# added later gets a clear refusal instead of a merge that fails halfway
# through.
REPOINT_TABLES = {"stop_times", "delay_observations"}
# delay_distribution is rebuilt per country by api.refresh_delay_distribution.
# Merging two stations' rows would concatenate percentiles, which is
# meaningless; dropping them lets the next rebuild compute them from the merged
# history.
DERIVED_TABLES = {"delay_distribution"}

# Each entry: the zip to fetch, the directory to import into, and that
# country's extra import.py flags. Germany is assembled from two zips that
# number routes and services independently, so the prefixes keep their ids
# apart (see import.py --service-id-prefix).
FEEDS = {
    "AT": [dict(url="https://static.web.oebb.at/open-data/soll-fahrplan-gtfs/GTFS_Fahrplan_2026.zip",
                dest="gtfs_at", flags=["--with-stop-times"])],
    "PL": [dict(url="https://mkuran.pl/gtfs/polish_trains.zip",
                dest="gtfs_pl", flags=["--with-stop-times"])],
    "DE": [dict(url="https://download.gtfs.de/germany/fv_free/latest.zip",
                dest="gtfs_de_fv", flags=["--route-id-prefix", "fv_",
                                          "--service-id-prefix", "fv_", "--with-stop-times"]),
           dict(url="https://download.gtfs.de/germany/rv_free/latest.zip",
                dest="gtfs_de_rv", flags=["--route-id-prefix", "rv_",
                                          "--service-id-prefix", "rv_", "--with-stop-times"])],
    # Hungary's static feed has no stable download URL, so it is refreshed by
    # hand. Its directory is still merged below if present.
}

WANTED = ("stops.txt", "routes.txt", "trips.txt", "stop_times.txt",
          "calendar.txt", "calendar_dates.txt")


def connect():
    return psycopg2.connect(**DB_CONFIG)


def fetch(url, dest):
    os.makedirs(WORK, exist_ok=True)
    os.makedirs(os.path.expanduser(f"~/{dest}"), exist_ok=True)
    zpath = os.path.join(WORK, dest + ".zip")
    print(f"  downloading {url}")
    r = subprocess.run(["curl", "-sSL", "--max-time", "1800", "-o", zpath, url])
    if r.returncode != 0:
        raise RuntimeError(f"download failed for {url}")
    size = os.path.getsize(zpath)
    print(f"  {size/1e6:.1f} MB")
    with zipfile.ZipFile(zpath) as z:
        for n in WANTED:
            if n in z.namelist():
                z.extract(n, os.path.expanduser(f"~/{dest}"))
    os.remove(zpath)          # ~200 MB of zips otherwise accumulate monthly
    return size


def run_import(country, dest, flags):
    cmd = [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "import.py"),
           "--country", country, "--gtfs-dir", os.path.expanduser(f"~/{dest}")] + flags
    print(f"  importing {dest}")
    r = subprocess.run(cmd, capture_output=True, text=True)
    for line in r.stdout.splitlines():
        if line.startswith("Imported") or line.startswith("Done"):
            print(f"    {line}")
    if r.returncode != 0:
        print(f"    FAILED: {r.stderr.strip()[:300]}")
    return r.returncode == 0


def merge_stations(country, write=False):
    """Collapse stations the re-import duplicated. See the module docstring."""
    conn = connect()
    cur = conn.cursor()
    try:
        # Merge groups of any size, not only pairs: merging N rows is the same
        # operation repeated. The size ceiling only guards against genuine
        # corruption, not ordinary feed churn.
        cur.execute("""
            SELECT DISTINCT c.conrelid::regclass::text
            FROM pg_constraint c
            WHERE c.contype='f' AND c.confrelid='stations'::regclass
        """)
        dependents = {r[0] for r in cur.fetchall()}
        unknown = dependents - REPOINT_TABLES - DERIVED_TABLES
        if unknown:
            print(f"  ABORT: {', '.join(sorted(unknown))} reference(s) stations and this "
                  f"script does not know whether to repoint or drop them.\n"
                  f"  Add each to REPOINT_TABLES or DERIVED_TABLES, then re-run.")
            return 0
        repoint = sorted(dependents & REPOINT_TABLES)
        derived = sorted(dependents & DERIVED_TABLES)

        cur.execute("""
            SELECT count(*) FROM (
              SELECT 1 FROM stations WHERE country_code=%s
              GROUP BY name, lat, lon HAVING count(*) > %s) x
        """, (country, MAX_GROUP_SIZE))
        oversized = cur.fetchone()[0]
        if oversized:
            print(f"  ABORT: {oversized} group(s) with more than {MAX_GROUP_SIZE} rows - "
                  f"that is not feed churn, look before merging")
            return 0
        cur.execute("""
            SELECT array_agg(id ORDER BY id) FROM stations
            WHERE country_code=%s AND lat IS NOT NULL
            GROUP BY name, lat, lon HAVING count(*) > 1
        """, (country,))
        groups = [r[0] for r in cur.fetchall()]
        if not groups:
            print("  no duplicate stations")
            return 0
        print(f"  {len(groups)} duplicate station groups")
        print(f"  repoint: {', '.join(repoint) or 'none'}   drop derived: {', '.join(derived) or 'none'}")
        moved = 0
        for ids in groups:
            # Keep whichever row carries the most history: moving the smaller
            # sets is cheaper and keeps the busiest station page continuous.
            cur.execute("""SELECT station_id, count(*) FROM delay_observations
                           WHERE station_id = ANY(%s) GROUP BY 1""", (ids,))
            counts = dict(cur.fetchall())
            keep = max(ids, key=lambda i: (counts.get(i, 0), i))
            drops = [i for i in ids if i != keep]
            # Adopt the id the CURRENT feed publishes. The highest row id is
            # the one the most recent import wrote, so it is the live one.
            cur.execute("SELECT gtfs_stop_id FROM stations WHERE id=%s", (max(ids),))
            live_gtfs_id = cur.fetchone()[0]
            if not write:
                moved += 1
                continue
            for tbl in repoint:
                cur.execute(f"UPDATE {tbl} SET station_id=%s WHERE station_id = ANY(%s)",
                            (keep, drops))
            for tbl in derived:
                cur.execute(f"DELETE FROM {tbl} WHERE station_id = ANY(%s)", (drops,))
            cur.execute("DELETE FROM stations WHERE id = ANY(%s)", (drops,))
            cur.execute("UPDATE stations SET gtfs_stop_id=%s WHERE id=%s", (live_gtfs_id, keep))
            moved += 1
        if write:
            conn.commit()
        return moved
    finally:
        cur.close()
        conn.close()


def station_count(country):
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT count(*) FROM stations WHERE country_code=%s", (country,))
        return cur.fetchone()[0]
    finally:
        cur.close()
        conn.close()


def refresh(country, write=False, skip_download=False):
    print(f"\n=== {country} ===")
    before = station_count(country)
    for feed in FEEDS.get(country, []):
        if not skip_download:
            try:
                fetch(feed["url"], feed["dest"])
            except Exception as exc:
                print(f"  download failed, keeping existing files: {exc}")
        if write:
            run_import(country, feed["dest"], feed["flags"])
        else:
            print(f"  would import {feed['dest']} {' '.join(feed['flags'])}")
    n = merge_stations(country, write=write)
    after = station_count(country)
    print(f"  stations {before} -> {after}   merged {n} group(s){'' if write else ' (dry run)'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--country")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--skip-download", action="store_true",
                    help="re-import and merge from files already on disk")
    ap.add_argument("--merge-only", action="store_true")
    a = ap.parse_args()
    targets = list(FEEDS) if a.all else ([a.country] if a.country else [])
    if not targets:
        ap.error("pass --country XX or --all")
    t0 = time.time()
    for c in targets:
        if a.merge_only:
            print(f"\n=== {c} (merge only) ===")
            before = station_count(c)
            n = merge_stations(c, write=a.write)
            print(f"  stations {before} -> {station_count(c)}   merged {n} group(s)")
        else:
            refresh(c, write=a.write, skip_download=a.skip_download)
    print(f"\ntotal {time.time()-t0:.0f}s{'' if a.write else '   DRY RUN - pass --write to apply'}")


if __name__ == "__main__":
    main()
