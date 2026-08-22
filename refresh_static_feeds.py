"""Re-import every country's static GTFS, and merge the stations it splits.

WHY THIS EXISTS. Static feeds drift away from the live ones, silently, and it
has cost this project real coverage three times:

  * 2026-08-08..19  Germany lost 99.7% of its observations for twelve days.
    The poller logged "Resolved 6/5724" every five minutes with no error and
    no exception. It recovered only when someone happened to re-import.
  * 2026-08-22      Germany again: gtfs.de rotated essentially every regional
    stop_id, 362 of 16,315 surviving. Resolution fell to 0.5%.
  * Poland          ran at 70.9% resolution for about three weeks - never
    alerting, because its floor is 50% - while 29% of its trains went
    unrecorded. Every one of the 1,912 missing trips was in the current feed.

monitor.py now DETECTS this. Nothing PREVENTS it. This does.

THE HARD PART IS NOT THE DOWNLOAD. Re-importing is itself what splits the
station table: `stations` is keyed UNIQUE (country_code, gtfs_stop_id), and a
feed that reissues ids presents every station as a new one, so the import
inserts a second row rather than updating the first. That is what
schema/007 and schema/009 were both written to repair by hand. A monthly job
that re-imports without merging would create that damage twelve times a year
instead of fixing anything, so the merge is part of this, not a follow-up.

MERGE DIRECTION IS DERIVED, NEVER ASSUMED. schema/007 kept the old row
because it held every stop_times row; schema/009 could not use that rule
because by then both sides held stop_times, and fell back to history. Both
decisions were made by measuring first. So this measures too: for each
duplicate group it keeps whichever row carries more observations - moving the
smaller set is both cheaper and keeps the busier station page continuous -
and then adopts the gtfs_stop_id the CURRENT feed publishes, because that is
the id the poller will look up tomorrow.

SAFETY. Nothing is deleted until its children are moved. A group with three or
more rows aborts the merge rather than half-merging it, which is the guard
schema/009 also carries. Dry run by default.

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

from config import DB_PASSWORD

WORK = os.path.expanduser("~/gtfs_refresh")

# Each entry: the zip to fetch, the directory to import from, and the extra
# import.py flags that country needs. The prefixes matter enormously for
# Germany: it is assembled from two zips that number routes AND services
# independently, and 861 service_ids collide between them with different week
# patterns (see import.py --service-id-prefix).
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
    # HU has no published static URL in this project's history - the feed was
    # placed on disk by hand. Left out rather than guessed at; its directory is
    # still merged below if someone refreshes it.
}

WANTED = ("stops.txt", "routes.txt", "trips.txt", "stop_times.txt",
          "calendar.txt", "calendar_dates.txt")


def connect():
    return psycopg2.connect(dbname="train_delay_db", user="trainuser",
                            password=DB_PASSWORD, host="localhost")


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
    cmd = [sys.executable, os.path.expanduser("~/import.py"),
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
        cur.execute("""
            SELECT count(*) FROM (
              SELECT 1 FROM stations WHERE country_code=%s
              GROUP BY name, lat, lon HAVING count(*) > 2) x
        """, (country,))
        if cur.fetchone()[0]:
            print(f"  ABORT: groups with 3+ rows exist - merge by hand, do not half-merge")
            return 0
        cur.execute("""
            SELECT array_agg(id ORDER BY id) FROM stations
            WHERE country_code=%s AND lat IS NOT NULL
            GROUP BY name, lat, lon HAVING count(*) = 2
        """, (country,))
        groups = [r[0] for r in cur.fetchall()]
        if not groups:
            print("  no duplicate stations")
            return 0
        print(f"  {len(groups)} duplicate station groups")
        moved = 0
        for a, b in groups:
            # Keep whichever row carries more history: moving the smaller set
            # is cheaper and keeps the busier station page continuous.
            cur.execute("""SELECT station_id, count(*) FROM delay_observations
                           WHERE station_id = ANY(%s) GROUP BY 1""", ([a, b],))
            counts = dict(cur.fetchall())
            keep, drop = (a, b) if counts.get(a, 0) >= counts.get(b, 0) else (b, a)
            # Adopt the id the CURRENT feed publishes - the higher row id is
            # the one this import just wrote, so it is the live one.
            cur.execute("SELECT gtfs_stop_id FROM stations WHERE id=%s", (max(a, b),))
            live_gtfs_id = cur.fetchone()[0]
            if not write:
                moved += 1
                continue
            cur.execute("UPDATE stop_times SET station_id=%s WHERE station_id=%s", (keep, drop))
            cur.execute("UPDATE delay_observations SET station_id=%s WHERE station_id=%s", (keep, drop))
            cur.execute("DELETE FROM stations WHERE id=%s", (drop,))
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
