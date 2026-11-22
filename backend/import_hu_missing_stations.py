"""Add the HU stations MÁV's live feed reports but its static export omits.

WHY THIS EXISTS. A coverage audit on 2026-08-22 found 23 of 296 Hungarian
observations per poll - 7.8% - being silently discarded, all of them on HÉV
lines (H5 Szentendre, H6 Ráckeve, H7 Csepel, H8/H9 Gödöllő). The Budapest
suburban railway is a different agency from MÁV and is not exported alongside
MÁV/GYSEV/Gyermekvasút in the static GTFS, so its stations - "Aquincum H",
"Batthyány tér H", "Csepel H" and 16 others - exist in no table we hold.

poller.py's INSERT is `SELECT s.id ... FROM stations s WHERE s.name = %s`, so
an unknown station inserts zero rows and reports success. The trains were being
observed, parsed, and thrown away.

MÁV's own OTP endpoint does know these stops - the same host the poller already
polls - with real coordinates and a vehicleMode of SUBURBAN_RAILWAY. So they
are imported from the operator's own API rather than invented from vehicle
positions, which would have put a station wherever a train happened to be.

Only RAIL and SUBURBAN_RAILWAY stops are taken. OTP also returns BUS stops
sharing the same name ("Budapest, Aquincum H" is three separate bus stops), and
importing those would put buses into a rail-only database.

Idempotent: re-running inserts nothing new. Safe to run whenever the audit
below reports unmatched names.

    python3 import_hu_missing_stations.py            # report only
    python3 import_hu_missing_stations.py --write    # actually insert
"""
import sys

import psycopg2
import requests

from config import DB_CONFIG
from poller import GRAPHQL_URL, QUERY, parse_vehicles
from station_names import clean_display_name

COUNTRY = "HU"
# The modes that belong in a rail database. OTP returns bus stops under the
# same names, and this is the line that keeps them out.
RAIL_MODES = {"RAIL", "SUBURBAN_RAILWAY"}

STOP_QUERY = '{ stops(name: "%s") { gtfsId name lat lon vehicleMode } }'


def unmatched_station_names(cur):
    """Station names the live feed reports that we cannot resolve."""
    data = requests.post(GRAPHQL_URL, json={"query": QUERY}, timeout=40).json()
    rows = parse_vehicles(data)
    names = sorted({r["stop_name"] for r in rows if r.get("stop_name")})
    if not names:
        return [], rows
    cur.execute(
        "SELECT name FROM stations WHERE country_code = %s AND name = ANY(%s)",
        (COUNTRY, names),
    )
    known = {r[0] for r in cur.fetchall()}
    return [n for n in names if n not in known], rows


def lookup_stop(name):
    """Ask MÁV's OTP for a rail stop with exactly this name."""
    # The name is interpolated because OTP's stops(name:) takes a plain string
    # and this runs against a fixed, trusted endpoint with names that came
    # from that same endpoint moments earlier. Quotes are stripped rather than
    # escaped so a malformed name cannot break out of the literal.
    safe = name.replace('"', "").replace("\\", "")
    resp = requests.post(GRAPHQL_URL, json={"query": STOP_QUERY % safe}, timeout=40)
    stops = (resp.json().get("data") or {}).get("stops") or []
    for s in stops:
        if s.get("name") == name and s.get("vehicleMode") in RAIL_MODES:
            return s
    return None


def main():
    write = "--write" in sys.argv
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    missing, rows = unmatched_station_names(cur)
    lost = [r for r in rows if r["stop_name"] in set(missing)]
    print(f"live rows: {len(rows)} | unresolvable: {len(lost)} "
          f"({100 * len(lost) / max(len(rows), 1):.1f}%) across {len(missing)} stations")

    added = skipped = 0
    for name in missing:
        stop = lookup_stop(name)
        if not stop:
            print(f"  no rail stop in OTP for {name!r} - left alone")
            skipped += 1
            continue
        if write:
            cur.execute("""
                INSERT INTO stations (name, gtfs_stop_id, lat, lon, country_code, display_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (country_code, gtfs_stop_id) DO NOTHING
            """, (name, stop["gtfsId"], stop.get("lat"), stop.get("lon"),
                  COUNTRY, clean_display_name(name, COUNTRY)))
            added += cur.rowcount
        else:
            added += 1
        print(f"  {'+' if write else '?'} {name}  {stop['gtfsId']}  "
              f"{stop.get('lat')},{stop.get('lon')}  {stop.get('vehicleMode')}")
    if write:
        conn.commit()
    cur.close()
    conn.close()
    print(f"{'inserted' if write else 'would insert'}: {added}, no rail match: {skipped}")
    if not write:
        print("dry run - pass --write to apply")


if __name__ == "__main__":
    main()
