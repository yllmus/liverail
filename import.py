import argparse
import os
import pandas as pd
import psycopg2

from station_names import clean_display_name

from config import DB_PASSWORD


def _gtfs_date(value):
    # GTFS dates are "YYYYMMDD" strings; NULL through if absent so an open-
    # ended service doesn't become a bogus date.
    if not value or not str(value).strip():
        return None
    text = str(value).strip()
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}" if len(text) == 8 else None

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": DB_PASSWORD,
    "host": "localhost"
}

parser = argparse.ArgumentParser(description="Import a GTFS static feed into train_delay_db.")
parser.add_argument("--country", default="HU", help="2-letter country code this feed belongs to (default: HU)")
parser.add_argument("--gtfs-dir", default="gtfs", help="Directory containing stops.txt/routes.txt/trips.txt (default: gtfs)")
parser.add_argument("--with-stop-times", action="store_true", help=(
    "Also import stop_times.txt. Opt-in and off by default: it's a much "
    "bigger import (100k+ rows) that HU/AT don't need since they resolve "
    "live vehicles by name/id directly; feeds like Poland's GTFS-RT (which "
    "only exposes stop_sequence + absolute times, no stop_id/delay) need "
    "scheduled stop_times to resolve stations and compute delay."
))
parser.add_argument("--route-id-prefix", default="", help=(
    "Prefix applied to gtfs_route_id on insert and on the trips->routes "
    "lookup (stops/trips ids are left alone). Needed when a single country "
    "is assembled from more than one GTFS extract whose route_id values "
    "were independently assigned and can collide: Germany's gtfs.de feed "
    "ships long-distance (fv_free) and regional (rv_free) rail as separate "
    "zips, and confirmed against the real data, their route_id namespaces "
    "are NOT shared - the same small integer names a different route in "
    "each file (e.g. route_id 10 is 'RJ' in fv_free but 'RE6' in rv_free), "
    "unlike stop_id and trip_id, which genuinely are shared/global across "
    "both files (verified: all 830 overlapping stop_ids agree on name). "
    "Importing both feeds into country_code='DE' without this would let "
    "ON CONFLICT DO NOTHING silently keep whichever route a colliding id "
    "saw first and mis-map every trip referencing the id from the other "
    "feed. Default '' keeps HU/AT/PL (each single-feed) unaffected."
))
parser.add_argument("--backfill-display-names", action="store_true", help=(
    "One-time (but safe to re-run) backfill: compute stations.display_name "
    "for existing rows via station_names.clean_display_name(). Pure DB "
    "operation, no GTFS files touched - ignores --gtfs-dir/--with-stop-times/"
    "--route-id-prefix entirely. Scope with --backfill-country to run one "
    "country at a time; omit it to cover all countries in the countries table."
))
parser.add_argument("--backfill-country", default=None, help=(
    "Restrict --backfill-display-names to a single country code. Only "
    "meaningful together with --backfill-display-names."
))
args = parser.parse_args()
country = args.country.upper()
gtfs_dir = args.gtfs_dir

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

if args.backfill_display_names:
    codes = [args.backfill_country.upper()] if args.backfill_country else None
    if codes is None:
        cur.execute("SELECT code FROM countries ORDER BY code")
        codes = [r[0] for r in cur.fetchall()]
    grand_total = grand_set = grand_null = grand_unchanged = 0
    for code in codes:
        cur.execute("SELECT id, name FROM stations WHERE country_code = %s", (code,))
        rows = cur.fetchall()
        set_count = null_count = unchanged_count = 0
        for station_id, name in rows:
            dn = clean_display_name(name, code)
            cur.execute("UPDATE stations SET display_name = %s WHERE id = %s", (dn, station_id))
            if dn is None:
                null_count += 1
            elif dn == name:
                unchanged_count += 1
            else:
                set_count += 1
        conn.commit()
        print(f"{code}: {len(rows)} stations - {set_count} cleaned, "
              f"{unchanged_count} unchanged (already clean), "
              f"{null_count} filtered to NULL (non-rail)")
        grand_total += len(rows); grand_set += set_count
        grand_null += null_count; grand_unchanged += unchanged_count
    print(f"TOTAL: {grand_total} stations - {grand_set} cleaned, "
          f"{grand_unchanged} unchanged, {grand_null} filtered")
    cur.close()
    conn.close()
    raise SystemExit(0)

cur.execute("SELECT 1 FROM countries WHERE code = %s", (country,))
if not cur.fetchone():
    raise SystemExit(
        f"Country '{country}' is not in the countries table yet. "
        f"Add it first: INSERT INTO countries (code, name) VALUES ('{country}', '<name>');"
    )

print(f"Importing stops for {country}...")
stops = pd.read_csv(f"{gtfs_dir}/stops.txt", dtype=str)
# Child stop_id -> parent (top-level) stop_id, built from the raw file
# before filtering below. Needed by --with-stop-times: confirmed against
# DE's static stop_times.txt that it references only platform-level child
# stop_ids (667/667 in a sample), never the top-level ids `stations` keeps
# - the same mismatch germany_poller.py's load_parent_map() already works
# around for the *live* feed, but the *static* import path never got the
# equivalent fix until now. Harmless where it isn't needed (HU doesn't use
# --with-stop-times; PL's own stop_times mostly references top-level ids
# directly, so this dict mostly stays unused for PL rows) and strictly
# improves PL's existing ~5% unresolved rate for the child ids it does hit.
parent_of = {}
if "parent_station" in stops.columns:
    for _, row in stops.iterrows():
        parent = row.get("parent_station")
        if isinstance(parent, str) and parent.strip():
            parent_of[row["stop_id"]] = parent.strip()
    # A row with a parent_station is a child (platform/entrance/node) of
    # another row in this same file — importing it too would duplicate a
    # single delay event across every platform of a station. Import only
    # the top-level rows (no parent_station), regardless of what
    # location_type value the feed happens to use for them: MAV's feed
    # never sets parent_station at all (nothing filtered, same as before);
    # OBB's is dominated by location_type=1 parents; Poland's is dominated
    # by *unparented* location_type=0 halts with only a few location_type=1
    # hub stations — location_type alone isn't a reliable signal across
    # feeds, but "does this row have a parent" is, by GTFS spec design.
    stops = stops[stops["parent_station"].isna() | (stops["parent_station"].str.strip() == "")]
for _, row in stops.iterrows():
    cur.execute("""
        INSERT INTO stations (name, gtfs_stop_id, lat, lon, country_code, display_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (country_code, gtfs_stop_id) DO NOTHING
    """, (row["stop_name"], row["stop_id"], row.get("stop_lat"), row.get("stop_lon"), country,
          clean_display_name(row["stop_name"], country)))
conn.commit()
print(f"Imported {len(stops)} stops.")

def _clean(v):
    # pandas represents a blank CSV field as float NaN, not "", and NaN is
    # truthy in Python — `v or fallback` never falls through, so a blank
    # route_short_name silently wins over a real route_long_name.
    #
    # Second, separate bug fixed here: this used to check v.strip() for
    # truthiness but return the *unstripped* v, so a non-blank value with
    # stray whitespace (e.g. "[Pótló 41011 ]") passed through untouched —
    # 4248/11132 (38%) of HU routes.name carried a trailing space because
    # of this. Returning v.strip() fixes it; re-running import.py backfills
    # existing rows since routes/trips upsert with ON CONFLICT DO UPDATE.
    return v.strip() if isinstance(v, str) and v.strip() else None

# agency.txt is optional per feed (not every extracted gtfs_dir has one —
# AT's/PL's don't, since only stops/routes/trips were pulled from their
# zips) and agency is a route-level GTFS concept, not a trip-level one, so
# this becomes routes.agency_name rather than anything on trips. Missing
# file or missing agency_id just leaves agency_name NULL, same graceful-skip
# convention as everything else here.
agency_path = f"{gtfs_dir}/agency.txt"
agency_names = {}
if os.path.exists(agency_path):
    agency = pd.read_csv(agency_path, dtype=str)
    for _, row in agency.iterrows():
        name = _clean(row.get("agency_name"))
        if name and pd.notna(row.get("agency_id")):
            agency_names[row["agency_id"]] = name

print(f"Importing routes for {country}...")
routes = pd.read_csv(f"{gtfs_dir}/routes.txt", dtype=str)
for _, row in routes.iterrows():
    short_name = _clean(row.get("route_short_name"))
    long_name = _clean(row.get("route_long_name"))
    name = short_name or long_name
    # Only keep long_name as a separate friendly-name field when it's real
    # extra information - i.e. short_name won the `name` slot above. When
    # short_name was blank and long_name became `name` itself, storing it
    # again here would just be the same string twice (see item 7's
    # 004_route_long_name.sql for why this column exists at all: AT/PL's
    # route_long_name is a real "origin - destination" corridor description
    # that route_short_name-preferring `name` was discarding entirely).
    friendly = long_name if (short_name and long_name and long_name != short_name) else None
    agency_name = agency_names.get(row.get("agency_id"))
    cur.execute("""
        INSERT INTO routes (name, gtfs_route_id, route_type, country_code, agency_name, long_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (country_code, gtfs_route_id) DO UPDATE SET
            name = EXCLUDED.name, route_type = EXCLUDED.route_type, agency_name = EXCLUDED.agency_name,
            long_name = EXCLUDED.long_name
    """, (name, args.route_id_prefix + row["route_id"], row.get("route_type"), country, agency_name, friendly))
conn.commit()
print(f"Imported {len(routes)} routes.")

print(f"Importing trips for {country}...")
trips = pd.read_csv(f"{gtfs_dir}/trips.txt", dtype=str)
for _, row in trips.iterrows():
    headsign = _clean(row.get("trip_headsign"))
    cur.execute("""
        INSERT INTO trips (route_id, gtfs_trip_id, country_code, headsign, service_id, trip_short_name)
        SELECT r.id, %s, %s, %s, %s, %s FROM routes r WHERE r.gtfs_route_id = %s AND r.country_code = %s
        ON CONFLICT (country_code, gtfs_trip_id) DO UPDATE SET
            headsign = EXCLUDED.headsign, route_id = EXCLUDED.route_id,
            service_id = EXCLUDED.service_id,
            trip_short_name = EXCLUDED.trip_short_name
    """, (row["trip_id"], country, headsign, _clean(row.get("service_id")),
          _clean(row.get("trip_short_name")),
          args.route_id_prefix + row["route_id"], country))
conn.commit()
print(f"Imported {len(trips)} trips.")

# calendar.txt: which days each service_id actually runs. Needed because a
# train number alone matches many candidate trips that are the *same* service
# duplicated per calendar pattern - identical scheduled times, so nothing but
# the calendar can separate them (see schema/002_service_calendar.sql).
# Optional: not every feed ships calendar.txt, and a missing one degrades
# resolution rather than failing the import.
_calendar_path = f"{gtfs_dir}/calendar.txt"
if os.path.exists(_calendar_path):
    print(f"Importing calendar for {country}...")
    calendar = pd.read_csv(_calendar_path, dtype=str)
    _days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    for _, row in calendar.iterrows():
        cur.execute(f"""
            INSERT INTO service_calendar
                (country_code, service_id, {', '.join(_days)}, start_date, end_date)
            VALUES (%s, %s, {', '.join(['%s'] * 7)}, %s, %s)
            ON CONFLICT (country_code, service_id) DO UPDATE SET
                {', '.join(f'{d} = EXCLUDED.{d}' for d in _days)},
                start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date
        """, (country, row["service_id"], *[row.get(d) == "1" for d in _days],
              _gtfs_date(row.get("start_date")), _gtfs_date(row.get("end_date"))))
    conn.commit()
    print(f"Imported {len(calendar)} calendar entries.")
else:
    print(f"No calendar.txt in {gtfs_dir}, skipping calendar import.")

if args.with_stop_times:
    print(f"Importing stop_times for {country}...")
    stop_times = pd.read_csv(f"{gtfs_dir}/stop_times.txt", dtype=str)
    inserted = 0
    for _, row in stop_times.iterrows():
        resolved_stop_id = parent_of.get(row["stop_id"], row["stop_id"])
        cur.execute("""
            INSERT INTO stop_times (trip_id, station_id, scheduled_arrival, scheduled_departure, stop_sequence)
            SELECT t.id, s.id, %s, %s, %s
            FROM trips t, stations s
            WHERE t.gtfs_trip_id = %s AND t.country_code = %s
              AND s.gtfs_stop_id = %s AND s.country_code = %s
            ON CONFLICT (trip_id, stop_sequence) DO NOTHING
        """, (
            row.get("arrival_time"), row.get("departure_time"), row["stop_sequence"],
            row["trip_id"], country, resolved_stop_id, country,
        ))
        inserted += cur.rowcount
    conn.commit()
    print(f"Imported {inserted}/{len(stop_times)} stop_times rows "
          f"(gap is expected: rows referencing a stop_id filtered out above, e.g. a platform-level child stop).")

cur.close()
conn.close()
print("Done.")
