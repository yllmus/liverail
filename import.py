import argparse
import pandas as pd
import psycopg2

DB_CONFIG = {
    "dbname": "train_delay_db",
    "user": "trainuser",
    "password": "changeme",
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
args = parser.parse_args()
country = args.country.upper()
gtfs_dir = args.gtfs_dir

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

cur.execute("SELECT 1 FROM countries WHERE code = %s", (country,))
if not cur.fetchone():
    raise SystemExit(
        f"Country '{country}' is not in the countries table yet. "
        f"Add it first: INSERT INTO countries (code, name) VALUES ('{country}', '<name>');"
    )

print(f"Importing stops for {country}...")
stops = pd.read_csv(f"{gtfs_dir}/stops.txt", dtype=str)
if "parent_station" in stops.columns:
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
        INSERT INTO stations (name, gtfs_stop_id, lat, lon, country_code)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (country_code, gtfs_stop_id) DO NOTHING
    """, (row["stop_name"], row["stop_id"], row.get("stop_lat"), row.get("stop_lon"), country))
conn.commit()
print(f"Imported {len(stops)} stops.")

print(f"Importing routes for {country}...")
routes = pd.read_csv(f"{gtfs_dir}/routes.txt", dtype=str)
for _, row in routes.iterrows():
    cur.execute("""
        INSERT INTO routes (name, gtfs_route_id, route_type, country_code)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (country_code, gtfs_route_id) DO NOTHING
    """, (row.get("route_short_name") or row.get("route_long_name"), args.route_id_prefix + row["route_id"], row.get("route_type"), country))
conn.commit()
print(f"Imported {len(routes)} routes.")

print(f"Importing trips for {country}...")
trips = pd.read_csv(f"{gtfs_dir}/trips.txt", dtype=str)
for _, row in trips.iterrows():
    cur.execute("""
        INSERT INTO trips (route_id, gtfs_trip_id, country_code)
        SELECT r.id, %s, %s FROM routes r WHERE r.gtfs_route_id = %s AND r.country_code = %s
        ON CONFLICT (country_code, gtfs_trip_id) DO NOTHING
    """, (row["trip_id"], country, args.route_id_prefix + row["route_id"], country))
conn.commit()
print(f"Imported {len(trips)} trips.")

if args.with_stop_times:
    print(f"Importing stop_times for {country}...")
    stop_times = pd.read_csv(f"{gtfs_dir}/stop_times.txt", dtype=str)
    inserted = 0
    for _, row in stop_times.iterrows():
        cur.execute("""
            INSERT INTO stop_times (trip_id, station_id, scheduled_arrival, scheduled_departure, stop_sequence)
            SELECT t.id, s.id, %s, %s, %s
            FROM trips t, stations s
            WHERE t.gtfs_trip_id = %s AND t.country_code = %s
              AND s.gtfs_stop_id = %s AND s.country_code = %s
        """, (
            row.get("arrival_time"), row.get("departure_time"), row["stop_sequence"],
            row["trip_id"], country, row["stop_id"], country,
        ))
        inserted += cur.rowcount
    conn.commit()
    print(f"Imported {inserted}/{len(stop_times)} stop_times rows "
          f"(gap is expected: rows referencing a stop_id filtered out above, e.g. a platform-level child stop).")

cur.close()
conn.close()
print("Done.")
