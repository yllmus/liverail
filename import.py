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
    """, (row.get("route_short_name") or row.get("route_long_name"), row["route_id"], row.get("route_type"), country))
conn.commit()
print(f"Imported {len(routes)} routes.")

print(f"Importing trips for {country}...")
trips = pd.read_csv(f"{gtfs_dir}/trips.txt", dtype=str)
for _, row in trips.iterrows():
    cur.execute("""
        INSERT INTO trips (route_id, gtfs_trip_id, country_code)
        SELECT r.id, %s, %s FROM routes r WHERE r.gtfs_route_id = %s AND r.country_code = %s
        ON CONFLICT (country_code, gtfs_trip_id) DO NOTHING
    """, (row["trip_id"], country, row["route_id"], country))
conn.commit()
print(f"Imported {len(trips)} trips.")

cur.close()
conn.close()
print("Done.")
