"""The absorb/compound flip, straight from the data - no model in the way."""
import os, sys; sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backend"))
import psycopg2, numpy as np
from collections import defaultdict
from config import DB_CONFIG
c=psycopg2.connect(**DB_CONFIG); cur=c.cursor()
out={}
for country in ("DE","HU"):
    cur.execute("""
        SELECT DISTINCT ON (o.trip_id,o.observed_date,o.stop_sequence)
               o.trip_id,o.observed_date,o.stop_sequence,o.delay_minutes,o.actual_arrival
        FROM delay_observations o
        WHERE o.country_code=%s AND o.stop_sequence IS NOT NULL
          AND o.delay_minutes IS NOT NULL AND NOT o.is_cancelled
          AND o.observed_date >= CURRENT_DATE-14
        ORDER BY o.trip_id,o.observed_date,o.stop_sequence,o.id DESC
    """,(country,))
    runs=defaultdict(list)
    for t,d,s,dl,ts in cur.fetchall(): runs[(t,d)].append((s,float(dl),ts))
    pairs=[]
    for v in runs.values():
        v.sort()
        for (s1,d1,t1),(s2,d2,t2) in zip(v,v[1:]):
            if s2!=s1+1 or abs((t2-t1).total_seconds())>900: continue
            dd=d2-d1
            if abs(dd)>240: continue
            pairs.append((d1,dd))
    out[country]=np.array(pairs)
cur.close(); c.close()

for country,a in out.items():
    if len(a)<500: continue
    print(f"\n=== {country}  ({len(a)} adjacent-stop segments, fresh readings only) ===")
    print(f"  {'delay now':>12s} {'n':>7s} {'mean change':>12s} {'median':>8s}  ")
    bands=[(-1e9,-2),(-2,-1),(-1,0),(0,1),(1,2),(2,3),(3,5),(5,10),(10,20),(20,1e9)]
    prev=None
    for lo,hi in bands:
        m=(a[:,0]>=lo)&(a[:,0]<hi)
        if m.sum()<40: continue
        mean=a[m,1].mean(); med=np.median(a[m,1])
        lbl=f">{lo:g}" if hi>1e8 else (f"<{hi:g}" if lo<-1e8 else f"{lo:g}..{hi:g}")
        flip="   <-- FLIPS" if (prev is not None and (prev<0)!=(mean<0)) else ""
        print(f"  {lbl:>12s} {m.sum():7d} {mean:+12.3f} {med:+8.2f}{flip}")
        prev=mean
    late=a[a[:,0]>=3]; early=a[a[:,0]<=0]
    print(f"  trains 3+ min late   : mean change {late[:,1].mean():+.3f} min  (n={len(late)})")
    print(f"  trains on time/early : mean change {early[:,1].mean():+.3f} min  (n={len(early)})")
