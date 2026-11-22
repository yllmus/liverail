"""Permutation test: is the coupling asymmetry real, or what noise looks like?

Same pairs, same sample sizes, but B's residuals shuffled across days so any
true A->B link is destroyed while every other structure is preserved.
"""
import os, sys; sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backend"))
import psycopg2, numpy as np
from collections import defaultdict
from config import DB_CONFIG
LO,HI=10*60,75*60
c=psycopg2.connect(**DB_CONFIG); cur=c.cursor()
cur.execute("""
    SELECT DISTINCT ON (o.route_name,o.station_id,o.observed_date)
           o.station_id,o.observed_date,o.route_name,o.delay_minutes,o.actual_arrival
    FROM delay_observations o
    WHERE o.country_code='HU' AND o.delay_minutes IS NOT NULL AND NOT o.is_cancelled
      AND o.route_name IS NOT NULL AND o.observed_date >= CURRENT_DATE-60
    ORDER BY o.route_name,o.station_id,o.observed_date,o.id DESC
""")
rows=cur.fetchall(); cur.close(); c.close()
bystation=defaultdict(list)
for st,day,rt,dl,ts in rows: bystation[st].append((ts,rt,float(dl),day))
base=defaultdict(list)
for st,v in bystation.items():
    for ts,rt,dl,day in v: base[(st,rt)].append(dl)
basemed={k:float(np.median(v)) for k,v in base.items() if len(v)>=5}
pairs=defaultdict(list)
for st,v in bystation.items():
    v.sort()
    for i,(t1,r1,d1,day1) in enumerate(v):
        for t2,r2,d2,day2 in v[i+1:]:
            if day2!=day1: continue
            gap=(t2-t1).total_seconds()
            if gap<LO: continue
            if gap>HI: break
            if r1==r2: continue
            b=basemed.get((st,r2))
            if b is None: continue
            pairs[(st,r1,r2)].append((d1,d2-b))

def summarise(shuffle, rng=None):
    rs=[]
    for v in pairs.values():
        if len(v)<8: continue
        a=np.array([x[0] for x in v]); b=np.array([x[1] for x in v])
        if a.std()<1.0 or b.std()<1.0: continue
        if shuffle: b=rng.permutation(b)
        r=np.corrcoef(a,b)[0,1]
        if not np.isnan(r): rs.append(r)
    return np.array(rs)

real=summarise(False)
rng=np.random.default_rng(0)
nulls=[summarise(True,rng) for _ in range(5)]
null=np.concatenate(nulls)
print(f"pairs analysed: {len(real)}   (null draws: {len(null)})")
print(f"\n{'':22s} {'REAL':>10s} {'NULL':>10s}")
print(f"{'mean r':22s} {real.mean():+10.3f} {null.mean():+10.3f}")
print(f"{'median r':22s} {np.median(real):+10.3f} {np.median(null):+10.3f}")
print(f"{'share r > +0.5':22s} {np.mean(real>0.5)*100:9.1f}% {np.mean(null>0.5)*100:9.1f}%")
print(f"{'share r < -0.5':22s} {np.mean(real<-0.5)*100:9.1f}% {np.mean(null<-0.5)*100:9.1f}%")
print(f"{'share |r| > 0.8':22s} {np.mean(np.abs(real)>0.8)*100:9.1f}% {np.mean(np.abs(null)>0.8)*100:9.1f}%")
lift=np.mean(real>0.5)/max(np.mean(null>0.5),1e-9)
print(f"\nstrong-coupling pairs occur {lift:.1f}x more often than shuffling produces")
print(f"excess over null at r>+0.5: {(np.mean(real>0.5)-np.mean(null>0.5))*len(real):.0f} pairs")
