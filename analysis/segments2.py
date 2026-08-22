"""Absorbers vs amplifiers, with the freshness trap closed."""
import sys; sys.path.insert(0,'/home/ubuntu')
import psycopg2, numpy as np
from collections import defaultdict
from config import DB_PASSWORD
c=psycopg2.connect(dbname="train_delay_db",user="trainuser",password=DB_PASSWORD,host="localhost"); cur=c.cursor()
cur.execute("""
    SELECT DISTINCT ON (o.trip_id,o.observed_date,o.stop_sequence)
           o.trip_id,o.observed_date,o.stop_sequence,o.delay_minutes,
           o.actual_arrival, s.display_name, o.route_name
    FROM delay_observations o JOIN stations s ON s.id=o.station_id
    WHERE o.country_code='DE' AND o.stop_sequence IS NOT NULL
      AND o.delay_minutes IS NOT NULL AND NOT o.is_cancelled
      AND o.observed_date >= CURRENT_DATE-7
    ORDER BY o.trip_id,o.observed_date,o.stop_sequence,o.id DESC
""")
runs=defaultdict(list)
for trip,day,seq,dl,ts,name,rt in cur.fetchall(): runs[(trip,day)].append((seq,float(dl),ts,name,rt))
cur.close(); c.close()

MAX_GAP=900   # 15 min: both readings must describe roughly the same moment
seg=defaultdict(list); kept=dropped=0; raw=[]; fresh=[]
for v in runs.values():
    v.sort()
    for (s1,d1,t1,n1,r1),(s2,d2,t2,n2,r2) in zip(v,v[1:]):
        if s2!=s1+1: continue          # adjacent stops only
        delta=d2-d1
        if abs(delta)>240: continue
        raw.append(delta)
        gap=abs((t2-t1).total_seconds())
        if gap>MAX_GAP: dropped+=1; continue
        kept+=1; fresh.append(delta)
        seg[(n1,n2,r1)].append(delta)

print(f"adjacent-stop pairs: {len(raw)}")
print(f"  readings >15 min apart, DISCARDED: {dropped} ({100*dropped/max(len(raw),1):.1f}%)")
print(f"  kept: {kept}")
r=np.array(raw); f=np.array(fresh)
print(f"\nnaive baseline (delay unchanged)      raw        fresh-only")
print(f"  mean abs error            {np.abs(r).mean():8.2f} {np.abs(f).mean():13.2f} min")
print(f"  bias (mean delta)         {r.mean():+8.3f} {f.mean():+13.3f} min")
print(f"  within 1 min              {np.mean(np.abs(r)<=1)*100:7.1f}% {np.mean(np.abs(f)<=1)*100:12.1f}%")

stats=[]
for (n1,n2,rt),v in seg.items():
    if len(v)<20: continue
    a=np.array(v)
    stats.append((float(np.median(a)),float(a.mean()),len(v),n1,n2,rt))
stats.sort()
print(f"\nsegments with 20+ fresh observations: {len(stats)}")
print(f"\nTOP ABSORBERS  (median delta, minutes)")
print(f"  {'med':>6s} {'mean':>6s} {'n':>4s}  segment")
for md,m,n,n1,n2,rt in stats[:8]:
    print(f"  {md:+6.2f} {m:+6.2f} {n:4d}  {str(n1)[:26]} -> {str(n2)[:26]} [{str(rt)[:7]}]")
print(f"\nTOP AMPLIFIERS")
for md,m,n,n1,n2,rt in stats[-8:][::-1]:
    print(f"  {md:+6.2f} {m:+6.2f} {n:4d}  {str(n1)[:26]} -> {str(n2)[:26]} [{str(rt)[:7]}]")
allm=np.array([s[0] for s in stats])
print(f"\n  absorbers (median < -0.25): {np.sum(allm<-0.25)}")
print(f"  neutral                    : {np.sum((allm>=-0.25)&(allm<=0.25))}")
print(f"  amplifiers (median > +0.25): {np.sum(allm>0.25)}")
