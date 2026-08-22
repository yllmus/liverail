"""Is timetable padding directional? Same physical segment, both ways."""
import sys; sys.path.insert(0,'/home/ubuntu')
import psycopg2, numpy as np
from collections import defaultdict
from config import DB_PASSWORD
c=psycopg2.connect(dbname="train_delay_db",user="trainuser",password=DB_PASSWORD,host="localhost"); cur=c.cursor()
cur.execute("""
    SELECT DISTINCT ON (o.trip_id,o.observed_date,o.stop_sequence)
           o.trip_id,o.observed_date,o.stop_sequence,o.delay_minutes,o.actual_arrival,s.display_name
    FROM delay_observations o JOIN stations s ON s.id=o.station_id
    WHERE o.country_code='DE' AND o.stop_sequence IS NOT NULL
      AND o.delay_minutes IS NOT NULL AND NOT o.is_cancelled
      AND o.observed_date >= CURRENT_DATE-7
    ORDER BY o.trip_id,o.observed_date,o.stop_sequence,o.id DESC
""")
runs=defaultdict(list)
for trip,day,seq,dl,ts,name in cur.fetchall(): runs[(trip,day)].append((seq,float(dl),ts,name))
cur.close(); c.close()
seg=defaultdict(list)
for v in runs.values():
    v.sort()
    for (s1,d1,t1,n1),(s2,d2,t2,n2) in zip(v,v[1:]):
        if s2!=s1+1 or not n1 or not n2: continue
        delta=d2-d1
        if abs(delta)>240 or abs((t2-t1).total_seconds())>900: continue
        seg[(n1,n2)].append(delta)

pairs=[]
for (a,b),v in seg.items():
    rev=seg.get((b,a))
    if rev is None or len(v)<15 or len(rev)<15 or a>b: continue
    pairs.append((float(np.median(v)),float(np.median(rev)),len(v),len(rev),a,b))
print(f"physical segments observed in BOTH directions with 15+ each: {len(pairs)}")
opposite=[p for p in pairs if p[0]*p[1]<0]
print(f"  where the two directions have OPPOSITE sign: {len(opposite)} ({100*len(opposite)/max(len(pairs),1):.0f}%)")
same=[p for p in pairs if p[0]*p[1]>0]
print(f"  same sign both ways: {len(same)}")
fwd=np.array([p[0] for p in pairs]); bwd=np.array([p[1] for p in pairs])
print(f"  correlation between the two directions: {np.corrcoef(fwd,bwd)[0,1]:+.3f}")
print(f"     (near 0 or negative => direction matters; near +1 => the segment itself is the cause)")
print(f"\nLARGEST DIRECTIONAL SPLITS")
opposite.sort(key=lambda p: -(abs(p[0])+abs(p[1])))
print(f"  {'A->B':>6s} {'B->A':>6s}   segment")
for f_,b_,nf,nb,a,b in opposite[:10]:
    print(f"  {f_:+6.2f} {b_:+6.2f}   {str(a)[:30]} <-> {str(b)[:30]}  (n {nf}/{nb})")
