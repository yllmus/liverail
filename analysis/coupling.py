"""Does train A's delay predict train B's, beyond what B's own history explains?

The rolling-stock/crew hypothesis: a late arrival becomes a late departure
under a different train number a short while later. If real, A's delay should
predict B's RESIDUAL - B's delay minus B's own typical delay at that stop.
Correlating raw delays would just rediscover that everything is late at once.
"""
import os, sys; sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backend"))
import psycopg2, numpy as np
from collections import defaultdict
from config import DB_PASSWORD

COUNTRY=sys.argv[1] if len(sys.argv)>1 else "DE"
LO,HI=10*60,75*60        # B seen 10-75 min after A
c=psycopg2.connect(dbname="train_delay_db",user="trainuser",password=DB_PASSWORD,host="localhost"); cur=c.cursor()
cur.execute("""
    SELECT DISTINCT ON (o.route_name,o.station_id,o.observed_date)
           o.station_id, o.observed_date, o.route_name, o.delay_minutes, o.actual_arrival
    FROM delay_observations o
    WHERE o.country_code=%s AND o.delay_minutes IS NOT NULL AND NOT o.is_cancelled
      AND o.route_name IS NOT NULL AND o.observed_date >= CURRENT_DATE-60
    ORDER BY o.route_name,o.station_id,o.observed_date,o.id DESC
""",(COUNTRY,))
rows=cur.fetchall(); cur.close(); c.close()
print(f"{COUNTRY}: {len(rows)} train-station-days")

bystation=defaultdict(list)
for st,day,rt,dl,ts in rows: bystation[st].append((ts,rt,float(dl),day))

# B's own baseline at that station, so we correlate against the residual
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
            pairs[(st,r1,r2)].append((d1,d2-b))     # A's delay, B's residual

out=[]
for (st,r1,r2),v in pairs.items():
    if len(v)<8: continue
    a=np.array([x[0] for x in v]); b=np.array([x[1] for x in v])
    if a.std()<1.0 or b.std()<1.0: continue     # nothing to correlate
    r=float(np.corrcoef(a,b)[0,1])
    if np.isnan(r): continue
    out.append((r,len(v),st,r1,r2,float(a.std()),float(b.std())))

print(f"candidate (station, A, B) pairs with 8+ shared days and real variance: {len(out)}")
if not out:
    sys.exit()
rs=np.array([o[0] for o in out])
print(f"\ncorrelation of A's delay with B's RESIDUAL:")
print(f"   mean {rs.mean():+.3f}   median {np.median(rs):+.3f}")
print(f"   share > +0.5 : {np.mean(rs>0.5)*100:.1f}%")
print(f"   share < -0.5 : {np.mean(rs<-0.5)*100:.1f}%")
print(f"   (a symmetric spread around 0 = noise; a positive skew = real coupling)")

out.sort(reverse=True)
cur2=psycopg2.connect(dbname="train_delay_db",user="trainuser",password=DB_PASSWORD,host="localhost").cursor()
def nm(sid):
    cur2.execute("SELECT COALESCE(display_name,name) FROM stations WHERE id=%s",(sid,)); r=cur2.fetchone()
    return r[0] if r else str(sid)
print(f"\nSTRONGEST COUPLINGS  (A late -> B late beyond B's own norm)")
print(f"  {'r':>6s} {'n':>4s}  station                    A  ->  B")
for r,n,st,r1,r2,sa,sb in out[:12]:
    print(f"  {r:+6.2f} {n:4d}  {nm(st)[:24]:26s} {str(r1)[:12]:12s} -> {str(r2)[:12]}")
