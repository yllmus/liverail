"""Where does delay flip from absorbed to compounding? SHAP dependence.

Target: the delay CHANGE to the next stop. Feature of interest: the delay you
already have. LightGBM's pred_contrib gives exact SHAP values with no extra
dependency.
"""
import os, sys; sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backend"))
import psycopg2, numpy as np, lightgbm as lgb
from collections import defaultdict
from config import DB_CONFIG
c=psycopg2.connect(**DB_CONFIG); cur=c.cursor()
cur.execute("""
    SELECT DISTINCT ON (o.trip_id,o.observed_date,o.stop_sequence)
           o.trip_id,o.observed_date,o.stop_sequence,o.delay_minutes,
           o.actual_arrival,o.station_id,o.route_name
    FROM delay_observations o
    WHERE o.country_code='DE' AND o.stop_sequence IS NOT NULL
      AND o.delay_minutes IS NOT NULL AND NOT o.is_cancelled
      AND o.observed_date >= CURRENT_DATE-14
    ORDER BY o.trip_id,o.observed_date,o.stop_sequence,o.id DESC
""")
runs=defaultdict(list)
for t,d,s,dl,ts,st,rt in cur.fetchall(): runs[(t,d)].append((s,float(dl),ts,st,rt))
cur.close(); c.close()

X,y=[],[]
for v in runs.values():
    v.sort()
    for (s1,d1,t1,st1,r1),(s2,d2,t2,st2,r2) in zip(v,v[1:]):
        if s2!=s1+1: continue
        if abs((t2-t1).total_seconds())>900: continue     # freshness guard
        delta=d2-d1
        if abs(delta)>240: continue
        X.append([d1, t1.hour, t1.weekday(), s1, st1]); y.append(delta)
X=np.asarray(X,dtype=np.float32); y=np.asarray(y,dtype=np.float32)
print(f"segments: {len(y)}   mean delta {y.mean():+.3f}  sd {y.std():.2f}")
FEATS=["delay_now","hour","dow","stop_seq","station"]
n=int(len(y)*0.75)
model=lgb.train({"objective":"regression","learning_rate":0.05,"num_leaves":31,
                 "min_data_in_leaf":100,"num_threads":2,"verbose":-1,"seed":0},
                lgb.Dataset(X[:n],label=y[:n],feature_name=FEATS,
                            categorical_feature=["station"]),
                num_boost_round=250)
pred=model.predict(X[n:])
print(f"holdout MAE  model {np.abs(pred-y[n:]).mean():.3f}  vs naive-zero {np.abs(y[n:]).mean():.3f}")

sv=model.predict(X[n:],pred_contrib=True)     # SHAP, exact
s_delay=sv[:,0]; d_now=X[n:,0]
print(f"\nSHAP DEPENDENCE — how 'the delay you already have' pushes the next-stop change")
print(f"  {'delay now':>14s} {'n':>7s} {'mean SHAP':>10s}  effect")
bands=[(-1e9,-2),(-2,0),(0,1),(1,2),(2,3),(3,5),(5,10),(10,20),(20,1e9)]
prev=None
for lo,hi in bands:
    m=(d_now>=lo)&(d_now<hi)
    if m.sum()<50: continue
    v=s_delay[m].mean()
    lbl=f"{lo:g}..{hi:g}" if abs(lo)<1e8 else f"<{hi:g}"
    if hi>1e8: lbl=f">{lo:g}"
    arrow="absorbs" if v<0 else "compounds"
    flip=""
    if prev is not None and (prev<0)!=(v<0): flip="   <-- SIGN FLIPS HERE"
    print(f"  {lbl:>14s} {m.sum():7d} {v:+10.3f}  {arrow}{flip}")
    prev=v
print(f"\nfeature importance (gain):")
for nme,g in sorted(zip(FEATS,model.feature_importance('gain')),key=lambda x:-x[1]):
    print(f"   {nme:10s} {g:12.0f}")
