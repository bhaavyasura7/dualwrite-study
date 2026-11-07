import json,sys,os
import pandas as pd
T=sys.argv[1]
d=pd.read_csv(f"{T}/results.csv")
pd.set_option("display.width",220)
d["ovh"]=(pd.to_datetime(d.finished_at)-pd.to_datetime(d.started_at)).dt.total_seconds()-d.load_s
print(d[["cycle_id","status","events_total","lost","ghost","duplicates","out_of_order","stale_redelivery","recovery_s","load_failed","failover_lag_bytes","failover_lag_ms","load_s","ovh"]].to_string(index=False))
for _,r in d.iterrows():
    f=f"{T}/damage/{r.cycle_id}.json"
    if not os.path.exists(f) or pd.isna(r.failover_at): continue
    j=json.load(open(f)); L=j.get("lost_detail") or []
    if not L: continue
    t0=pd.Timestamp(r.failover_at)
    rel=[(pd.Timestamp(x["commit_time"])-t0).total_seconds() for x in L]
    s=pd.Series(rel)
    print(r.cycle_id,"lost",len(L),"intermediate",sum(x["intermediate"] for x in L),"seq",pd.Series([x["seq"] for x in L]).value_counts().to_dict(),
          "commit vs failover s: min",round(s.min(),2),"max",round(s.max(),2),"before",int((s<0).sum()),"after",int((s>=0).sum()))
