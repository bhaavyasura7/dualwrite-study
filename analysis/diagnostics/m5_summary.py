import gzip,json,sys,glob,os
import pandas as pd
T=sys.argv[1]
d=pd.read_csv(f"{T}/results.csv"); d["t0"]=pd.to_datetime(d.started_at); d["t1"]=pd.to_datetime(d.finished_at)
d["cycle_s"]=(d.t1-d.t0).dt.total_seconds(); d["overhead_s"]=d.cycle_s-d.load_s
print(len(d),"rows;",d.status.value_counts().to_dict())
print(d.groupby("vector_id")[["cycle_s","load_s","overhead_s"]].mean().round(1).to_string())
print("overhead mean/min/max",d.overhead_s.mean().round(1),d.overhead_s.min(),d.overhead_s.max())
a=d[d.vector_id=="A-1"]
print("A-1 hooked",a.hooked_lost.value_counts().to_dict(),"ghost",a.ghost.sum(),"dup",a.duplicates.sum())
print(a[["collateral_lost","lost_after_send","inflight_at_crash","oldest_pending_ms"]].describe().loc[["mean","50%","max"]].round(1).to_string())
print("corr collateral~inflight",round(a.collateral_lost.corr(a.inflight_at_crash),3))
print("cycles collateral>inflight",(a.collateral_lost>a.inflight_at_crash).sum())
rows=[]
for _,r in a.iterrows():
    sent,acked,fail={}, set(), set(); crash=None; after=0
    for l in gzip.open(f"{T}/logs/{r.cycle_id}.log.gz","rt"):
        try:e=json.loads(l)
        except ValueError: continue
        if not isinstance(e,dict): continue
        m=e.get("msg")
        if m=="CRASH_POINT_HIT": crash=e; continue
        if m=="orderservice started" and crash: break
        if m=="SENT_CALLED":
            sent[(e["order_id"],e["seq"])]=e["event_id"]
            if crash: after+=1
    dm=json.load(open(f"{T}/damage/{r.cycle_id}.json"))
    lost=[(p["order"],p["seq"]) for p in dm["lost"] or []]
    rows.append(dict(cycle=r.cycle_id,lost=len(lost),no_sent=sum(1 for p in lost if p not in sent),inflight=r.inflight_at_crash,sent_after_marker=after))
x=pd.DataFrame(rows); print(x.sort_values("lost").tail(7).to_string(index=False))
print("lost never reaching SENT_CALLED, total",x.no_sent.sum()," minus hooked",x.no_sent.sum()-30)
b=d[d.vector_id=="A-2"]; print(b[["cycle_id","lost","hooked_lost","collateral_lost","inflight_at_crash"]].to_string(index=False))
c=d[d.vector_id=="A-8"]; print(c[["cycle_id","sweep_json","lost","hooked_lost","lost_after_send","inflight_at_crash"]].to_string(index=False))
print("p99 latency median",a.latency_p99_ms.median(),"range",a.latency_p99_ms.min(),a.latency_p99_ms.max(),"db cpu",a.db_cpu_pct.mean().round(1))
