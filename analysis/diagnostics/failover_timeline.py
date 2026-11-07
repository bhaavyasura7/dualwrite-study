import gzip,json,sys,glob,os
import pandas as pd
T=sys.argv[1]
d=pd.read_csv(f"{T}/results.csv").set_index("cycle_id")
print(d[["throughput_ops","latency_p99_ms","pre_p99_ms","pre_flag","started_at","failover_at"]].to_string())
for f in sorted(glob.glob(f"{T}/logs/*.log.gz")):
    name=os.path.basename(f)[:-7]
    rows=[]
    for l in gzip.open(f,"rt"):
        try:e=json.loads(l)
        except ValueError: continue
        if isinstance(e,dict) and "time" in e: rows.append(e)
    if not rows: continue
    x=pd.DataFrame(rows); x["t"]=pd.to_datetime(x.time,format="ISO8601",utc=True)
    fo=pd.Timestamp(d.loc[name].failover_at) if name in d.index else None
    st=x.t.min()
    x["s"]=((x.t-st).dt.total_seconds()//5*5).astype(int)
    err=x[x.level=="ERROR"]
    print(name,"first log->failover s",round((fo-st).total_seconds(),1) if fo is not None else None,"lines",len(x))
    print("  errors by 5s:",err.groupby("s").size().to_dict())
    if "error" in err: print("  ",err.error.astype(str).str[:100].value_counts().head(3).to_dict())
