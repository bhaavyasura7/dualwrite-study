import gzip,json,sys,glob
import pandas as pd
d=sys.argv[1]
for name in ("D-3_P3_r01",):
    for f in glob.glob(f"{d}/{name}.log.gz"):
        rows=[]
        for l in gzip.open(f,"rt"):
            try:e=json.loads(l)
            except ValueError: continue
            if isinstance(e,dict) and "time" in e: rows.append(e)
        if not rows: continue
        x=pd.DataFrame(rows); x["t"]=pd.to_datetime(x.time,format="ISO8601",utc=True); t0=x.t.min()
        x["s"]=((x.t-t0).dt.total_seconds()//10*10).astype(int)
        print(name, x.groupby(["component","msg"]).size().sort_values(ascending=False).head(10).to_string())
        print(x[x.level=="ERROR"].groupby(["component","s"]).size().to_string()[:500])
        print(x[x.level=="ERROR"].error.astype(str).str[:90].value_counts().head(5).to_string())
        print(x[x.msg=="RELAY_BATCH"].groupby("s").size().to_string()[:400])
