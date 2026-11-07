import json,sys,pandas as pd
S=sys.argv[1]
rows=[]
for l in open(f"{S}/ovr.log"):
    try:e=json.loads(l)
    except ValueError: continue
    if isinstance(e,dict) and "time" in e: rows.append(e)
d=pd.DataFrame(rows); d["t"]=pd.to_datetime(d.time); t0=d.t.min()
d["s"]=(d.t-t0).dt.total_seconds().round(0)
print(d[d.msg.isin(["SENT_CALLED","SEND_ACKED","SEND_FAILED"])].groupby(["s","msg"]).size().unstack(fill_value=0).loc[5:22].to_string())
