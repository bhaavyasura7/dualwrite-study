# For each partition at the cut: was a batch written but not yet acknowledged?
import gzip,json,sys,re,os
import pandas as pd
T=sys.argv[1]
cuts={}
for l in open(f"{T}/f24.log"):
    m=re.search(r'time=(\S+) .*fault injected.*cycle=(\S+)',l)
    if m: cuts[m[2]]=pd.Timestamp(m[1]).tz_convert("UTC")
for cyc,cut in cuts.items():
    f=f"{T}/logs/{cyc}.log.gz"
    if not os.path.exists(f): print(cyc,"no loss (log not kept)"); continue
    w,a,fail={},{},{}
    for l in gzip.open(f,"rt"):
        try:e=json.loads(l)
        except ValueError: continue
        if not isinstance(e,dict) or "partition" not in e: continue
        t=pd.Timestamp(e["time"]).tz_convert("UTC"); p=e["partition"]
        if e["msg"]=="BATCH_WRITTEN" and t<=cut: w[p]=max(w.get(p,t),t)
        if e["msg"]=="SEND_ACKED" and t<=cut: a[p]=max(a.get(p,t),t)
        if e["msg"]=="SEND_FAILED": fail[p]=fail.get(p,0)+1
    row=[]
    for p in range(3):
        inflight = p in w and (p not in a or w[p]>a[p])
        row.append(f"p{p}: inflight={'Y' if inflight else 'n'} failed={fail.get(p,0)}")
    print(cyc[-3:]," | ".join(row))
