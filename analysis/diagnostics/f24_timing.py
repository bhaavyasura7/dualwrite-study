# When did each partition's records fail or get acknowledged, relative to the cut?
import gzip,json,sys,re,os
import pandas as pd
T=sys.argv[1]
cuts={}
for l in open(f"{T}/f24.log"):
    m=re.search(r'time=(\S+) .*fault injected.*cycle=(\S+)',l)
    if m: cuts[m[2]]=pd.Timestamp(m[1]).tz_convert("UTC")
for cyc in sorted(cuts):
    f=f"{T}/logs/{cyc}.log.gz"
    if not os.path.exists(f): continue
    cut=cuts[cyc]; fails={},; fails={}; acks={}; errs={}
    for l in gzip.open(f,"rt"):
        try:e=json.loads(l)
        except ValueError: continue
        if not isinstance(e,dict) or "partition" not in e: continue
        t=(pd.Timestamp(e["time"]).tz_convert("UTC")-cut).total_seconds(); p=e["partition"]
        if e["msg"]=="SEND_FAILED":
            fails.setdefault(p,[]).append(t); errs.setdefault(p,set()).add(e.get("error","")[:45])
        if e["msg"]=="SEND_ACKED" and t>0: acks.setdefault(p,[]).append(t)
    out=[]
    for p in range(3):
        F=fails.get(p,[]); A=acks.get(p,[])
        out.append(f"p{p}: fail n={len(F)} at {min(F):.1f}..{max(F):.1f}s" if F else f"p{p}: no fail, first ack {min(A):.1f}s")
    print(cyc[-3:]," | ".join(out))
print("errors:",{e for v in errs.values() for e in v})
